//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/sroar"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/notimplemented"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/sorter"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

type Searcher struct {
	store         *lsmkv.Store
	schema        schema.Schema
	rowCache      cacher
	classSearcher ClassSearcher // to allow recursive searches on ref-props
	propIndices   propertyspecific.Indices
	deletedDocIDs DeletedDocIDChecker
	stopwords     stopwords.StopwordDetector
	shardVersion  uint16
}

type cacher interface {
	Store(id []byte, entry *CacheEntry)
	Load(id []byte) (*CacheEntry, bool)
}

type DeletedDocIDChecker interface {
	Contains(id uint64) bool
}

func NewSearcher(store *lsmkv.Store, schema schema.Schema,
	rowCache cacher, propIndices propertyspecific.Indices,
	classSearcher ClassSearcher, deletedDocIDs DeletedDocIDChecker,
	stopwords stopwords.StopwordDetector, shardVersion uint16,
) *Searcher {
	return &Searcher{
		store:         store,
		schema:        schema,
		rowCache:      rowCache,
		propIndices:   propIndices,
		classSearcher: classSearcher,
		deletedDocIDs: deletedDocIDs,
		stopwords:     stopwords,
		shardVersion:  shardVersion,
	}
}

// Objects returns a list of full objects
func (s *Searcher) Objects(ctx context.Context, limit int,
	filter *filters.LocalFilter, sort []filters.Sort, additional additional.Properties,
	className schema.ClassName,
) ([]*storobj.Object, error) {
	pv, err := s.extractPropValuePair(filter.Root, className)
	if err != nil {
		return nil, err
	}

	if err := pv.fetchDocIDs(s, limit); err != nil {
		return nil, errors.Wrap(err, "fetch doc ids for prop/value pair")
	}

	dbm, err := pv.mergeDocIDs()
	if err != nil {
		return nil, errors.Wrap(err, "merge doc ids by operator")
	}

	if len(sort) > 0 {
		return s.sortedObjectsByDocID(ctx, limit, sort, dbm.IDs(), additional, className)
	}

	return s.allObjectsByDocID(dbm.IDs(), limit, additional)
}

func (s *Searcher) allObjectsByDocID(ids []uint64, limit int,
	additional additional.Properties,
) ([]*storobj.Object, error) {
	// cutoff if required, e.g. after merging unlimted filters
	docIDs := ids
	if len(docIDs) > limit {
		docIDs = docIDs[:limit]
	}

	res, err := s.objectsByDocID(docIDs, additional)
	if err != nil {
		return nil, errors.Wrap(err, "resolve doc ids to objects")
	}
	return res, nil
}

func (s *Searcher) sortedObjectsByDocID(ctx context.Context, limit int, sort []filters.Sort, ids []uint64,
	additional additional.Properties, className schema.ClassName,
) ([]*storobj.Object, error) {
	docIDs, err := s.sort(ctx, limit, sort, ids, additional, className)
	if err != nil {
		return nil, errors.Wrap(err, "sort doc ids")
	}
	return s.objectsByDocID(docIDs, additional)
}

func (s *Searcher) sort(ctx context.Context, limit int, sort []filters.Sort, docIDs []uint64,
	additional additional.Properties, className schema.ClassName,
) ([]uint64, error) {
	lsmSorter, err := sorter.NewLSMSorter(s.store, s.schema, className)
	if err != nil {
		return nil, err
	}
	return lsmSorter.SortDocIDs(ctx, limit, sort, docIDs)
}

func (s *Searcher) objectsByDocID(ids []uint64,
	additional additional.Properties,
) ([]*storobj.Object, error) {
	out := make([]*storobj.Object, len(ids))

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, errors.Errorf("objects bucket not found")
	}

	i := 0

	for _, id := range ids {
		keyBuf := bytes.NewBuffer(nil)
		binary.Write(keyBuf, binary.LittleEndian, &id)
		docIDBytes := keyBuf.Bytes()
		res, err := bucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return nil, err
		}

		if res == nil {
			continue
		}

		unmarshalled, err := storobj.FromBinaryOptional(res, additional)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal data object at position %d", i)
		}

		out[i] = unmarshalled
		i++
	}

	return out[:i], nil
}

// DocIDs is similar to Objects, but does not actually resolve the docIDs to
// full objects. Instead it returns the pure object id pointers. They can then
// be used in a secondary index (e.g. vector index)
//
// DocID queries does not contain a limit by design, as we won't know if the limit
// wouldn't remove the item that is most important for the follow up query.
// Imagine the user sets the limit to 1 and the follow-up is a vector search.
// If we already limited the allowList to 1, the vector search would be
// pointless, as only the first element would be allowed, regardless of which
// had the shortest distance
func (s *Searcher) DocIDs(ctx context.Context, filter *filters.LocalFilter,
	additional additional.Properties, className schema.ClassName,
) (helpers.AllowList, error) {
	return s.docIDs(ctx, filter, additional, className, true)
}

// DocIDsPreventCaching is the same as DocIDs, but makes sure that no filter
// cache entries are written. This can be used when we can guarantee that the
// filter is part of an operation that will lead to a state change, such as
// batch delete. The state change would make the cached filter unusable
// anyway, so we don't need to unnecessarily populate the cache with an entry.
func (s *Searcher) DocIDsPreventCaching(ctx context.Context, filter *filters.LocalFilter,
	additional additional.Properties, className schema.ClassName,
) (helpers.AllowList, error) {
	return s.docIDs(ctx, filter, additional, className, false)
}

func (s *Searcher) docIDs(ctx context.Context, filter *filters.LocalFilter,
	additional additional.Properties, className schema.ClassName,
	allowCaching bool,
) (helpers.AllowList, error) {
	pv, err := s.extractPropValuePair(filter.Root, className)
	if err != nil {
		return nil, err
	}

	cacheable := pv.cacheable()
	if cacheable && allowCaching {
		if err := pv.fetchHashes(s); err != nil {
			return nil, errors.Wrap(err, "fetch row hashes to check for cach eligibility")
		}

		res, ok := s.rowCache.Load(pv.docIDs.checksum)
		if ok && res.Type == CacheTypeAllowList {
			return res.AllowList, nil
		}
	}

	if err := pv.fetchDocIDs(s, 0); err != nil {
		return nil, errors.Wrap(err, "fetch doc ids for prop/value pair")
	}

	dbm, err := pv.mergeDocIDs()
	if err != nil {
		return nil, errors.Wrap(err, "merge doc ids by operator")
	}

	// TODO make allowlist use bitmaps under the hood?
	out := make(helpers.AllowList, dbm.count())
	for _, p := range dbm.IDs() {
		out.Insert(p)
	}

	// TODO should really be single pv.docIDs or merged ones?
	if cacheable && allowCaching {
		// TODO change cache to work on BM?
		// transforming to pointers for backward compatibility
		s.rowCache.Store(pv.docIDs.checksum, &CacheEntry{
			Type:      CacheTypeAllowList,
			AllowList: out,
			Partial:   pv.docIDs.toDocPointers(),
			Hash:      pv.docIDs.checksum,
		})
	}

	return out, nil
}

func (s *Searcher) extractPropValuePair(filter *filters.Clause,
	className schema.ClassName,
) (*propValuePair, error) {
	out := newPropValuePair()
	if filter.Operands != nil {
		// nested filter
		out.children = make([]*propValuePair, len(filter.Operands))

		for i, clause := range filter.Operands {
			child, err := s.extractPropValuePair(&clause, className)
			if err != nil {
				return nil, errors.Wrapf(err, "nested clause at pos %d", i)
			}
			out.children[i] = child
		}
		out.operator = filter.Operator
		return &out, nil
	}

	// on value or non-nested filter
	props := filter.On.Slice()
	if len(props) != 1 {
		return s.extractReferenceFilter(filter, className)
	}
	// we are on a value element

	if s.onInternalProp(props[0]) {
		return s.extractInternalProp(props[0], filter.Value.Type, filter.Value.Value, filter.Operator)
	}

	if s.onRefProp(className, props[0]) && filter.Value.Type == schema.DataTypeInt {
		// ref prop and int type is a special case, the user is looking for the
		// reference count as opposed to the content
		return s.extractReferenceCount(props[0], filter.Value.Value, filter.Operator)
	}

	if s.onGeoProp(className, props[0]) {
		return s.extractGeoFilter(props[0], filter.Value.Value, filter.Value.Type,
			filter.Operator)
	}

	if s.onTokenizablePropValue(filter.Value.Type) {
		property, err := s.schema.GetProperty(className, schema.PropertyName(props[0]))
		if err != nil {
			return nil, err
		}

		return s.extractTokenizableProp(props[0], filter.Value.Type, filter.Value.Value,
			filter.Operator, property.Tokenization)
	}

	return s.extractPrimitiveProp(props[0], filter.Value.Type, filter.Value.Value,
		filter.Operator)
}

func (s *Searcher) extractReferenceFilter(filter *filters.Clause,
	className schema.ClassName,
) (*propValuePair, error) {
	ctx := context.TODO()
	return newRefFilterExtractor(s.classSearcher, filter, className, s.schema).Do(ctx)
}

func (s *Searcher) extractPrimitiveProp(propName string, dt schema.DataType,
	value interface{}, operator filters.Operator,
) (*propValuePair, error) {
	var extractValueFn func(in interface{}) ([]byte, error)
	var hasFrequency bool
	switch dt {
	case schema.DataTypeBoolean:
		extractValueFn = s.extractBoolValue
		hasFrequency = false
	case schema.DataTypeInt:
		extractValueFn = s.extractIntValue
		hasFrequency = false
	case schema.DataTypeNumber:
		extractValueFn = s.extractNumberValue
		hasFrequency = false
	case schema.DataTypeDate:
		extractValueFn = s.extractDateValue
		hasFrequency = false
	case "":
		return nil, fmt.Errorf("data type cannot be empty")
	default:
		return nil, fmt.Errorf("data type %q not supported yet in standalone mode, "+
			"see %s for details", dt, notimplemented.Link)
	}

	byteValue, err := extractValueFn(value)
	if err != nil {
		return nil, err
	}

	return &propValuePair{
		value:        byteValue,
		hasFrequency: hasFrequency,
		prop:         propName,
		operator:     operator,
	}, nil
}

func (s *Searcher) extractReferenceCount(propName string, value interface{},
	operator filters.Operator,
) (*propValuePair, error) {
	byteValue, err := s.extractIntCountValue(value)
	if err != nil {
		return nil, err
	}

	return &propValuePair{
		value:        byteValue,
		hasFrequency: false,
		prop:         helpers.MetaCountProp(propName),
		operator:     operator,
	}, nil
}

func (s *Searcher) extractGeoFilter(propName string, value interface{},
	valueType schema.DataType, operator filters.Operator,
) (*propValuePair, error) {
	if valueType != schema.DataTypeGeoCoordinates {
		return nil, fmt.Errorf("prop %q is of type geoCoordinates, it can only"+
			"be used with geoRange filters", propName)
	}

	parsed := value.(filters.GeoRange)

	return &propValuePair{
		value:         nil, // not going to be served by an inverted index
		valueGeoRange: &parsed,
		hasFrequency:  false,
		prop:          propName,
		operator:      operator,
	}, nil
}

func (s *Searcher) extractInternalProp(propName string, propType schema.DataType, value interface{},
	operator filters.Operator,
) (*propValuePair, error) {
	switch propName {
	case filters.InternalPropBackwardsCompatID, filters.InternalPropID:
		return s.extractIDProp(value, operator)
	case filters.InternalPropCreationTimeUnix, filters.InternalPropLastUpdateTimeUnix:
		return extractTimestampProp(propName, propType, value, operator)
	default:
		return nil, fmt.Errorf(
			"failed to extract internal prop, unsupported internal prop '%s'", propName)
	}
}

func (s *Searcher) extractIDProp(value interface{},
	operator filters.Operator,
) (*propValuePair, error) {
	v, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected value to be string, got %T", value)
	}

	return &propValuePair{
		value:        []byte(v),
		hasFrequency: false,
		prop:         filters.InternalPropID,
		operator:     operator,
	}, nil
}

func extractTimestampProp(propName string, propType schema.DataType, value interface{},
	operator filters.Operator,
) (*propValuePair, error) {
	if propType != schema.DataTypeDate && propType != schema.DataTypeString {
		return nil, fmt.Errorf(
			"failed to extract internal prop, unsupported type %T for prop %s", value, propName)
	}

	var valResult []byte
	// if propType is a `valueDate`, we need to convert
	// it to ms before fetching. this is the format by
	// which our timestamps are indexed
	if propType == schema.DataTypeDate {
		v, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("expected value to be time.Time, got %T", value)
		}

		b, err := json.Marshal(v.UnixNano() / int64(time.Millisecond))
		if err != nil {
			return nil, fmt.Errorf("failed to extract internal prop: %s", err)
		}
		valResult = b
	} else {
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected value to be string, got %T", value)
		}
		valResult = []byte(v)
	}

	return &propValuePair{
		value:        valResult,
		hasFrequency: false,
		prop:         propName,
		operator:     operator,
	}, nil
}

func (s *Searcher) extractTokenizableProp(propName string, dt schema.DataType, value interface{},
	operator filters.Operator, tokenization string,
) (*propValuePair, error) {
	var parts []string

	switch dt {
	case schema.DataTypeString:
		switch tokenization {
		case models.PropertyTokenizationWord:
			parts = helpers.TokenizeString(value.(string))
		case models.PropertyTokenizationField:
			parts = []string{helpers.TrimString(value.(string))}
		default:
			return nil, fmt.Errorf("unsupported tokenization '%v' configured for data type '%v'", tokenization, dt)
		}
	case schema.DataTypeText:
		switch tokenization {
		case models.PropertyTokenizationWord:
			if operator == filters.OperatorLike {
				// if the operator is like, we cannot apply the regular text-splitting
				// logic as it would remove all wildcard symbols
				parts = helpers.TokenizeTextKeepWildcards(value.(string))
			} else {
				parts = helpers.TokenizeText(value.(string))
			}
		default:
			return nil, fmt.Errorf("unsupported tokenization '%v' configured for data type '%v'", tokenization, dt)
		}
	default:
		return nil, fmt.Errorf("expected value type to be string or text, got %v", dt)
	}

	propValuePairs := make([]*propValuePair, 0, len(parts))
	for _, part := range parts {
		if s.stopwords.IsStopword(part) {
			continue
		}
		propValuePairs = append(propValuePairs, &propValuePair{
			value:        []byte(part),
			hasFrequency: true,
			prop:         propName,
			operator:     operator,
		})
	}

	if len(propValuePairs) > 1 {
		return &propValuePair{operator: filters.OperatorAnd, children: propValuePairs}, nil
	}
	if len(propValuePairs) == 1 {
		return propValuePairs[0], nil
	}
	return nil, errors.Errorf("invalid search term, only stopwords provided. Stopwords can be configured in class.invertedIndexConfig.stopwords")
}

// TODO: repeated calls to on... aren't too efficient because we iterate over
// the schema each time, might be smarter to have a single method that
// determines the type and then we switch based on the result. However, the
// effect of that should be very small unless the schema is absolutely massive.
func (s *Searcher) onRefProp(className schema.ClassName, propName string) bool {
	property, err := s.schema.GetProperty(className, schema.PropertyName(propName))
	if err != nil {
		return false
	}

	return schema.IsRefDataType(property.DataType)
}

// TODO: repeated calls to on... aren't too efficient because we iterate over
// the schema each time, might be smarter to have a single method that
// determines the type and then we switch based on the result. However, the
// effect of that should be very small unless the schema is absolutely massive.
func (s *Searcher) onGeoProp(className schema.ClassName, propName string) bool {
	property, err := s.schema.GetProperty(className, schema.PropertyName(propName))
	if err != nil {
		return false
	}

	return schema.DataType(property.DataType[0]) == schema.DataTypeGeoCoordinates
}

func (s *Searcher) onInternalProp(propName string) bool {
	return filters.IsInternalProperty(schema.PropertyName(propName))
}

func (s *Searcher) onTokenizablePropValue(valueType schema.DataType) bool {
	switch valueType {
	case schema.DataTypeString, schema.DataTypeText:
		return true
	default:
		return false
	}
}

type docBitmap struct {
	docIDs   *sroar.Bitmap
	checksum []byte
}

func newDocBitmap() docBitmap {
	return docBitmap{docIDs: sroar.NewBitmap()}
}

func (dbm *docBitmap) count() int {
	if dbm.docIDs == nil {
		return 0
	}
	return dbm.docIDs.GetCardinality()
}

func (dbm *docBitmap) IDs() []uint64 {
	if dbm.docIDs == nil {
		return []uint64{}
	}
	return dbm.docIDs.ToArray()
}

func (dbm *docBitmap) toDocPointers() *docPointers {
	if dbm.docIDs == nil {
		return &docPointers{checksum: dbm.checksum}
	}
	return &docPointers{
		count:    uint64(dbm.docIDs.GetCardinality()),
		docIDs:   dbm.docIDs.ToArray(),
		checksum: dbm.checksum,
	}
}

type docPointers struct {
	count    uint64
	docIDs   []uint64
	checksum []byte // helps us judge if a cached read is still fresh
}

type docPointersWithScore struct {
	count    uint64
	docIDs   []docPointerWithScore
	checksum []byte // helps us judge if a cached read is still fresh
}

type docPointerWithScore struct {
	id         uint64
	frequency  float64
	propLength float64
	score      float64
	Additional map[string]interface{}
}

func (d docPointers) IDs() []uint64 {
	return d.docIDs
}

func (d docPointersWithScore) IDs() []uint64 {
	out := make([]uint64, len(d.docIDs))
	for i, elem := range d.docIDs {
		out[i] = elem.id
	}
	return out
}

func (d *docPointers) removeDuplicates() {
	counts := make(map[uint64]uint16, len(d.docIDs))
	for _, id := range d.docIDs {
		counts[id]++
	}

	updated := make([]uint64, len(d.docIDs))
	i := 0
	for _, id := range d.docIDs {
		if counts[id] == 1 {
			updated[i] = id
			i++
		}

		counts[id]--
	}

	d.docIDs = updated[:i]
}
