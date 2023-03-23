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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
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
	"golang.org/x/sync/errgroup"
)

type Searcher struct {
	logger        logrus.FieldLogger
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

func NewSearcher(logger logrus.FieldLogger, store *lsmkv.Store,
	schema schema.Schema, rowCache cacher,
	propIndices propertyspecific.Indices, classSearcher ClassSearcher,
	deletedDocIDs DeletedDocIDChecker, stopwords stopwords.StopwordDetector,
	shardVersion uint16,
) *Searcher {
	return &Searcher{
		logger:        logger,
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

	if err := pv.fetchDocIDs(s, limit, !pv.cacheable()); err != nil {
		return nil, errors.Wrap(err, "fetch doc ids for prop/value pair")
	}

	dbm, err := pv.mergeDocIDs()
	if err != nil {
		return nil, errors.Wrap(err, "merge doc ids by operator")
	}

	allowList := helpers.NewAllowListFromBitmap(dbm.docIDs)
	var it docIDsIterator
	if len(sort) > 0 {
		docIDs, err := s.sort(ctx, limit, sort, allowList, additional, className)
		if err != nil {
			return nil, errors.Wrap(err, "sort doc ids")
		}
		it = newSliceDocIDsIterator(docIDs)
	} else {
		it = allowList.LimitedIterator(limit)
	}

	return s.objectsByDocID(it, additional)
}

func (s *Searcher) sort(ctx context.Context, limit int, sort []filters.Sort, docIDs helpers.AllowList,
	additional additional.Properties, className schema.ClassName,
) ([]uint64, error) {
	lsmSorter, err := sorter.NewLSMSorter(s.store, s.schema, className)
	if err != nil {
		return nil, err
	}
	return lsmSorter.SortDocIDs(ctx, limit, sort, docIDs)
}

func (s *Searcher) objectsByDocID(it docIDsIterator,
	additional additional.Properties,
) ([]*storobj.Object, error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, errors.Errorf("objects bucket not found")
	}

	out := make([]*storobj.Object, it.Len())
	docIDBytes := make([]byte, 8)

	i := 0
	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		res, err := bucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return nil, err
		}

		if res == nil {
			continue
		}

		var unmarshalled *storobj.Object
		if additional.ReferenceQuery {
			unmarshalled, err = storobj.FromBinaryUUIDOnly(res)
		} else {
			unmarshalled, err = storobj.FromBinaryOptional(res, additional)
		}
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

		if res, ok := s.rowCache.Load(pv.docIDs.checksum); ok {
			return res.AllowList, nil
		}
	}

	if err := pv.fetchDocIDs(s, 0, !pv.cacheable()); err != nil {
		return nil, errors.Wrap(err, "fetch doc ids for prop/value pair")
	}

	dbm, err := pv.mergeDocIDs()
	if err != nil {
		return nil, errors.Wrap(err, "merge doc ids by operator")
	}

	out := helpers.NewAllowListFromBitmap(dbm.docIDs)

	if cacheable && allowCaching {
		s.rowCache.Store(pv.docIDs.checksum, &CacheEntry{
			AllowList: out,
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

		eg := errgroup.Group{}

		for i, clause := range filter.Operands {
			i, clause := i, clause
			eg.Go(func() error {
				child, err := s.extractPropValuePair(&clause, className)
				if err != nil {
					return errors.Wrapf(err, "nested clause at pos %d", i)
				}
				out.children[i] = child

				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, fmt.Errorf("nested query: %w", err)
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

	if s.onUUIDProp(className, props[0]) {
		return s.extractUUIDFilter(props[0], filter.Value.Value, filter.Value.Type,
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
	return newRefFilterExtractor(s.logger, s.classSearcher, filter, className, s.schema).
		Do(ctx)
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

func (s *Searcher) extractUUIDFilter(propName string, value interface{},
	valueType schema.DataType, operator filters.Operator,
) (*propValuePair, error) {
	if valueType != schema.DataTypeString {
		return nil, fmt.Errorf("prop %q is of type uuid, the uuid to filter"+
			"on must be specified as a string (e.g. valueString:<uuid>)", propName)
	}

	asStr, ok := value.(string)
	if !ok {
		return nil,
			fmt.Errorf("expected to see uuid as string in filter, got %T", value)
	}

	parsed, err := uuid.Parse(asStr)
	if err != nil {
		return nil, fmt.Errorf("parse uuid string: %w", err)
	}

	return &propValuePair{
		value:        parsed[:],
		hasFrequency: false,
		prop:         propName,
		operator:     operator,
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

// Note: A UUID prop is a user-specified prop of type UUID. This has nothing to
// do with the primary ID of an object which happens to always be a UUID in
// Weaviate v1
//
// TODO: repeated calls to on... aren't too efficient because we iterate over
// the schema each time, might be smarter to have a single method that
// determines the type and then we switch based on the result. However, the
// effect of that should be very small unless the schema is absolutely massive.
func (s *Searcher) onUUIDProp(className schema.ClassName, propName string) bool {
	property, err := s.schema.GetProperty(className, schema.PropertyName(propName))
	if err != nil {
		return false
	}

	dt := schema.DataType(property.DataType[0])
	return dt == schema.DataTypeUUID || dt == schema.DataTypeUUIDArray
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

type docIDsIterator interface {
	Next() (uint64, bool)
	Len() int
}

type sliceDocIDsIterator struct {
	docIDs []uint64
	pos    int
}

func newSliceDocIDsIterator(docIDs []uint64) docIDsIterator {
	return &sliceDocIDsIterator{docIDs: docIDs, pos: 0}
}

func (it *sliceDocIDsIterator) Next() (uint64, bool) {
	if it.pos >= len(it.docIDs) {
		return 0, false
	}
	pos := it.pos
	it.pos++
	return it.docIDs[pos], true
}

func (it *sliceDocIDsIterator) Len() int {
	return len(it.docIDs)
}

type docBitmap struct {
	docIDs   *sroar.Bitmap
	checksum []byte
}

// newUnitializedDocBitmap can be used whenever we can be sure that the first
// user of the docBitmap will set or replace the bitmap, such as a row reader
func newUnitializedDocBitmap() docBitmap {
	return docBitmap{docIDs: nil}
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

func (dbm *docBitmap) IDsWithLimit(limit int) []uint64 {
	card := dbm.docIDs.GetCardinality()
	if limit >= card {
		return dbm.IDs()
	}

	out := make([]uint64, limit)
	for i := range out {
		// safe to ignore error, it can only error if the index is >= cardinality
		// which we have already ruled out
		out[i], _ = dbm.docIDs.Select(uint64(i))
	}

	return out
}

type docPointerWithScore struct {
	id         uint64
	frequency  float32
	propLength float32
}
