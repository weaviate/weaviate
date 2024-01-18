//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/sorter"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"golang.org/x/sync/errgroup"
)

type Searcher struct {
	logger                 logrus.FieldLogger
	store                  *lsmkv.Store
	schema                 schema.Schema
	classSearcher          ClassSearcher // to allow recursive searches on ref-props
	propIndices            propertyspecific.Indices
	stopwords              stopwords.StopwordDetector
	shardVersion           uint16
	isFallbackToSearchable IsFallbackToSearchable
	tenant                 string
	// nestedCrossRefLimit limits the number of nested cross refs returned for a query
	nestedCrossRefLimit int64
}

func NewSearcher(logger logrus.FieldLogger, store *lsmkv.Store,
	schema schema.Schema, propIndices propertyspecific.Indices,
	classSearcher ClassSearcher, stopwords stopwords.StopwordDetector,
	shardVersion uint16, isFallbackToSearchable IsFallbackToSearchable,
	tenant string, nestedCrossRefLimit int64,
) *Searcher {
	return &Searcher{
		logger:                 logger,
		store:                  store,
		schema:                 schema,
		propIndices:            propIndices,
		classSearcher:          classSearcher,
		stopwords:              stopwords,
		shardVersion:           shardVersion,
		isFallbackToSearchable: isFallbackToSearchable,
		tenant:                 tenant,
		nestedCrossRefLimit:    nestedCrossRefLimit,
	}
}

// Objects returns a list of full objects
func (s *Searcher) Objects(ctx context.Context, limit int,
	filter *filters.LocalFilter, sort []filters.Sort, additional additional.Properties,
	className schema.ClassName,
) ([]*storobj.Object, error) {
	allowList, err := s.docIDs(ctx, filter, additional, className, limit)
	if err != nil {
		return nil, err
	}

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
	return s.docIDs(ctx, filter, additional, className, 0)
}

func (s *Searcher) docIDs(ctx context.Context, filter *filters.LocalFilter,
	additional additional.Properties, className schema.ClassName,
	limit int,
) (helpers.AllowList, error) {
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

	return helpers.NewAllowListFromBitmap(dbm.docIDs), nil
}

func (s *Searcher) extractPropValuePair(filter *filters.Clause,
	className schema.ClassName,
) (*propValuePair, error) {
	class := s.schema.FindClassByName(schema.ClassName(className))
	if class == nil {
		return nil, fmt.Errorf("class %q not found", className)
	}
	out, err := newPropValuePair(class)
	if err != nil {
		return nil, errors.Wrap(err, "new prop value pair")
	}
	if filter.Operands != nil {
		// nested filter
		children, err := s.extractPropValuePairs(filter.Operands, className)
		if err != nil {
			return nil, err
		}
		out.children = children
		out.operator = filter.Operator
		return out, nil
	}

	if filter.Operator == filters.ContainsAny || filter.Operator == filters.ContainsAll {
		return s.extractContains(filter.On, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	}

	// on value or non-nested filter
	props := filter.On.Slice()
	propName := props[0]

	if s.onInternalProp(propName) {
		return s.extractInternalProp(propName, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	}

	if extractedPropName, ok := schema.IsPropertyLength(propName, 0); ok {
		property, err := s.schema.GetProperty(className, schema.PropertyName(extractedPropName))
		if err != nil {
			return nil, err
		}
		return s.extractPropertyLength(property, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	}

	property, err := s.schema.GetProperty(className, schema.PropertyName(propName))
	if err != nil {
		return nil, err
	}

	if s.onRefProp(property) && len(props) != 1 {
		return s.extractReferenceFilter(property, filter, class)
	}

	if s.onRefProp(property) && filter.Value.Type == schema.DataTypeInt {
		// ref prop and int type is a special case, the user is looking for the
		// reference count as opposed to the content
		return s.extractReferenceCount(property, filter.Value.Value, filter.Operator, class)
	}

	if filter.Operator == filters.OperatorIsNull {
		return s.extractPropertyNull(property, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	}

	if s.onGeoProp(property) {
		return s.extractGeoFilter(property, filter.Value.Value, filter.Value.Type, filter.Operator, class)
	}

	if s.onUUIDProp(property) {
		return s.extractUUIDFilter(property, filter.Value.Value, filter.Value.Type, filter.Operator, class)
	}

	if s.onTokenizableProp(property) {
		return s.extractTokenizableProp(property, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	}

	return s.extractPrimitiveProp(property, filter.Value.Type, filter.Value.Value, filter.Operator, class)
}

func (s *Searcher) extractPropValuePairs(operands []filters.Clause, className schema.ClassName) ([]*propValuePair, error) {
	children := make([]*propValuePair, len(operands))
	eg := errgroup.Group{}
	// prevent unbounded concurrency, see
	// https://github.com/weaviate/weaviate/issues/3179 for details
	eg.SetLimit(2 * _NUMCPU)

	for i, clause := range operands {
		i, clause := i, clause
		eg.Go(func() error {
			child, err := s.extractPropValuePair(&clause, className)
			if err != nil {
				return errors.Wrapf(err, "nested clause at pos %d", i)
			}
			children[i] = child

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("nested query: %w", err)
	}
	return children, nil
}

func (s *Searcher) extractReferenceFilter(prop *models.Property,
	filter *filters.Clause, class *models.Class,
) (*propValuePair, error) {
	ctx := context.TODO()
	return newRefFilterExtractor(s.logger, s.classSearcher, filter, class, prop, s.tenant, s.nestedCrossRefLimit).
		Do(ctx)
}

func (s *Searcher) extractPrimitiveProp(prop *models.Property, propType schema.DataType,
	value interface{}, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var extractValueFn func(in interface{}) ([]byte, error)
	switch propType {
	case schema.DataTypeBoolean:
		extractValueFn = s.extractBoolValue
	case schema.DataTypeInt:
		extractValueFn = s.extractIntValue
	case schema.DataTypeNumber:
		extractValueFn = s.extractNumberValue
	case schema.DataTypeDate:
		extractValueFn = s.extractDateValue
	case "":
		return nil, fmt.Errorf("data type cannot be empty")
	default:
		return nil, fmt.Errorf("data type %q not supported in query", propType)
	}

	byteValue, err := extractValueFn(value)
	if err != nil {
		return nil, err
	}

	hasFilterableIndex := HasFilterableIndex(prop)
	hasSearchableIndex := HasSearchableIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex {
		return nil, inverted.NewMissingFilterableIndexError(prop.Name)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               prop.Name,
		operator:           operator,
		hasFilterableIndex: hasFilterableIndex,
		hasSearchableIndex: hasSearchableIndex,
		Class:              class,
	}, nil
}

func (s *Searcher) extractReferenceCount(prop *models.Property, value interface{},
	operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	byteValue, err := s.extractIntCountValue(value)
	if err != nil {
		return nil, err
	}

	hasFilterableIndex := HasFilterableIndexMetaCount && HasInvertedIndex(prop)
	hasSearchableIndex := HasSearchableIndexMetaCount && HasInvertedIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex {
		return nil, inverted.NewMissingFilterableMetaCountIndexError(prop.Name)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               helpers.MetaCountProp(prop.Name),
		operator:           operator,
		hasFilterableIndex: hasFilterableIndex,
		hasSearchableIndex: hasSearchableIndex,
		Class:              class,
	}, nil
}

func (s *Searcher) extractGeoFilter(prop *models.Property, value interface{},
	valueType schema.DataType, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	if valueType != schema.DataTypeGeoCoordinates {
		return nil, fmt.Errorf("prop %q is of type geoCoordinates, it can only"+
			"be used with geoRange filters", prop.Name)
	}

	parsed := value.(filters.GeoRange)

	return &propValuePair{
		value:              nil, // not going to be served by an inverted index
		valueGeoRange:      &parsed,
		prop:               prop.Name,
		operator:           operator,
		hasFilterableIndex: HasFilterableIndex(prop),
		hasSearchableIndex: HasSearchableIndex(prop),
		Class:              class,
	}, nil
}

func (s *Searcher) extractUUIDFilter(prop *models.Property, value interface{},
	valueType schema.DataType, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var byteValue []byte

	switch valueType {
	case schema.DataTypeText:
		asStr, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected to see uuid as string in filter, got %T", value)
		}
		parsed, err := uuid.Parse(asStr)
		if err != nil {
			return nil, fmt.Errorf("parse uuid string: %w", err)
		}
		byteValue = parsed[:]
	default:
		return nil, fmt.Errorf("prop %q is of type uuid, the uuid to filter "+
			"on must be specified as a string (e.g. valueText:<uuid>)", prop.Name)
	}

	hasFilterableIndex := HasFilterableIndex(prop)
	hasSearchableIndex := HasSearchableIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex {
		return nil, inverted.NewMissingFilterableIndexError(prop.Name)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               prop.Name,
		operator:           operator,
		hasFilterableIndex: hasFilterableIndex,
		hasSearchableIndex: hasSearchableIndex,
		Class:              class,
	}, nil
}

func (s *Searcher) extractInternalProp(propName string, propType schema.DataType, value interface{},
	operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	switch propName {
	case filters.InternalPropBackwardsCompatID, filters.InternalPropID:
		return s.extractIDProp(propName, propType, value, operator, class)
	case filters.InternalPropCreationTimeUnix, filters.InternalPropLastUpdateTimeUnix:
		return s.extractTimestampProp(propName, propType, value, operator, class)
	default:
		return nil, fmt.Errorf(
			"failed to extract internal prop, unsupported internal prop '%s'", propName)
	}
}

func (s *Searcher) extractIDProp(propName string, propType schema.DataType,
	value interface{}, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var byteValue []byte

	switch propType {
	case schema.DataTypeText:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected value to be string, got '%T'", value)
		}
		byteValue = []byte(v)
	default:
		return nil, fmt.Errorf(
			"failed to extract id prop, unsupported type '%T' for prop '%s'", propType, propName)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               filters.InternalPropID,
		operator:           operator,
		hasFilterableIndex: HasFilterableIndexIdProp,
		hasSearchableIndex: HasSearchableIndexIdProp,
		Class:              class,
	}, nil
}

func (s *Searcher) extractTimestampProp(propName string, propType schema.DataType, value interface{},
	operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var byteValue []byte

	switch propType {
	case schema.DataTypeText:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected value to be string, got '%T'", value)
		}
		_, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("expected value to be timestamp, got '%s'", v)
		}
		byteValue = []byte(v)
	case schema.DataTypeDate:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected value to be string, got '%T'", value)
		}
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, errors.Wrap(err, "trying parse time as RFC3339 string")
		}

		// if propType is a `valueDate`, we need to convert
		// it to ms before fetching. this is the format by
		// which our timestamps are indexed
		byteValue = []byte(strconv.FormatInt(t.UnixMilli(), 10))
	default:
		return nil, fmt.Errorf(
			"failed to extract timestamp prop, unsupported type '%T' for prop '%s'", propType, propName)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               propName,
		operator:           operator,
		hasFilterableIndex: HasFilterableIndexTimestampProp, // TODO text_rbm_inverted_index & with settings
		hasSearchableIndex: HasSearchableIndexTimestampProp, // TODO text_rbm_inverted_index & with settings
		Class:              class,
	}, nil
}

func (s *Searcher) extractTokenizableProp(prop *models.Property, propType schema.DataType,
	value interface{}, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var terms []string

	valueString, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected value to be string, got '%T'", value)
	}

	switch propType {
	case schema.DataTypeText:
		// if the operator is like, we cannot apply the regular text-splitting
		// logic as it would remove all wildcard symbols
		if operator == filters.OperatorLike {
			terms = helpers.TokenizeWithWildcards(prop.Tokenization, valueString)
		} else {
			terms = helpers.Tokenize(prop.Tokenization, valueString)
		}
	default:
		return nil, fmt.Errorf("expected value type to be text, got %v", propType)
	}

	hasFilterableIndex := HasFilterableIndex(prop) && !s.isFallbackToSearchable()
	hasSearchableIndex := HasSearchableIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex {
		return nil, inverted.NewMissingFilterableIndexError(prop.Name)
	}

	propValuePairs := make([]*propValuePair, 0, len(terms))
	for _, term := range terms {
		if s.stopwords.IsStopword(term) {
			continue
		}
		propValuePairs = append(propValuePairs, &propValuePair{
			value:              []byte(term),
			prop:               prop.Name,
			operator:           operator,
			hasFilterableIndex: hasFilterableIndex,
			hasSearchableIndex: hasSearchableIndex,
			Class:              class,
		})
	}

	if len(propValuePairs) > 1 {
		return &propValuePair{operator: filters.OperatorAnd, children: propValuePairs, Class: class}, nil
	}
	if len(propValuePairs) == 1 {
		return propValuePairs[0], nil
	}
	return nil, errors.Errorf("invalid search term, only stopwords provided. Stopwords can be configured in class.invertedIndexConfig.stopwords")
}

func (s *Searcher) extractPropertyLength(prop *models.Property, propType schema.DataType,
	value interface{}, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var byteValue []byte

	switch propType {
	case schema.DataTypeInt:
		b, err := s.extractIntValue(value)
		if err != nil {
			return nil, err
		}
		byteValue = b
	default:
		return nil, fmt.Errorf(
			"failed to extract length of prop, unsupported type '%T' for length of prop '%s'", propType, prop.Name)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               helpers.PropLength(prop.Name),
		operator:           operator,
		hasFilterableIndex: HasFilterableIndexPropLength, // TODO text_rbm_inverted_index & with settings
		hasSearchableIndex: HasSearchableIndexPropLength, // TODO text_rbm_inverted_index & with settings
		Class:              class,
	}, nil
}

func (s *Searcher) extractPropertyNull(prop *models.Property, propType schema.DataType,
	value interface{}, operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var valResult []byte

	switch propType {
	case schema.DataTypeBoolean:
		b, err := s.extractBoolValue(value)
		if err != nil {
			return nil, err
		}
		valResult = b
	default:
		return nil, fmt.Errorf(
			"failed to extract null prop, unsupported type '%T' for null prop '%s'", propType, prop.Name)
	}

	return &propValuePair{
		value:              valResult,
		prop:               helpers.PropNull(prop.Name),
		operator:           operator,
		hasFilterableIndex: HasFilterableIndexPropNull, // TODO text_rbm_inverted_index & with settings
		hasSearchableIndex: HasSearchableIndexPropNull, // TODO text_rbm_inverted_index & with settings
		Class:              class,
	}, nil
}

func (s *Searcher) extractContains(path *filters.Path, propType schema.DataType, value interface{},
	operator filters.Operator, class *models.Class,
) (*propValuePair, error) {
	var operands []filters.Clause
	switch propType {
	case schema.DataTypeText, schema.DataTypeTextArray:
		valueStringArray, err := s.extractStringArray(value)
		if err != nil {
			return nil, err
		}
		operands = getContainsOperands(propType, path, valueStringArray)
	case schema.DataTypeInt, schema.DataTypeIntArray:
		valueIntArray, err := s.extractIntArray(value)
		if err != nil {
			return nil, err
		}
		operands = getContainsOperands(propType, path, valueIntArray)
	case schema.DataTypeNumber, schema.DataTypeNumberArray:
		valueFloat64Array, err := s.extractFloat64Array(value)
		if err != nil {
			return nil, err
		}
		operands = getContainsOperands(propType, path, valueFloat64Array)
	case schema.DataTypeBoolean, schema.DataTypeBooleanArray:
		valueBooleanArray, err := s.extractBoolArray(value)
		if err != nil {
			return nil, err
		}
		operands = getContainsOperands(propType, path, valueBooleanArray)
	case schema.DataTypeDate, schema.DataTypeDateArray:
		valueDateArray, err := s.extractStringArray(value)
		if err != nil {
			return nil, err
		}
		operands = getContainsOperands(propType, path, valueDateArray)
	default:
		return nil, fmt.Errorf("unsupported type '%T' for '%v' operator", propType, operator)
	}

	children, err := s.extractPropValuePairs(operands, schema.ClassName(class.Class))
	if err != nil {
		return nil, err
	}
	out, err := newPropValuePair(class)
	if err != nil {
		return nil, errors.Wrap(err, "new prop value pair")
	}
	out.children = children
	// filters.ContainsAny
	out.operator = filters.OperatorOr
	if operator == filters.ContainsAll {
		out.operator = filters.OperatorAnd
	}
	out.Class = class
	return out, nil
}

// TODO: repeated calls to on... aren't too efficient because we iterate over
// the schema each time, might be smarter to have a single method that
// determines the type and then we switch based on the result. However, the
// effect of that should be very small unless the schema is absolutely massive.
func (s *Searcher) onRefProp(property *models.Property) bool {
	return schema.IsRefDataType(property.DataType)
}

// TODO: repeated calls to on... aren't too efficient because we iterate over
// the schema each time, might be smarter to have a single method that
// determines the type and then we switch based on the result. However, the
// effect of that should be very small unless the schema is absolutely massive.
func (s *Searcher) onGeoProp(prop *models.Property) bool {
	return schema.DataType(prop.DataType[0]) == schema.DataTypeGeoCoordinates
}

// Note: A UUID prop is a user-specified prop of type UUID. This has nothing to
// do with the primary ID of an object which happens to always be a UUID in
// Weaviate v1
//
// TODO: repeated calls to on... aren't too efficient because we iterate over
// the schema each time, might be smarter to have a single method that
// determines the type and then we switch based on the result. However, the
// effect of that should be very small unless the schema is absolutely massive.
func (s *Searcher) onUUIDProp(prop *models.Property) bool {
	switch dt, _ := schema.AsPrimitive(prop.DataType); dt {
	case schema.DataTypeUUID, schema.DataTypeUUIDArray:
		return true
	default:
		return false
	}
}

func (s *Searcher) onInternalProp(propName string) bool {
	return filters.IsInternalProperty(schema.PropertyName(propName))
}

func (s *Searcher) onTokenizableProp(prop *models.Property) bool {
	switch dt, _ := schema.AsPrimitive(prop.DataType); dt {
	case schema.DataTypeText, schema.DataTypeTextArray:
		return true
	default:
		return false
	}
}

func (s *Searcher) extractStringArray(value interface{}) ([]string, error) {
	switch v := value.(type) {
	case []string:
		return v, nil
	case []interface{}:
		vals := make([]string, len(v))
		for i := range v {
			val, ok := v[i].(string)
			if !ok {
				return nil, fmt.Errorf("value[%d] type should be string but is %T", i, v[i])
			}
			vals[i] = val
		}
		return vals, nil
	default:
		return nil, fmt.Errorf("value type should be []string but is %T", value)
	}
}

func (s *Searcher) extractIntArray(value interface{}) ([]int, error) {
	switch v := value.(type) {
	case []int:
		return v, nil
	case []interface{}:
		vals := make([]int, len(v))
		for i := range v {
			// in this case all number values are unmarshalled to float64, so we need to cast to float64
			// and then make int
			val, ok := v[i].(float64)
			if !ok {
				return nil, fmt.Errorf("value[%d] type should be float64 but is %T", i, v[i])
			}
			vals[i] = int(val)
		}
		return vals, nil
	default:
		return nil, fmt.Errorf("value type should be []int but is %T", value)
	}
}

func (s *Searcher) extractFloat64Array(value interface{}) ([]float64, error) {
	switch v := value.(type) {
	case []float64:
		return v, nil
	case []interface{}:
		vals := make([]float64, len(v))
		for i := range v {
			val, ok := v[i].(float64)
			if !ok {
				return nil, fmt.Errorf("value[%d] type should be float64 but is %T", i, v[i])
			}
			vals[i] = val
		}
		return vals, nil
	default:
		return nil, fmt.Errorf("value type should be []float64 but is %T", value)
	}
}

func (s *Searcher) extractBoolArray(value interface{}) ([]bool, error) {
	switch v := value.(type) {
	case []bool:
		return v, nil
	case []interface{}:
		vals := make([]bool, len(v))
		for i := range v {
			val, ok := v[i].(bool)
			if !ok {
				return nil, fmt.Errorf("value[%d] type should be bool but is %T", i, v[i])
			}
			vals[i] = val
		}
		return vals, nil
	default:
		return nil, fmt.Errorf("value type should be []bool but is %T", value)
	}
}

func getContainsOperands[T any](propType schema.DataType, path *filters.Path, values []T) []filters.Clause {
	operands := make([]filters.Clause, len(values))
	for i := range values {
		operands[i] = filters.Clause{
			Operator: filters.OperatorEqual,
			On:       path,
			Value: &filters.Value{
				Type:  propType,
				Value: values[i],
			},
		}
	}
	return operands
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
	docIDs *sroar.Bitmap
}

// newUninitializedDocBitmap can be used whenever we can be sure that the first
// user of the docBitmap will set or replace the bitmap, such as a row reader
func newUninitializedDocBitmap() docBitmap {
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
