//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/sorter"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/tokenizer"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

type Searcher struct {
	logger                 logrus.FieldLogger
	store                  *lsmkv.Store
	getClass               func(string) *models.Class
	classSearcher          ClassSearcher // to allow recursive searches on ref-props
	propIndices            propertyspecific.Indices
	stopwords              stopwords.StopwordDetector
	shardVersion           uint16
	isFallbackToSearchable IsFallbackToSearchable
	tenant                 string
	// nestedCrossRefLimit limits the number of nested cross refs returned for a query
	nestedCrossRefLimit int64
	bitmapFactory       *roaringset.BitmapFactory
}

var ErrOnlyStopwords = fmt.Errorf("invalid search term, only stopwords provided. " +
	"Stopwords can be configured in class.invertedIndexConfig.stopwords")

func NewSearcher(logger logrus.FieldLogger, store *lsmkv.Store,
	getClass func(string) *models.Class, propIndices propertyspecific.Indices,
	classSearcher ClassSearcher, stopwords stopwords.StopwordDetector,
	shardVersion uint16, isFallbackToSearchable IsFallbackToSearchable,
	tenant string, nestedCrossRefLimit int64, bitmapFactory *roaringset.BitmapFactory,
) *Searcher {
	return &Searcher{
		logger:                 logger,
		store:                  store,
		getClass:               getClass,
		propIndices:            propIndices,
		classSearcher:          classSearcher,
		stopwords:              stopwords,
		shardVersion:           shardVersion,
		isFallbackToSearchable: isFallbackToSearchable,
		tenant:                 tenant,
		nestedCrossRefLimit:    nestedCrossRefLimit,
		bitmapFactory:          bitmapFactory,
	}
}

// Objects returns a list of full objects
func (s *Searcher) Objects(ctx context.Context, limit int,
	filter *filters.LocalFilter, sort []filters.Sort, additional additional.Properties,
	className schema.ClassName, properties []string,
	disableInvertedSorter *runtime.DynamicValue[bool],
) ([]*storobj.Object, error) {
	ctx = concurrency.CtxWithBudget(ctx, concurrency.TimesGOMAXPROCS(2))
	beforeFilters := time.Now()
	allowList, err := s.docIDs(ctx, filter, className, limit)
	if err != nil {
		return nil, err
	}
	defer allowList.Close()
	helpers.AnnotateSlowQueryLog(ctx, "build_allow_list_took", time.Since(beforeFilters))
	helpers.AnnotateSlowQueryLog(ctx, "allow_list_doc_ids_count", allowList.Len())

	var it docIDsIterator
	if len(sort) > 0 {
		beforeSort := time.Now()
		docIDs, err := s.sort(ctx, limit, sort, allowList, className, disableInvertedSorter)
		if err != nil {
			return nil, fmt.Errorf("sort doc ids: %w", err)
		}
		helpers.AnnotateSlowQueryLog(ctx, "sort_doc_ids_took", time.Since(beforeSort))
		it = newSliceDocIDsIterator(docIDs)
	} else {
		it = allowList.Iterator()
	}

	beforeObjects := time.Now()
	defer func() {
		helpers.AnnotateSlowQueryLog(ctx, "objects_by_doc_ids_took", time.Since(beforeObjects))
	}()
	return s.objectsByDocID(ctx, it, additional, limit, properties)
}

func (s *Searcher) sort(ctx context.Context, limit int, sort []filters.Sort,
	docIDs helpers.AllowList, className schema.ClassName,
	disableInvertedSorter *runtime.DynamicValue[bool],
) ([]uint64, error) {
	lsmSorter, err := sorter.NewLSMSorter(s.store, s.getClass, className, disableInvertedSorter)
	if err != nil {
		return nil, err
	}
	return lsmSorter.SortDocIDs(ctx, limit, sort, docIDs)
}

func (s *Searcher) objectsByDocID(ctx context.Context, it docIDsIterator,
	additional additional.Properties, limit int, properties []string,
) ([]*storobj.Object, error) {
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, fmt.Errorf("objects bucket not found")
	}

	// Prevent unbounded iteration
	if limit == 0 {
		limit = int(config.DefaultQueryMaximumResults)
	}
	outlen := it.Len()
	if outlen > limit {
		outlen = limit
	}

	out := make([]*storobj.Object, outlen)
	docIDBytes := make([]byte, 8)

	propertyPaths := make([][]string, len(properties))
	for j := range properties {
		propertyPaths[j] = []string{properties[j]}
	}

	props := &storobj.PropertyExtraction{
		PropertyPaths: propertyPaths,
	}

	deletedIds := sroar.NewBitmap()
	deletedCount := 0
	handleDeletedId := func(id uint64) {
		if deletedIds.Set(id) {
			if deletedCount++; deletedCount >= 1024 {
				s.bitmapFactory.Remove(deletedIds)
				deletedIds = sroar.NewBitmap().CloneToBuf(deletedIds.ToBuffer())
				deletedCount = 0
			}
		}
	}
	defer func() {
		if deletedCount > 0 {
			s.bitmapFactory.Remove(deletedIds)
		}
	}()

	i := 0
	loop := 0
	for docID, ok := it.Next(); ok; docID, ok = it.Next() {
		if loop%1000 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		loop++

		binary.LittleEndian.PutUint64(docIDBytes, docID)
		res, err := bucket.GetBySecondary(ctx, 0, docIDBytes)
		if err != nil {
			return nil, err
		}

		if res == nil {
			handleDeletedId(docID)
			continue
		}

		var unmarshalled *storobj.Object
		if additional.ReferenceQuery {
			unmarshalled, err = storobj.FromBinaryUUIDOnly(res)
		} else {
			unmarshalled, err = storobj.FromBinaryOptional(res, additional, props)
		}
		if err != nil {
			return nil, fmt.Errorf("unmarshal data object at position %d: %w", i, err)
		}

		out[i] = unmarshalled
		i++

		if i >= limit {
			break
		}
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
	ctx = concurrency.CtxWithBudget(ctx, concurrency.GOMAXPROCSx2)
	return s.docIDs(ctx, filter, className, 0)
}

func (s *Searcher) DocIDsLimited(ctx context.Context, filter *filters.LocalFilter,
	additional additional.Properties, className schema.ClassName, limit int,
) (helpers.AllowList, error) {
	ctx = concurrency.CtxWithBudget(ctx, concurrency.GOMAXPROCSx2)
	return s.docIDs(ctx, filter, className, max(0, limit))
}

func (s *Searcher) docIDs(ctx context.Context, filter *filters.LocalFilter,
	className schema.ClassName, limit int,
) (helpers.AllowList, error) {
	pv, err := s.extractPropValuePair(ctx, filter.Root, className)
	if err != nil {
		return nil, err
	}

	beforeResolve := time.Now()
	helpers.AnnotateSlowQueryLog(ctx, "build_allow_list_resolve_len", len(pv.children))
	dbm, err := pv.resolveDocIDs(ctx, s, limit)
	if err != nil {
		return nil, fmt.Errorf("resolve doc ids for prop/value pair: %w", err)
	}
	helpers.AnnotateSlowQueryLog(ctx, "build_allow_list_resolve_took", time.Since(beforeResolve))

	return helpers.NewAllowListCloseableFromBitmap(dbm.docIDs, dbm.release), nil
}

func (s *Searcher) extractPropValuePair(
	ctx context.Context, filter *filters.Clause, className schema.ClassName,
) (*propValuePair, error) {
	class := s.getClass(className.String())
	if class == nil {
		return nil, fmt.Errorf("class %q not found", className)
	}
	out, err := newPropValuePair(class)
	if err != nil {
		return nil, fmt.Errorf("new prop value pair: %w", err)
	}
	if filter.Operands != nil {
		// nested filter
		children, err := s.extractPropValuePairs(ctx, filter.Operands, filter.Operator, className)
		if err != nil {
			return nil, err
		}
		out.children = children
		out.operator = filter.Operator
		return out, nil
	}

	switch filter.Operator {
	case filters.ContainsAll, filters.ContainsAny, filters.ContainsNone:
		return s.extractContains(ctx, filter.On, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	default:
		// proceed
	}

	// on value or non-nested filter
	props := filter.On.Slice()
	propName := props[0]

	if s.onInternalProp(propName) {
		return s.extractInternalProp(propName, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	}

	if extractedPropName, ok := schema.IsPropertyLength(propName, 0); ok {
		class := s.getClass(schema.ClassName(className).String())
		if class == nil {
			return nil, fmt.Errorf("could not find class %s in schema", className)
		}

		property, err := schema.GetPropertyByName(class, extractedPropName)
		if err != nil {
			return nil, err
		}
		return s.extractPropertyLength(property, filter.Value.Type, filter.Value.Value, filter.Operator, class)
	}

	property, err := schema.GetPropertyByName(class, propName)
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

func (s *Searcher) extractPropValuePairs(ctx context.Context,
	operands []filters.Clause, operator filters.Operator, className schema.ClassName,
) ([]*propValuePair, error) {
	children := make([]*propValuePair, len(operands))
	eg := enterrors.NewErrorGroupWrapper(s.logger)
	outerConcurrencyLimit := concurrency.BudgetFromCtx(ctx, concurrency.GOMAXPROCS)
	eg.SetLimit(outerConcurrencyLimit)

	concurrencyReductionFactor := min(len(operands), outerConcurrencyLimit)

	for i, clause := range operands {
		i, clause := i, clause
		eg.Go(func() error {
			ctx := concurrency.ContextWithFractionalBudget(ctx, concurrencyReductionFactor, concurrency.GOMAXPROCS)
			child, err := s.extractPropValuePair(ctx, &clause, className)
			// check for stopword errors on ContainsAny operator only at the end
			if err != nil && errors.Is(err, ErrOnlyStopwords) && operator == filters.ContainsAny {
				return nil
			}
			if err != nil {
				return fmt.Errorf("nested clause at pos %d: %w", i, err)
			}
			children[i] = child
			return nil
		}, clause)
	}
	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("nested query: %w", err)
	}
	i := 0
	for _, c := range children {
		if c != nil {
			children[i] = c
			i++
		}
	}
	children = children[:i]

	// if all children were stopwords and the operator is ContainsAny,
	// we return the stopword error anyway for consistency with other
	// filter behaviors
	// The logic is, if at least one of the provided terms is a valid
	// search term (i.e., not a stopword), we proceed with the search,
	// as we can still match on that term.
	// However, if all provided terms are stopwords, we return an error
	// for consistency with other filter behaviors.
	//
	// TODO(amourao): the stopword logic should be rethought globally,
	// as returning errors for specific filter values may generate unexpected
	// results for the end user
	if len(children) == 0 && operator == filters.ContainsAny {
		return nil, ErrOnlyStopwords
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
	hasRangeableIndex := HasRangeableIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex && !hasRangeableIndex {
		return nil, inverted.NewMissingFilterableIndexError(prop.Name)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               prop.Name,
		operator:           operator,
		hasFilterableIndex: hasFilterableIndex,
		hasSearchableIndex: hasSearchableIndex,
		hasRangeableIndex:  hasRangeableIndex,
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

	hasFilterableIndex := HasFilterableIndexMetaCount && HasAnyInvertedIndex(prop)
	hasSearchableIndex := HasSearchableIndexMetaCount && HasAnyInvertedIndex(prop)
	hasRangeableIndex := HasRangeableIndexMetaCount && HasAnyInvertedIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex && !hasRangeableIndex {
		return nil, inverted.NewMissingFilterableMetaCountIndexError(prop.Name)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               helpers.MetaCountProp(prop.Name),
		operator:           operator,
		hasFilterableIndex: hasFilterableIndex,
		hasSearchableIndex: hasSearchableIndex,
		hasRangeableIndex:  hasRangeableIndex,
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
		hasRangeableIndex:  HasRangeableIndex(prop),
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
	hasRangeableIndex := HasRangeableIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex && !hasRangeableIndex {
		return nil, inverted.NewMissingFilterableIndexError(prop.Name)
	}

	return &propValuePair{
		value:              byteValue,
		prop:               prop.Name,
		operator:           operator,
		hasFilterableIndex: hasFilterableIndex,
		hasSearchableIndex: hasSearchableIndex,
		hasRangeableIndex:  hasRangeableIndex,
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
		var asInt64 int64

		switch t := value.(type) {
		case string:
			parsed, err := time.Parse(time.RFC3339, t)
			if err != nil {
				return nil, fmt.Errorf("trying parse time as RFC3339 string: %w", err)
			}
			asInt64 = parsed.UnixMilli()

		case time.Time:
			asInt64 = t.UnixMilli()

		default:
			return nil, fmt.Errorf("expected value to be string or time.Time, got '%T'", value)
		}

		// if propType is a `valueDate`, we need to convert
		// it to ms before fetching. this is the format by
		// which our timestamps are indexed
		byteValue = []byte(strconv.FormatInt(asInt64, 10))
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
			terms = tokenizer.TokenizeWithWildcardsForClass(prop.Tokenization, valueString, class.Class)
		} else {
			terms = tokenizer.TokenizeForClass(prop.Tokenization, valueString, class.Class)
		}
	default:
		return nil, fmt.Errorf("expected value type to be text, got %v", propType)
	}

	hasFilterableIndex := HasFilterableIndex(prop) && !s.isFallbackToSearchable()
	hasSearchableIndex := HasSearchableIndex(prop)
	hasRangeableIndex := HasRangeableIndex(prop)

	if !hasFilterableIndex && !hasSearchableIndex && !hasRangeableIndex {
		return nil, inverted.NewMissingFilterableIndexError(prop.Name)
	}

	propValuePairs := make([]*propValuePair, 0, len(terms))
	for _, term := range terms {
		if s.stopwords.IsStopword(term) && prop.Tokenization == models.PropertyTokenizationWord {
			continue
		}
		propValuePairs = append(propValuePairs, &propValuePair{
			value:              []byte(term),
			prop:               prop.Name,
			operator:           operator,
			hasFilterableIndex: hasFilterableIndex,
			hasSearchableIndex: hasSearchableIndex,
			hasRangeableIndex:  hasRangeableIndex,
			Class:              class,
		})
	}

	if len(propValuePairs) > 1 {
		return &propValuePair{operator: filters.OperatorAnd, children: propValuePairs, Class: class}, nil
	}
	if len(propValuePairs) == 1 {
		return propValuePairs[0], nil
	}
	return nil, ErrOnlyStopwords
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

func (s *Searcher) extractContains(ctx context.Context,
	path *filters.Path, propType schema.DataType, value interface{},
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

	children, err := s.extractPropValuePairs(ctx, operands, operator, schema.ClassName(class.Class))
	if err != nil {
		return nil, err
	}
	out, err := newPropValuePair(class)
	if err != nil {
		return nil, fmt.Errorf("new prop value pair: %w", err)
	}

	out.children = children
	out.Class = class

	switch operator {
	case filters.ContainsAll:
		out.operator = filters.OperatorAnd
	case filters.ContainsAny:
		out.operator = filters.OperatorOr
	case filters.ContainsNone:
		out.operator = filters.OperatorOr

		parent, err := newPropValuePair(class)
		if err != nil {
			return nil, fmt.Errorf("new prop value pair: %w", err)
		}
		parent.operator = filters.OperatorNot
		parent.children = []*propValuePair{out}
		parent.Class = class
		out = parent
	default:
		return nil, fmt.Errorf("unknown contains operator %v", operator)
	}
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
	docIDs  *sroar.Bitmap
	release func()
}

// newUninitializedDocBitmap can be used whenever we can be sure that the first
// user of the docBitmap will set or replace the bitmap, such as a row reader
func newUninitializedDocBitmap() docBitmap {
	return docBitmap{}
}

func newDocBitmap() docBitmap {
	return docBitmap{docIDs: sroar.NewBitmap(), release: func() {}}
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
