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

package aggregator

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/docid"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/traverser/hybrid"
)

type filteredAggregator struct {
	*Aggregator
}

func newFilteredAggregator(agg *Aggregator) *filteredAggregator {
	return &filteredAggregator{Aggregator: agg}
}

func (fa *filteredAggregator) GetPropertyLengthTracker() *inverted.JsonShardMetaData {
	return fa.propLenTracker
}

func (fa *filteredAggregator) Do(ctx context.Context) (*aggregation.Result, error) {
	if fa.params.Hybrid != nil {
		return fa.hybrid(ctx)
	}

	return fa.filtered(ctx)
}

func (fa *filteredAggregator) hybrid(ctx context.Context) (*aggregation.Result, error) {
	sparseSearch := func() ([]*storobj.Object, []float32, error) {
		kw, err := fa.buildHybridKeywordRanking()
		if err != nil {
			return nil, nil, fmt.Errorf("build hybrid keyword ranking: %w", err)
		}

		if fa.params.ObjectLimit == nil {
			limit := hybrid.DefaultLimit
			fa.params.ObjectLimit = &limit
		}

		sparse, dists, err := fa.bm25Objects(ctx, kw)
		if err != nil {
			return nil, nil, fmt.Errorf("aggregate sparse search: %w", err)
		}

		return sparse, dists, nil
	}

	denseSearch := func(vec []float32) ([]*storobj.Object, []float32, error) {
		allowList, err := fa.buildAllowList(ctx)
		if err != nil {
			return nil, nil, err
		}

		res, dists, err := fa.objectVectorSearch(vec, allowList)
		if err != nil {
			return nil, nil, fmt.Errorf("aggregate dense search: %w", err)
		}

		return res, dists, nil
	}

	res, err := hybrid.Search(ctx, &hybrid.Params{
		HybridSearch: fa.params.Hybrid,
		Class:        fa.params.ClassName.String(),
	}, fa.logger, sparseSearch, denseSearch, nil, nil)
	if err != nil {
		return nil, err
	}

	ids := make([]uint64, len(res))
	for i, r := range res {
		ids[i] = r.DocID
	}

	return fa.prepareResult(ctx, ids)
}

func (fa *filteredAggregator) filtered(ctx context.Context) (*aggregation.Result, error) {
	var foundIDs []uint64

	allowList, err := fa.buildAllowList(ctx)
	if err != nil {
		return nil, err
	}

	if len(fa.params.SearchVector) > 0 {
		foundIDs, _, err = fa.vectorSearch(allowList, fa.params.SearchVector)
		if err != nil {
			return nil, err
		}
	} else {
		foundIDs = allowList.Slice()
	}

	return fa.prepareResult(ctx, foundIDs)
}

func (fa *filteredAggregator) bm25Objects(ctx context.Context, kw *searchparams.KeywordRanking) ([]*storobj.Object, []float32, error) {
	var (
		s     = fa.getSchema.GetSchemaSkipAuth()
		class = s.GetClass(fa.params.ClassName)
		cfg   = inverted.ConfigFromModel(class.InvertedIndexConfig)
	)
	objs, dists, err := inverted.NewBM25Searcher(cfg.BM25, fa.store, s,
		propertyspecific.Indices{}, fa.classSearcher,
		fa.GetPropertyLengthTracker(), fa.logger, fa.shardVersion,
	).BM25F(ctx, nil, fa.params.ClassName, *fa.params.ObjectLimit, *kw)
	if err != nil {
		return nil, nil, fmt.Errorf("bm25 objects: %w", err)
	}
	return objs, dists, nil
}

func (fa *filteredAggregator) properties(ctx context.Context,
	ids []uint64,
) (map[string]aggregation.Property, error) {
	propAggs, err := fa.prepareAggregatorsForProps()
	if err != nil {
		return nil, errors.Wrap(err, "prepare aggregators for props")
	}

	scan := func(properties *models.PropertySchema, docID uint64) (bool, error) {
		if err := fa.AnalyzeObject(ctx, properties, propAggs); err != nil {
			return false, errors.Wrapf(err, "analyze object %d", docID)
		}
		return true, nil
	}
	propertyNames := make([]string, 0, len(propAggs))
	for k := range propAggs {
		propertyNames = append(propertyNames, k)
	}

	err = docid.ScanObjectsLSM(fa.store, ids, scan, propertyNames)
	if err != nil {
		return nil, errors.Wrap(err, "properties view tx")
	}

	return propAggs.results()
}

func (fa *filteredAggregator) AnalyzeObject(ctx context.Context,
	properties *models.PropertySchema, propAggs map[string]propAgg,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if properties == nil {
		return nil
	}

	for propName, prop := range propAggs {
		value, ok := (*properties).(map[string]interface{})[propName]
		if !ok {
			continue
		}

		if err := fa.addPropValue(prop, value); err != nil {
			return fmt.Errorf("failed to add prop value: %s", err)
		}
	}

	return nil
}

func (fa *filteredAggregator) addPropValue(prop propAgg, value interface{}) error {
	switch prop.aggType {
	case aggregation.PropertyTypeBoolean:
		analyzeBool := func(value interface{}) error {
			asBool, ok := value.(bool)
			if !ok {
				return fmt.Errorf("expected property type boolean, received %T", value)
			}
			if err := prop.boolAgg.AddBool(asBool); err != nil {
				return err
			}
			return nil
		}
		switch prop.dataType {
		case schema.DataTypeBoolean:
			if err := analyzeBool(value); err != nil {
				return err
			}
		case schema.DataTypeBooleanArray:
			valueStruct, ok := value.([]interface{})
			if !ok {
				return fmt.Errorf("expected property type []boolean, received %T", valueStruct)
			}
			for _, val := range valueStruct {
				if err := analyzeBool(val); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unknown datatype %v for aggregation %v", prop.dataType, aggregation.PropertyTypeText)
		}
	case aggregation.PropertyTypeNumerical:
		analyzeFloat := func(value interface{}) error {
			asFloat, ok := value.(float64)
			if !ok {
				return fmt.Errorf("expected property type float64, received %T", value)
			}
			if err := prop.numericalAgg.AddFloat64(asFloat); err != nil {
				return err
			}
			return nil
		}
		switch prop.dataType {
		case schema.DataTypeNumber, schema.DataTypeInt:
			if err := analyzeFloat(value); err != nil {
				return err
			}
		case schema.DataTypeNumberArray, schema.DataTypeIntArray:
			valueStruct, ok := value.([]interface{})
			if !ok {
				return fmt.Errorf("expected property type []float* or []int*, received %T", valueStruct)
			}
			for _, val := range valueStruct {
				if err := analyzeFloat(val); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unknown datatype %v for aggregation %v", prop.dataType, aggregation.PropertyTypeText)
		}
	case aggregation.PropertyTypeText:
		analyzeString := func(value interface{}) error {
			asString, ok := value.(string)
			if !ok {
				return fmt.Errorf("expected property type string, received %T", value)
			}
			if err := prop.textAgg.AddText(asString); err != nil {
				return err
			}
			return nil
		}
		switch prop.dataType {
		case schema.DataTypeText:
			if err := analyzeString(value); err != nil {
				return err
			}
		case schema.DataTypeTextArray:
			valueStruct, ok := value.([]interface{})
			if !ok {
				return fmt.Errorf("expected property type []text or []string, received %T", valueStruct)
			}
			for _, val := range valueStruct {
				if err := analyzeString(val); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unknown datatype %v for aggregation %v", prop.dataType, aggregation.PropertyTypeText)
		}
	case aggregation.PropertyTypeDate:
		analyzeDate := func(value interface{}) error {
			asString, ok := value.(string)
			if !ok {
				return fmt.Errorf("expected property type date, received %T", value)
			}
			if err := prop.dateAgg.AddTimestamp(asString); err != nil {
				return err
			}
			return nil
		}
		switch prop.dataType {
		case schema.DataTypeDate:
			if err := analyzeDate(value); err != nil {
				return err
			}
		case schema.DataTypeDateArray:
			valueStruct, ok := value.([]interface{})
			if !ok {
				return fmt.Errorf("expected property type []date, received %T", valueStruct)
			}
			for _, val := range valueStruct {
				if err := analyzeDate(val); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unknown datatype %v for aggregation %v", prop.dataType, aggregation.PropertyTypeText)
		}
	case aggregation.PropertyTypeReference:
		if prop.dataType != schema.DataTypeCRef {
			return errors.New(string("unknown datatype for aggregation type reference: " + prop.dataType))
		}

		analyzeRef := func(value interface{}) error {
			referenceList, ok := value.([]interface{})
			if !ok {
				return fmt.Errorf("expected property type reference, received %T", value)
			}
			if len(referenceList) != 1 {
				return fmt.Errorf("expected list with length 1, got %T", len(referenceList))
			}
			refMap, ok := referenceList[0].(map[string]interface{})
			if !ok {
				return fmt.Errorf("expected property type reference, received %T", value)
			}

			if err := prop.refAgg.AddReference(refMap); err != nil {
				return err
			}
			return nil
		}
		if err := analyzeRef(value); err != nil {
			return err
		}
	default:
		return errors.New(string("Unknown aggregation type " + prop.aggType))

	}

	return nil
}

func (fa *filteredAggregator) prepareResult(ctx context.Context, foundIDs []uint64) (*aggregation.Result, error) {
	var out aggregation.Result
	// without grouping there is always exactly one group
	out.Groups = make([]aggregation.Group, 1)

	if fa.params.IncludeMetaCount {
		out.Groups[0].Count = len(foundIDs)
	}

	props, err := fa.properties(ctx, foundIDs)
	if err != nil {
		return nil, errors.Wrap(err, "aggregate properties")
	}

	out.Groups[0].Properties = props
	return &out, nil
}

// a helper type to select the right aggregator for a prop
type propAgg struct {
	name schema.PropertyName

	// the user is interested in those specific aggregations
	specifiedAggregators []aggregation.Aggregator

	// underlying data type of prop
	dataType schema.DataType

	// use aggType to chose with agg to use
	aggType aggregation.PropertyType

	boolAgg      *boolAggregator
	textAgg      *textAggregator
	numericalAgg *numericalAggregator
	dateAgg      *dateAggregator
	refAgg       *refAggregator
}

// propAggs groups propAgg helpers by prop name
type propAggs map[string]propAgg

func (pa *propAgg) initAggregator() {
	switch pa.aggType {
	case aggregation.PropertyTypeText:
		limit := extractLimitFromTopOccs(pa.specifiedAggregators)
		pa.textAgg = newTextAggregator(limit)
	case aggregation.PropertyTypeBoolean:
		pa.boolAgg = newBoolAggregator()
	case aggregation.PropertyTypeNumerical:
		pa.numericalAgg = newNumericalAggregator()
	case aggregation.PropertyTypeDate:
		pa.dateAgg = newDateAggregator()
	case aggregation.PropertyTypeReference:
		pa.refAgg = newRefAggregator()
	default:
		panic("Unknown aggregation type: " + pa.aggType)
	}
}

func (pa propAggs) results() (map[string]aggregation.Property, error) {
	out := map[string]aggregation.Property{}

	for _, prop := range pa {
		aggProp := aggregation.Property{
			Type: prop.aggType,
		}

		switch prop.aggType {
		case aggregation.PropertyTypeBoolean:
			aggProp.BooleanAggregation = prop.boolAgg.Res()
			out[prop.name.String()] = aggProp
		case aggregation.PropertyTypeText:
			aggProp.TextAggregation = prop.textAgg.Res()
			out[prop.name.String()] = aggProp
		case aggregation.PropertyTypeNumerical:
			addNumericalAggregations(&aggProp, prop.specifiedAggregators,
				prop.numericalAgg)
			out[prop.name.String()] = aggProp
		case aggregation.PropertyTypeDate:
			addDateAggregations(&aggProp, prop.specifiedAggregators,
				prop.dateAgg)
			out[prop.name.String()] = aggProp
		case aggregation.PropertyTypeReference:
			addReferenceAggregations(&aggProp, prop.specifiedAggregators,
				prop.refAgg)
			out[prop.name.String()] = aggProp
		default:
			return nil, errors.New(string("unknown aggregation type " + prop.aggType))
		}
	}

	return out, nil
}

func (fa *filteredAggregator) prepareAggregatorsForProps() (propAggs, error) {
	out := propAggs{}

	for _, prop := range fa.params.Properties {
		pa := propAgg{
			name:                 prop.Name,
			specifiedAggregators: prop.Aggregators,
		}

		at, dt, err := fa.aggTypeOfProperty(prop.Name)
		if err != nil {
			return nil, errors.Wrapf(err, "property %s", prop.Name)
		}

		pa.aggType = at
		pa.dataType = dt
		pa.initAggregator()
		out[prop.Name.String()] = pa
	}

	return out, nil
}
