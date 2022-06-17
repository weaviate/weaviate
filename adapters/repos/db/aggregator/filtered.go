//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package aggregator

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/docid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

type filteredAggregator struct {
	*Aggregator
}

func newFilteredAggregator(agg *Aggregator) *filteredAggregator {
	return &filteredAggregator{Aggregator: agg}
}

func (fa *filteredAggregator) Do(ctx context.Context) (*aggregation.Result, error) {
	out := aggregation.Result{}

	// without grouping there is always exactly one group
	out.Groups = make([]aggregation.Group, 1)

	var (
		allowList helpers.AllowList
		foundIDs  []uint64
		err       error
	)

	if fa.params.Filters != nil {
		s := fa.getSchema.GetSchemaSkipAuth()
		allowList, err = inverted.NewSearcher(fa.store, s, fa.invertedRowCache, nil,
			fa.Aggregator.classSearcher, fa.deletedDocIDs, fa.stopwords, fa.shardVersion).
			DocIDs(ctx, fa.params.Filters, additional.Properties{},
				fa.params.ClassName)
		if err != nil {
			return nil, errors.Wrap(err, "retrieve doc IDs from searcher")
		}
	}

	if len(fa.params.SearchVector) > 0 {
		foundIDs, err = fa.vectorSearch(allowList)
		if err != nil {
			return nil, err
		}
	} else {
		foundIDs = allowList.Slice()
	}

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

func (fa *filteredAggregator) properties(ctx context.Context,
	ids []uint64) (map[string]aggregation.Property, error) {
	propAggs, err := fa.prepareAggregatorsForProps()
	if err != nil {
		return nil, errors.Wrap(err, "prepare aggregators for props")
	}

	scan := func(obj *storobj.Object) (bool, error) {
		if err := fa.analyzeObject(ctx, obj, propAggs); err != nil {
			return false, errors.Wrapf(err, "analyze object %s", obj.ID())
		}
		return true, nil
	}

	err = docid.ScanObjectsLSM(fa.store, ids, scan)
	if err != nil {
		return nil, errors.Wrap(err, "properties view tx")
	}

	return propAggs.results()
}

func (fa *filteredAggregator) analyzeObject(ctx context.Context,
	obj *storobj.Object, propAggs map[string]propAgg) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if obj.Properties() == nil {
		return nil
	}

	for propName, prop := range propAggs {
		value, ok := obj.Properties().(map[string]interface{})[propName]
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
		asBool, ok := value.(bool)
		if !ok {
			return fmt.Errorf("expected property type boolean, received %T", value)
		}
		if err := prop.boolAgg.AddBool(asBool); err != nil {
			return err
		}
	case aggregation.PropertyTypeNumerical:
		asFloat, ok := value.(float64)
		if !ok {
			return fmt.Errorf("expected property type float64, received %T", value)
		}
		if err := prop.numericalAgg.AddFloat64(asFloat); err != nil {
			return err
		}
	case aggregation.PropertyTypeText:
		asString, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected property type string, received %T", value)
		}
		if err := prop.textAgg.AddText(asString); err != nil {
			return err
		}
	case aggregation.PropertyTypeDate:
		asString, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected property type date, received %T", value)
		}
		if err := prop.dateAgg.AddTimestamp(asString); err != nil {
			return err
		}
	default:
	}

	return nil
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
	default:
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
			prop.numericalAgg.buildPairsFromCounts()
			addNumericalAggregations(&aggProp, prop.specifiedAggregators,
				prop.numericalAgg)
			out[prop.name.String()] = aggProp

		case aggregation.PropertyTypeDate:
			prop.dateAgg.buildPairsFromCounts()
			addDateAggregations(&aggProp, prop.specifiedAggregators,
				prop.dateAgg)
			out[prop.name.String()] = aggProp
		default:
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
