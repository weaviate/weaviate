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

	"github.com/semi-technologies/weaviate/entities/models"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/docid"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/schema"
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
	ids []uint64,
) (map[string]aggregation.Property, error) {
	propAggs, err := fa.prepareAggregatorsForProps()
	if err != nil {
		return nil, errors.Wrap(err, "prepare aggregators for props")
	}

	scan := func(properties *models.PropertySchema, docID uint64) (bool, error) {
		if err := fa.analyzeObject(ctx, properties, propAggs); err != nil {
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

func (fa *filteredAggregator) analyzeObject(ctx context.Context,
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
		case schema.DataTypeText, schema.DataTypeString:
			if err := analyzeString(value); err != nil {
				return err
			}
		case schema.DataTypeTextArray, schema.DataTypeStringArray:
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
