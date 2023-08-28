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

package grpc

import (
	"fmt"
	"time"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	pb "github.com/weaviate/weaviate/grpc"
)

func searchParamsFromProto(req *pb.SearchRequest, scheme schema.Schema) (dto.GetParams, error) {
	out := dto.GetParams{}
	_, err := schema.GetClassByName(scheme.Objects, req.ClassName)
	if err != nil {
		return dto.GetParams{}, err
	}

	out.ClassName = req.ClassName

	out.Tenant = req.Tenant

	if req.AdditionalProperties != nil {
		out.AdditionalProperties.ID = req.AdditionalProperties.Uuid
		out.AdditionalProperties.Vector = req.AdditionalProperties.Vector
		out.AdditionalProperties.Distance = req.AdditionalProperties.Distance
		out.AdditionalProperties.LastUpdateTimeUnix = req.AdditionalProperties.LastUpdateTimeUnix
		out.AdditionalProperties.CreationTimeUnix = req.AdditionalProperties.CreationTimeUnix
		out.AdditionalProperties.Score = req.AdditionalProperties.Score
		out.AdditionalProperties.Certainty = req.AdditionalProperties.Certainty
		out.AdditionalProperties.ExplainScore = req.AdditionalProperties.ExplainScore
	}

	out.Properties, err = extractPropertiesRequest(req.Properties, scheme, req.ClassName)
	if err != nil {
		return dto.GetParams{}, err
	}
	if len(out.Properties) == 0 && req.AdditionalProperties != nil {
		// This is a pure-ID query without any props. Indicate this to the DB, so
		// it can optimize accordingly
		out.AdditionalProperties.NoProps = true
	} else if len(out.Properties) == 0 && req.AdditionalProperties == nil {
		// no return values selected, return all properties and metadata. Ignore blobs and refs to not overload the
		// response
		out.AdditionalProperties.ID = true
		out.AdditionalProperties.Vector = true
		out.AdditionalProperties.Distance = true
		out.AdditionalProperties.LastUpdateTimeUnix = true
		out.AdditionalProperties.CreationTimeUnix = true
		out.AdditionalProperties.Score = true
		out.AdditionalProperties.Certainty = true
		out.AdditionalProperties.ExplainScore = true
		returnProps, err := getAllNonRefNonBlobProperties(scheme, req.ClassName)
		if err != nil {
			return dto.GetParams{}, err
		}
		out.Properties = returnProps
	}

	if hs := req.HybridSearch; hs != nil {
		fusionType := common_filters.HybridRankedFusion
		if hs.FusionType == pb.HybridSearchParams_FUSION_TYPE_RELATIVE_SCORE {
			fusionType = common_filters.HybridRelativeScoreFusion
		} else {
			fusionType = common_filters.HybridRankedFusion // default value
		}
		out.HybridSearch = &searchparams.HybridSearch{Query: hs.Query, Properties: hs.Properties, Vector: hs.Vector, Alpha: float64(hs.Alpha), FusionAlgorithm: fusionType}
	}

	if bm25 := req.Bm25Search; bm25 != nil {
		out.KeywordRanking = &searchparams.KeywordRanking{Query: bm25.Query, Properties: bm25.Properties, Type: "bm25", AdditionalExplanations: out.AdditionalProperties.ExplainScore}
	}

	if nv := req.NearVector; nv != nil {
		out.NearVector = &searchparams.NearVector{
			Vector: nv.Vector,
		}

		// The following business logic should not sit in the API. However, it is
		// also part of the GraphQL API, so we need to duplicate it in order to get
		// the same behavior
		if nv.Distance != nil && nv.Certainty != nil {
			return out, fmt.Errorf("near_vector: cannot provide distance and certainty")
		}

		if nv.Certainty != nil {
			out.NearVector.Certainty = *nv.Certainty
		}

		if nv.Distance != nil {
			out.NearVector.Distance = *nv.Distance
			out.NearVector.WithDistance = true
		}
	}

	if no := req.NearObject; no != nil {
		out.NearObject = &searchparams.NearObject{
			ID: req.NearObject.Id,
		}

		// The following business logic should not sit in the API. However, it is
		// also part of the GraphQL API, so we need to duplicate it in order to get
		// the same behavior
		if no.Distance != nil && no.Certainty != nil {
			return out, fmt.Errorf("near_object: cannot provide distance and certainty")
		}

		if no.Certainty != nil {
			out.NearObject.Certainty = *no.Certainty
		}

		if no.Distance != nil {
			out.NearObject.Distance = *no.Distance
			out.NearObject.WithDistance = true
		}
	}

	out.Pagination = &filters.Pagination{Offset: int(req.Offset), Autocut: int(req.Autocut)}
	if req.Limit > 0 {
		out.Pagination.Limit = int(req.Limit)
	} else {
		// TODO: align default with other APIs
		out.Pagination.Limit = 10
	}

	if len(req.After) > 0 {
		out.Cursor = &filters.Cursor{After: req.After, Limit: out.Pagination.Limit}
	}

	if req.Filters != nil {
		clause, err := extractFilters(req.Filters, scheme, req.ClassName)
		if err != nil {
			return dto.GetParams{}, err
		}
		filter := &filters.LocalFilter{Root: &clause}
		if err := filters.ValidateFilters(scheme, filter); err != nil {
			return dto.GetParams{}, err
		}
		out.Filters = filter
	}

	return out, nil
}

func extractFilters(filterIn *pb.Filters, scheme schema.Schema, className string) (filters.Clause, error) {
	returnFilter := filters.Clause{}
	if filterIn.Operator == pb.Filters_OPERATOR_AND || filterIn.Operator == pb.Filters_OPERATOR_OR {
		if filterIn.Operator == pb.Filters_OPERATOR_AND {
			returnFilter.Operator = filters.OperatorAnd
		} else {
			returnFilter.Operator = filters.OperatorOr
		}

		clauses := make([]filters.Clause, len(filterIn.Filters))
		for i, clause := range filterIn.Filters {
			retClause, err := extractFilters(clause, scheme, className)
			if err != nil {
				return filters.Clause{}, err
			}
			clauses[i] = retClause
		}

		returnFilter.Operands = clauses

	} else {
		if len(filterIn.On)%2 != 1 {
			return filters.Clause{}, fmt.Errorf(
				"paths needs to have a uneven number of components: property, class, property, ...., got %v", filterIn.On,
			)
		}
		path, err := extractPath(scheme, className, filterIn.On)
		if err != nil {
			return filters.Clause{}, err
		}
		returnFilter.On = path

		switch filterIn.Operator {
		case pb.Filters_OPERATOR_EQUAL:
			returnFilter.Operator = filters.OperatorEqual
		case pb.Filters_OPERATOR_NOT_EQUAL:
			returnFilter.Operator = filters.OperatorNotEqual
		case pb.Filters_OPERATOR_GREATER_THAN:
			returnFilter.Operator = filters.OperatorGreaterThan
		case pb.Filters_OPERATOR_GREATER_THAN_EQUAL:
			returnFilter.Operator = filters.OperatorGreaterThanEqual
		case pb.Filters_OPERATOR_LESS_THAN:
			returnFilter.Operator = filters.OperatorLessThan
		case pb.Filters_OPERATOR_LESS_THAN_EQUAL:
			returnFilter.Operator = filters.OperatorLessThanEqual
		case pb.Filters_OPERATOR_WITHING_GEO_RANGE:
			returnFilter.Operator = filters.OperatorWithinGeoRange
		case pb.Filters_OPERATOR_LIKE:
			returnFilter.Operator = filters.OperatorLike
		case pb.Filters_OPERATOR_IS_NULL:
			returnFilter.Operator = filters.OperatorIsNull
		case pb.Filters_OPERATOR_CONTAINS_ANY:
			returnFilter.Operator = filters.ContainsAny
		case pb.Filters_OPERATOR_CONTAINS_ALL:
			returnFilter.Operator = filters.ContainsAll

		default:
			return filters.Clause{}, fmt.Errorf("unknown filter operator %v", filterIn.Operator)
		}

		dataType, err := extractDataType(scheme, returnFilter.Operator, className, filterIn.On)
		if err != nil {
			return filters.Clause{}, err
		}

		var val interface{}
		switch filterIn.TestValue.(type) {
		case *pb.Filters_ValueStr:
			val = filterIn.GetValueStr()
		case *pb.Filters_ValueInt:
			val = int(filterIn.GetValueInt())
		case *pb.Filters_ValueBool:
			val = filterIn.GetValueBool()
		case *pb.Filters_ValueFloat:
			val = filterIn.GetValueFloat()
		case *pb.Filters_ValueDate:
			val = filterIn.GetValueDate().AsTime()
		case *pb.Filters_ValueIntArray:
			// convert from int32 GRPC to go int
			valInt32 := filterIn.GetValueIntArray().Vals
			valInt := make([]int, len(valInt32))
			for i := 0; i < len(valInt32); i++ {
				valInt[i] = int(valInt32[i])
			}
			val = valInt
		case *pb.Filters_ValueStrArray:
			val = filterIn.GetValueStrArray().Vals
		case *pb.Filters_ValueFloatArray:
			val = filterIn.GetValueFloatArray().Vals
		case *pb.Filters_ValueBoolArray:
			val = filterIn.GetValueBoolArray().Vals
		case *pb.Filters_ValueDateArray:
			// convert from GRPC timestamp to go time
			valTimestamps := filterIn.GetValueDateArray().Vals
			valTime := make([]time.Time, len(valTimestamps))
			for i := 0; i < len(valTime); i++ {
				valTime[i] = valTimestamps[i].AsTime()
			}
			val = valTime
		default:
			return filters.Clause{}, fmt.Errorf("unknown value type %v", filterIn.TestValue)
		}

		// correct the type of value when filtering on a float property but sending an int. This is easy to get wrong
		if number, ok := val.(int); ok && dataType == schema.DataTypeNumber {
			val = float64(number)
		}

		// correct type for containsXXX in case users send int for a float array
		if (returnFilter.Operator == filters.ContainsAll || returnFilter.Operator == filters.ContainsAny) && dataType == schema.DataTypeNumber {
			valSlice, ok := val.([]int)
			if ok {
				val64 := make([]float64, len(valSlice))
				for i := 0; i < len(valSlice); i++ {
					val64[i] = float64(valSlice[i])
				}
				val = val64
			}
		}

		value := filters.Value{Value: val, Type: dataType}
		returnFilter.Value = &value

	}

	return returnFilter, nil
}

func extractDataType(scheme schema.Schema, operator filters.Operator, classname string, on []string) (schema.DataType, error) {
	var dataType schema.DataType
	if operator == filters.OperatorIsNull {
		dataType = schema.DataTypeBoolean
	} else if len(on) > 1 {
		propToCheck := on[len(on)-1]
		_, isPropLengthFilter := schema.IsPropertyLength(propToCheck, 0)
		if isPropLengthFilter {
			return schema.DataTypeInt, nil
		}

		classOfProp := on[len(on)-2]
		prop, err := scheme.GetProperty(schema.ClassName(classOfProp), schema.PropertyName(propToCheck))
		if err != nil {
			return dataType, err
		}
		return schema.DataType(prop.DataType[0]), nil
	} else {
		propToCheck := on[0]
		_, isPropLengthFilter := schema.IsPropertyLength(propToCheck, 0)
		if isPropLengthFilter {
			return schema.DataTypeInt, nil
		}

		prop, err := scheme.GetProperty(schema.ClassName(classname), schema.PropertyName(propToCheck))
		if err != nil {
			return dataType, err
		}
		dataType = schema.DataType(prop.DataType[0])
	}

	if operator == filters.ContainsAll || operator == filters.ContainsAny {
		if baseType, isArray := schema.IsArrayType(dataType); isArray {
			return baseType, nil
		}
	}
	return dataType, nil
}

func extractPath(scheme schema.Schema, className string, on []string) (*filters.Path, error) {
	var child *filters.Path = nil
	if len(on) > 1 {
		var err error
		child, err = extractPath(scheme, on[1], on[2:])
		if err != nil {
			return nil, err
		}

	}
	return &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(on[0]), Child: child}, nil
}

func extractPropertiesRequest(reqProps *pb.Properties, scheme schema.Schema, className string) ([]search.SelectProperty, error) {
	var props []search.SelectProperty
	if reqProps == nil {
		return props, nil
	}
	if reqProps.NonRefProperties != nil && len(reqProps.NonRefProperties) > 0 {
		for _, prop := range reqProps.NonRefProperties {
			props = append(props, search.SelectProperty{
				Name:        prop,
				IsPrimitive: true,
			})
		}
	}

	if reqProps.RefProperties != nil && len(reqProps.RefProperties) > 0 {
		class := scheme.GetClass(schema.ClassName(className))
		for _, prop := range reqProps.RefProperties {
			schemaProp, err := schema.GetPropertyByName(class, prop.ReferenceProperty)
			if err != nil {
				return nil, err
			}

			var linkedClass string
			if len(schemaProp.DataType) == 1 {
				// use datatype of the reference property to get the name of the linked class
				linkedClass = schemaProp.DataType[0]
			} else {
				linkedClass = prop.WhichCollection
				if linkedClass == "" {
					return nil, fmt.Errorf(
						"multi target references from collection %v and property %v with need an explicit"+
							"linked collection. Available linked collections are %v",
						className, prop.ReferenceProperty, schemaProp.DataType)
				}
			}
			refProperties, err := extractPropertiesRequest(prop.LinkedProperties, scheme, linkedClass)
			if err != nil {
				return nil, err
			}
			props = append(props, search.SelectProperty{
				Name:        prop.ReferenceProperty,
				IsPrimitive: false,
				Refs: []search.SelectClass{{
					ClassName:            linkedClass,
					RefProperties:        refProperties,
					AdditionalProperties: extractAdditionalPropsForRefs(prop.Metadata),
				}},
			})
		}
	}

	return props, nil
}

func extractAdditionalPropsForRefs(prop *pb.AdditionalProperties) additional.Properties {
	return additional.Properties{
		Vector:             prop.Vector,
		Certainty:          prop.Certainty,
		ID:                 prop.Uuid,
		CreationTimeUnix:   prop.CreationTimeUnix,
		LastUpdateTimeUnix: prop.LastUpdateTimeUnix,
		Distance:           prop.Distance,
		Score:              prop.Score,
		ExplainScore:       prop.ExplainScore,
	}
}

func getAllNonRefNonBlobProperties(scheme schema.Schema, className string) ([]search.SelectProperty, error) {
	var props []search.SelectProperty
	class := scheme.GetClass(schema.ClassName(className))

	for _, prop := range class.Properties {
		dt, err := schema.GetPropertyDataType(class, prop.Name)
		if err != nil {
			return []search.SelectProperty{}, err
		}
		if *dt == schema.DataTypeCRef || *dt == schema.DataTypeBlob {
			continue
		}

		props = append(props, search.SelectProperty{
			Name:        prop.Name,
			IsPrimitive: true,
		})

	}

	return props, nil
}
