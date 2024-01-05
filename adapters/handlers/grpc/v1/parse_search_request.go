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

package v1

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/byteops"

	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/rank"

	"github.com/weaviate/weaviate/usecases/modulecomponents/nearVideo"

	"github.com/weaviate/weaviate/usecases/modulecomponents/nearAudio"

	"github.com/weaviate/weaviate/usecases/modulecomponents/nearImage"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	nearText2 "github.com/weaviate/weaviate/usecases/modulecomponents/nearText"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/entities/schema/crossref"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func searchParamsFromProto(req *pb.SearchRequest, scheme schema.Schema) (dto.GetParams, error) {
	out := dto.GetParams{}
	class, err := schema.GetClassByName(scheme.Objects, req.Collection)
	if err != nil {
		return dto.GetParams{}, err
	}

	out.ClassName = req.Collection
	out.ReplicationProperties = extractReplicationProperties(req.ConsistencyLevel)

	out.Tenant = req.Tenant

	if req.Metadata != nil {
		addProps, err := extractAdditionalPropsFromMetadata(class, req.Metadata)
		if err != nil {
			return dto.GetParams{}, errors.Wrap(err, "extract additional props")
		}
		out.AdditionalProperties = addProps
	}

	out.Properties, err = extractPropertiesRequest(req.Properties, scheme, req.Collection, req.Uses_123Api)
	if err != nil {
		return dto.GetParams{}, errors.Wrap(err, "extract properties request")
	}
	if len(out.Properties) == 0 {
		out.AdditionalProperties.NoProps = true
	}

	if hs := req.HybridSearch; hs != nil {
		fusionType := common_filters.HybridFusionDefault
		if hs.FusionType == pb.Hybrid_FUSION_TYPE_RANKED {
			fusionType = common_filters.HybridRankedFusion
		} else if hs.FusionType == pb.Hybrid_FUSION_TYPE_RELATIVE_SCORE {
			fusionType = common_filters.HybridRelativeScoreFusion
		}

		var vector []float32
		// bytes vector has precedent for being more efficient
		if len(hs.VectorBytes) > 0 {
			vector = byteops.Float32FromByteVector(hs.VectorBytes)
		} else if len(hs.Vector) > 0 {
			vector = hs.Vector
		}

		out.HybridSearch = &searchparams.HybridSearch{Query: hs.Query, Properties: schema.LowercaseFirstLetterOfStrings(hs.Properties), Vector: vector, Alpha: float64(hs.Alpha), FusionAlgorithm: fusionType}
	}

	if bm25 := req.Bm25Search; bm25 != nil {
		out.KeywordRanking = &searchparams.KeywordRanking{Query: bm25.Query, Properties: schema.LowercaseFirstLetterOfStrings(bm25.Properties), Type: "bm25", AdditionalExplanations: out.AdditionalProperties.ExplainScore}
	}

	if nv := req.NearVector; nv != nil {
		var vector []float32
		// bytes vector has precedent for being more efficient
		if len(nv.VectorBytes) > 0 {
			vector = byteops.Float32FromByteVector(nv.VectorBytes)
		} else {
			vector = nv.Vector
		}
		out.NearVector = &searchparams.NearVector{
			Vector: vector,
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

	if ni := req.NearImage; ni != nil {
		nearImageOut, err := parseNearImage(ni)
		if err != nil {
			return dto.GetParams{}, err
		}

		if out.ModuleParams == nil {
			out.ModuleParams = make(map[string]interface{})
		}
		out.ModuleParams["nearImage"] = nearImageOut
	}

	if na := req.NearAudio; na != nil {
		nearAudioOut, err := parseNearAudio(na)
		if err != nil {
			return dto.GetParams{}, err
		}
		if out.ModuleParams == nil {
			out.ModuleParams = make(map[string]interface{})
		}
		out.ModuleParams["nearAudio"] = nearAudioOut
	}

	if nv := req.NearVideo; nv != nil {
		nearVideoOut, err := parseNearVideo(nv)
		if err != nil {
			return dto.GetParams{}, err
		}
		if out.ModuleParams == nil {
			out.ModuleParams = make(map[string]interface{})
		}
		out.ModuleParams["nearVideo"] = nearVideoOut
	}

	out.Pagination = &filters.Pagination{Offset: int(req.Offset), Autocut: int(req.Autocut)}
	if req.Limit > 0 {
		out.Pagination.Limit = int(req.Limit)
	} else {
		// TODO: align default with other APIs
		out.Pagination.Limit = 10
	}

	if req.NearText != nil {
		moveAwayOut, err := extractNearTextMove(req.Collection, req.NearText.MoveAway)
		if err != nil {
			return dto.GetParams{}, err
		}
		moveToOut, err := extractNearTextMove(req.Collection, req.NearText.MoveTo)
		if err != nil {
			return dto.GetParams{}, err
		}

		nearText := &nearText2.NearTextParams{
			Values:       req.NearText.Query,
			Limit:        out.Pagination.Limit,
			MoveAwayFrom: moveAwayOut,
			MoveTo:       moveToOut,
		}

		if req.NearText.Certainty != nil {
			nearText.Certainty = *req.NearText.Certainty
		}
		if req.NearText.Distance != nil {
			nearText.Distance = *req.NearText.Distance
			nearText.WithDistance = true
		}
		if out.ModuleParams == nil {
			out.ModuleParams = make(map[string]interface{})
		}
		out.ModuleParams["nearText"] = nearText
	}

	if req.Generative != nil {
		if out.AdditionalProperties.ModuleParams == nil {
			out.AdditionalProperties.ModuleParams = make(map[string]interface{})
		}
		out.AdditionalProperties.ModuleParams["generate"] = extractGenerative(req)
	}

	if req.Rerank != nil {
		if out.AdditionalProperties.ModuleParams == nil {
			out.AdditionalProperties.ModuleParams = make(map[string]interface{})
		}
		out.AdditionalProperties.ModuleParams["rerank"] = extractRerank(req)
	}

	if len(req.After) > 0 {
		out.Cursor = &filters.Cursor{After: req.After, Limit: out.Pagination.Limit}
	}

	if req.Filters != nil {
		clause, err := extractFilters(req.Filters, scheme, req.Collection)
		if err != nil {
			return dto.GetParams{}, err
		}
		filter := &filters.LocalFilter{Root: &clause}
		if err := filters.ValidateFilters(scheme, filter); err != nil {
			return dto.GetParams{}, err
		}
		out.Filters = filter
	}

	if len(req.SortBy) > 0 {
		if req.NearText != nil || req.NearVideo != nil || req.NearAudio != nil || req.NearImage != nil || req.NearObject != nil || req.NearVector != nil || req.HybridSearch != nil || req.Bm25Search != nil || req.Generative != nil {
			return dto.GetParams{}, errors.New("sorting cannot be combined with search")
		}
		out.Sort = extractSorting(req.SortBy)
	}

	if req.GroupBy != nil {
		groupBy, err := extractGroupBy(req.GroupBy, &out)
		if err != nil {
			return dto.GetParams{}, err
		}
		out.AdditionalProperties.Group = true

		out.GroupBy = groupBy
	}

	return out, nil
}

func extractGroupBy(groupIn *pb.GroupBy, out *dto.GetParams) (*searchparams.GroupBy, error) {
	if len(groupIn.Path) != 1 {
		return nil, fmt.Errorf("groupby path can only have one entry, received %v", groupIn.Path)
	}

	groupOut := &searchparams.GroupBy{
		Property:        groupIn.Path[0],
		ObjectsPerGroup: int(groupIn.ObjectsPerGroup),
		Groups:          int(groupIn.NumberOfGroups),
	}

	// add the property in case it was not requested as return prop - otherwise it is not resolved
	if out.Properties.FindProperty(groupOut.Property) == nil {
		out.Properties = append(out.Properties, search.SelectProperty{Name: groupOut.Property, IsPrimitive: true})
	}
	out.AdditionalProperties.NoProps = false

	return groupOut, nil
}

func extractSorting(sortIn []*pb.SortBy) []filters.Sort {
	sortOut := make([]filters.Sort, len(sortIn))
	for i := range sortIn {
		order := "asc"
		if !sortIn[i].Ascending {
			order = "desc"
		}
		sortOut[i] = filters.Sort{Order: order, Path: sortIn[i].Path}
	}
	return sortOut
}

func extractGenerative(req *pb.SearchRequest) *generate.Params {
	generative := generate.Params{}
	if req.Generative.SingleResponsePrompt != "" {
		generative.Prompt = &req.Generative.SingleResponsePrompt
	}
	if req.Generative.GroupedResponseTask != "" {
		generative.Task = &req.Generative.GroupedResponseTask
	}
	if len(req.Generative.GroupedProperties) > 0 {
		generative.Properties = req.Generative.GroupedProperties
	}
	return &generative
}

func extractRerank(req *pb.SearchRequest) *rank.Params {
	rerank := rank.Params{
		Property: &req.Rerank.Property,
	}
	if req.Rerank.Query != nil {
		rerank.Query = req.Rerank.Query
	}
	return &rerank
}

func extractNearTextMove(classname string, Move *pb.NearTextSearch_Move) (nearText2.ExploreMove, error) {
	var moveAwayOut nearText2.ExploreMove

	if moveAwayReq := Move; moveAwayReq != nil {
		moveAwayOut.Force = moveAwayReq.Force
		if moveAwayReq.Uuids != nil && len(moveAwayReq.Uuids) > 0 {
			moveAwayOut.Objects = make([]nearText2.ObjectMove, len(moveAwayReq.Uuids))
			for i, objUUid := range moveAwayReq.Uuids {
				uuidFormat, err := uuid.Parse(objUUid)
				if err != nil {
					return moveAwayOut, err
				}
				moveAwayOut.Objects[i] = nearText2.ObjectMove{
					ID:     objUUid,
					Beacon: crossref.NewLocalhost(classname, strfmt.UUID(uuidFormat.String())).String(),
				}
			}
		}

		moveAwayOut.Values = moveAwayReq.Concepts
	}
	return moveAwayOut, nil
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
		case pb.Filters_OPERATOR_WITHIN_GEO_RANGE:
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

		// datatype UUID is just a string
		if dataType == schema.DataTypeUUID {
			dataType = schema.DataTypeText
		}

		var val interface{}
		switch filterIn.TestValue.(type) {
		case *pb.Filters_ValueText:
			val = filterIn.GetValueText()
		case *pb.Filters_ValueInt:
			val = int(filterIn.GetValueInt())
		case *pb.Filters_ValueBoolean:
			val = filterIn.GetValueBoolean()
		case *pb.Filters_ValueNumber:
			val = filterIn.GetValueNumber()
		case *pb.Filters_ValueIntArray:
			// convert from int32 GRPC to go int
			valInt32 := filterIn.GetValueIntArray().Values
			valInt := make([]int, len(valInt32))
			for i := 0; i < len(valInt32); i++ {
				valInt[i] = int(valInt32[i])
			}
			val = valInt
		case *pb.Filters_ValueTextArray:
			val = filterIn.GetValueTextArray().Values
		case *pb.Filters_ValueNumberArray:
			val = filterIn.GetValueNumberArray().Values
		case *pb.Filters_ValueBooleanArray:
			val = filterIn.GetValueBooleanArray().Values
		case *pb.Filters_ValueGeo:
			valueFilter := filterIn.GetValueGeo()
			val = filters.GeoRange{
				GeoCoordinates: &models.GeoCoordinates{
					Latitude:  &valueFilter.Latitude,
					Longitude: &valueFilter.Longitude,
				},
				Distance: valueFilter.Distance,
			}
		default:
			return filters.Clause{}, fmt.Errorf("unknown value type %v", filterIn.TestValue)
		}

		// correct the type of value when filtering on a float/int property but sending an int/float. This is easy to
		// get wrong
		if number, ok := val.(int); ok && dataType == schema.DataTypeNumber {
			val = float64(number)
		}
		if number, ok := val.(float64); ok && dataType == schema.DataTypeInt {
			val = int(number)
			if float64(int(number)) != number {
				return filters.Clause{}, fmt.Errorf("filtering for integer, but received a floating point number %v", number)
			}
		}

		// correct type for containsXXX in case users send int/float for a float/int array
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

		if (returnFilter.Operator == filters.ContainsAll || returnFilter.Operator == filters.ContainsAny) && dataType == schema.DataTypeInt {
			valSlice, ok := val.([]float64)
			if ok {
				valInt := make([]int, len(valSlice))
				for i := 0; i < len(valSlice); i++ {
					if float64(int(valSlice[i])) != valSlice[i] {
						return filters.Clause{}, fmt.Errorf("filtering for integer, but received a floating point number %v", valSlice[i])
					}
					valInt[i] = int(valSlice[i])
				}
				val = valInt
			}
		}

		value := filters.Value{Value: val, Type: dataType}
		returnFilter.Value = &value

	}

	return returnFilter, nil
}

func extractDataTypeProperty(scheme schema.Schema, operator filters.Operator, classname string, on []string) (schema.DataType, error) {
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
		dataType = schema.DataType(prop.DataType[0])
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

	// searches on array datatypes always need the base-type as value-type
	if baseType, isArray := schema.IsArrayType(dataType); isArray {
		return baseType, nil
	}
	return dataType, nil
}

func extractDataType(scheme schema.Schema, operator filters.Operator, classname string, on []string) (schema.DataType, error) {
	propToFilterOn := on[len(on)-1]
	if propToFilterOn == filters.InternalPropID {
		return schema.DataTypeText, nil
	} else if propToFilterOn == filters.InternalPropCreationTimeUnix || propToFilterOn == filters.InternalPropLastUpdateTimeUnix {
		return schema.DataTypeDate, nil
	} else {
		return extractDataTypeProperty(scheme, operator, classname, on)
	}
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

func extractPropertiesRequest(reqProps *pb.PropertiesRequest, scheme schema.Schema, className string, usesNewDefaultLogic bool) ([]search.SelectProperty, error) {
	props := make([]search.SelectProperty, 0)

	if reqProps == nil {
		// No properties selected at all, return all non-ref properties.
		// Ignore blobs to not overload the response
		nonRefProps, err := getAllNonRefNonBlobProperties(scheme, className)
		if err != nil {
			return nil, errors.Wrap(err, "get all non ref non blob properties")
		}
		return nonRefProps, nil
	}

	if !usesNewDefaultLogic {
		// Old stubs being used, use deprecated method
		return extractPropertiesRequestDeprecated(reqProps, scheme, className)
	}

	if reqProps.ReturnAllNonrefProperties {
		// No non-ref return properties selected, return all non-ref properties.
		// Ignore blobs to not overload the response
		returnProps, err := getAllNonRefNonBlobProperties(scheme, className)
		if err != nil {
			return nil, errors.Wrap(err, "get all non ref non blob properties")
		}
		props = append(props, returnProps...)
	} else if len(reqProps.NonRefProperties) > 0 {
		// Non-ref properties are selected, return only those specified
		// This catches the case where users send an empty list of non ref properties as their request,
		// i.e. they want no non-ref properties
		for _, prop := range reqProps.NonRefProperties {
			props = append(props, search.SelectProperty{
				Name:        schema.LowercaseFirstLetter(prop),
				IsPrimitive: true,
				IsObject:    false,
			})
		}
	}

	if len(reqProps.RefProperties) > 0 {
		class := scheme.GetClass(schema.ClassName(className))
		for _, prop := range reqProps.RefProperties {
			normalizedRefPropName := schema.LowercaseFirstLetter(prop.ReferenceProperty)
			schemaProp, err := schema.GetPropertyByName(class, normalizedRefPropName)
			if err != nil {
				return nil, err
			}

			var linkedClassName string
			if len(schemaProp.DataType) == 1 {
				// use datatype of the reference property to get the name of the linked class
				linkedClassName = schemaProp.DataType[0]
			} else {
				linkedClassName = prop.TargetCollection
				if linkedClassName == "" {
					return nil, fmt.Errorf(
						"multi target references from collection %v and property %v with need an explicit"+
							"linked collection. Available linked collections are %v",
						className, prop.ReferenceProperty, schemaProp.DataType)
				}
			}
			var refProperties []search.SelectProperty
			var addProps additional.Properties
			if prop.Properties != nil {
				refProperties, err = extractPropertiesRequest(prop.Properties, scheme, linkedClassName, usesNewDefaultLogic)
				if err != nil {
					return nil, errors.Wrap(err, "extract properties request")
				}
			}
			if prop.Metadata != nil {
				addProps, err = extractAdditionalPropsFromMetadata(class, prop.Metadata)
				if err != nil {
					return nil, errors.Wrap(err, "extract additional props for refs")
				}
			}

			if prop.Properties == nil {
				refProperties, err = getAllNonRefNonBlobProperties(scheme, linkedClassName)
				if err != nil {
					return nil, errors.Wrap(err, "get all non ref non blob properties")
				}
			}
			if len(refProperties) == 0 && isIdOnlyRequest(prop.Metadata) {
				// This is a pure-ID query without any properties or additional metadata.
				// Indicate this to the DB, so it can optimize accordingly
				addProps.NoProps = true
			}

			props = append(props, search.SelectProperty{
				Name:        normalizedRefPropName,
				IsPrimitive: false,
				IsObject:    false,
				Refs: []search.SelectClass{{
					ClassName:            linkedClassName,
					RefProperties:        refProperties,
					AdditionalProperties: addProps,
				}},
			})
		}
	}

	if len(reqProps.ObjectProperties) > 0 {
		props = append(props, extractNestedProperties(reqProps.ObjectProperties)...)
	}

	return props, nil
}

func extractPropertiesRequestDeprecated(reqProps *pb.PropertiesRequest, scheme schema.Schema, className string) ([]search.SelectProperty, error) {
	if reqProps == nil {
		return nil, nil
	}
	props := make([]search.SelectProperty, 0)
	if reqProps.NonRefProperties != nil && len(reqProps.NonRefProperties) > 0 {
		for _, prop := range reqProps.NonRefProperties {
			props = append(props, search.SelectProperty{
				Name:        schema.LowercaseFirstLetter(prop),
				IsPrimitive: true,
				IsObject:    false,
			})
		}
	}

	if reqProps.RefProperties != nil && len(reqProps.RefProperties) > 0 {
		class := scheme.GetClass(schema.ClassName(className))
		for _, prop := range reqProps.RefProperties {
			normalizedRefPropName := schema.LowercaseFirstLetter(prop.ReferenceProperty)
			schemaProp, err := schema.GetPropertyByName(class, normalizedRefPropName)
			if err != nil {
				return nil, err
			}

			var linkedClassName string
			if len(schemaProp.DataType) == 1 {
				// use datatype of the reference property to get the name of the linked class
				linkedClassName = schemaProp.DataType[0]
			} else {
				linkedClassName = prop.TargetCollection
				if linkedClassName == "" {
					return nil, fmt.Errorf(
						"multi target references from collection %v and property %v with need an explicit"+
							"linked collection. Available linked collections are %v",
						className, prop.ReferenceProperty, schemaProp.DataType)
				}
			}
			var refProperties []search.SelectProperty
			var addProps additional.Properties
			if prop.Properties != nil {
				refProperties, err = extractPropertiesRequestDeprecated(prop.Properties, scheme, linkedClassName)
				if err != nil {
					return nil, errors.Wrap(err, "extract properties request")
				}
			}
			if prop.Metadata != nil {
				addProps, err = extractAdditionalPropsFromMetadata(class, prop.Metadata)
				if err != nil {
					return nil, errors.Wrap(err, "extract additional props for refs")
				}
			}

			if prop.Properties == nil {
				refProperties, err = getAllNonRefNonBlobProperties(scheme, linkedClassName)
				if err != nil {
					return nil, errors.Wrap(err, "get all non ref non blob properties")
				}
			}
			if len(refProperties) == 0 && isIdOnlyRequest(prop.Metadata) {
				// This is a pure-ID query without any properties or additional metadata.
				// Indicate this to the DB, so it can optimize accordingly
				addProps.NoProps = true
			}

			props = append(props, search.SelectProperty{
				Name:        normalizedRefPropName,
				IsPrimitive: false,
				IsObject:    false,
				Refs: []search.SelectClass{{
					ClassName:            linkedClassName,
					RefProperties:        refProperties,
					AdditionalProperties: addProps,
				}},
			})
		}
	}

	if reqProps.ObjectProperties != nil && len(reqProps.ObjectProperties) > 0 {
		props = append(props, extractNestedProperties(reqProps.ObjectProperties)...)
	}

	return props, nil
}

func extractNestedProperties(props []*pb.ObjectPropertiesRequest) []search.SelectProperty {
	selectProps := make([]search.SelectProperty, 0)
	for _, prop := range props {
		nestedProps := make([]search.SelectProperty, 0)
		if prop.PrimitiveProperties != nil && len(prop.PrimitiveProperties) > 0 {
			for _, primitive := range prop.PrimitiveProperties {
				nestedProps = append(nestedProps, search.SelectProperty{
					Name:        schema.LowercaseFirstLetter(primitive),
					IsPrimitive: true,
					IsObject:    false,
				})
			}
		}
		if prop.ObjectProperties != nil && len(prop.ObjectProperties) > 0 {
			nestedProps = append(nestedProps, extractNestedProperties(prop.ObjectProperties)...)
		}
		selectProps = append(selectProps, search.SelectProperty{
			Name:        schema.LowercaseFirstLetter(prop.PropName),
			IsPrimitive: false,
			IsObject:    true,
			Props:       nestedProps,
		})
	}
	return selectProps
}

func extractAdditionalPropsFromMetadata(class *models.Class, prop *pb.MetadataRequest) (additional.Properties, error) {
	props := additional.Properties{
		Vector:             prop.Vector,
		ID:                 prop.Uuid,
		CreationTimeUnix:   prop.CreationTimeUnix,
		LastUpdateTimeUnix: prop.LastUpdateTimeUnix,
		Distance:           prop.Distance,
		Score:              prop.Score,
		ExplainScore:       prop.ExplainScore,
		IsConsistent:       prop.IsConsistent,
	}

	vectorIndex, err := schema.TypeAssertVectorIndex(class)
	if err != nil {
		return props, err
	}

	// certainty is only compatible with cosine distance
	if vectorIndex.DistanceName() == common.DistanceCosine && prop.Certainty {
		props.Certainty = true
	} else {
		props.Certainty = false
	}

	return props, nil
}

func isIdOnlyRequest(metadata *pb.MetadataRequest) bool {
	// could also use reflect here but this is more explicit
	return (metadata != nil &&
		metadata.Uuid &&
		!metadata.Vector &&
		!metadata.CreationTimeUnix &&
		!metadata.LastUpdateTimeUnix &&
		!metadata.Distance &&
		!metadata.Certainty &&
		!metadata.Score &&
		!metadata.ExplainScore &&
		!metadata.IsConsistent)
}

func getAllNonRefNonBlobProperties(scheme schema.Schema, className string) ([]search.SelectProperty, error) {
	var props []search.SelectProperty
	class := scheme.GetClass(schema.ClassName(className))

	for _, prop := range class.Properties {
		dt, err := schema.GetPropertyDataType(class, prop.Name)
		if err != nil {
			return []search.SelectProperty{}, errors.Wrap(err, "get property data type")
		}
		if *dt == schema.DataTypeCRef || *dt == schema.DataTypeBlob {
			continue
		}
		if *dt == schema.DataTypeObject || *dt == schema.DataTypeObjectArray {
			nested, err := schema.GetPropertyByName(class, prop.Name)
			if err != nil {
				return []search.SelectProperty{}, errors.Wrap(err, "get nested property by name")
			}
			nestedProps, err := getAllNonRefNonBlobNestedProperties(&Property{Property: nested})
			if err != nil {
				return []search.SelectProperty{}, errors.Wrap(err, "get all non ref non blob nested properties")
			}
			props = append(props, search.SelectProperty{
				Name:        prop.Name,
				IsPrimitive: false,
				IsObject:    true,
				Props:       nestedProps,
			})
		} else {
			props = append(props, search.SelectProperty{
				Name:        prop.Name,
				IsPrimitive: true,
			})
		}
	}
	return props, nil
}

func getAllNonRefNonBlobNestedProperties[P schema.PropertyInterface](property P) ([]search.SelectProperty, error) {
	var props []search.SelectProperty
	for _, prop := range property.GetNestedProperties() {
		dt, err := schema.GetNestedPropertyDataType(property, prop.Name)
		if err != nil {
			return []search.SelectProperty{}, errors.Wrap(err, "get nested property data type")
		}
		if *dt == schema.DataTypeCRef || *dt == schema.DataTypeBlob {
			continue
		}
		if *dt == schema.DataTypeObject || *dt == schema.DataTypeObjectArray {
			nested, err := schema.GetNestedPropertyByName(property, prop.Name)
			if err != nil {
				return []search.SelectProperty{}, errors.Wrap(err, "get nested property by name")
			}
			nestedProps, err := getAllNonRefNonBlobNestedProperties(&NestedProperty{NestedProperty: nested})
			if err != nil {
				return []search.SelectProperty{}, errors.Wrap(err, "get all non ref non blob nested properties")
			}
			props = append(props, search.SelectProperty{
				Name:        prop.Name,
				IsPrimitive: false,
				IsObject:    true,
				Props:       nestedProps,
			})
		} else {
			props = append(props, search.SelectProperty{
				Name:        prop.Name,
				IsPrimitive: true,
			})
		}
	}
	return props, nil
}

func parseNearImage(ni *pb.NearImageSearch) (*nearImage.NearImageParams, error) {
	nearImageOut := &nearImage.NearImageParams{
		Image: ni.Image,
	}

	// The following business logic should not sit in the API. However, it is
	// also part of the GraphQL API, so we need to duplicate it in order to get
	// the same behavior
	if ni.Distance != nil && ni.Certainty != nil {
		return nil, fmt.Errorf("near_image: cannot provide distance and certainty")
	}

	if ni.Certainty != nil {
		nearImageOut.Certainty = *ni.Certainty
	}

	if ni.Distance != nil {
		nearImageOut.Distance = *ni.Distance
		nearImageOut.WithDistance = true
	}

	return nearImageOut, nil
}

func parseNearAudio(na *pb.NearAudioSearch) (*nearAudio.NearAudioParams, error) {
	nearAudioOut := &nearAudio.NearAudioParams{
		Audio: na.Audio,
	}

	// The following business logic should not sit in the API. However, it is
	// also part of the GraphQL API, so we need to duplicate it in order to get
	// the same behavior
	if na.Distance != nil && na.Certainty != nil {
		return nil, fmt.Errorf("near_audio: cannot provide distance and certainty")
	}

	if na.Certainty != nil {
		nearAudioOut.Certainty = *na.Certainty
	}

	if na.Distance != nil {
		nearAudioOut.Distance = *na.Distance
		nearAudioOut.WithDistance = true
	}

	return nearAudioOut, nil
}

func parseNearVideo(nv *pb.NearVideoSearch) (*nearVideo.NearVideoParams, error) {
	nearVideoOut := &nearVideo.NearVideoParams{
		Video: nv.Video,
	}

	// The following business logic should not sit in the API. However, it is
	// also part of the GraphQL API, so we need to duplicate it in order to get
	// the same behavior
	if nv.Distance != nil && nv.Certainty != nil {
		return nil, fmt.Errorf("near_video: cannot provide distance and certainty")
	}

	if nv.Certainty != nil {
		nearVideoOut.Certainty = *nv.Certainty
	}

	if nv.Distance != nil {
		nearVideoOut.Distance = *nv.Distance
		nearVideoOut.WithDistance = true
	}

	return nearVideoOut, nil
}
