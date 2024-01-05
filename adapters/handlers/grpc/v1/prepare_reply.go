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
	"math/big"
	"strings"
	"time"

	"github.com/weaviate/weaviate/usecases/byteops"

	"github.com/weaviate/weaviate/entities/schema"
	generative "github.com/weaviate/weaviate/usecases/modulecomponents/additional/generate"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/search"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func searchResultsToProto(res []interface{}, start time.Time, searchParams dto.GetParams, scheme schema.Schema, usesPropertiesMessage bool) (*pb.SearchReply, error) {
	tookSeconds := float64(time.Since(start)) / float64(time.Second)
	out := &pb.SearchReply{
		Took:                    float32(tookSeconds),
		GenerativeGroupedResult: new(string), // pointer to empty string
	}

	if searchParams.GroupBy != nil {
		out.GroupByResults = make([]*pb.GroupByResult, len(res))
		for i, raw := range res {
			group, generativeGroupResponse, err := extractGroup(raw, searchParams, scheme, usesPropertiesMessage)
			if err != nil {
				return nil, err
			}
			if generativeGroupResponse != "" {
				out.GenerativeGroupedResult = &generativeGroupResponse
			}
			out.GroupByResults[i] = group
		}
	} else {
		objects, generativeGroupResponse, err := extractObjectsToResults(res, searchParams, scheme, false, usesPropertiesMessage)
		if err != nil {
			return nil, err
		}
		out.GenerativeGroupedResult = &generativeGroupResponse
		out.Results = objects
	}
	return out, nil
}

func extractObjectsToResults(res []interface{}, searchParams dto.GetParams, scheme schema.Schema, fromGroup, usesPropertiesMessage bool) ([]*pb.SearchResult, string, error) {
	results := make([]*pb.SearchResult, len(res))
	generativeGroupResultsReturn := ""
	for i, raw := range res {
		asMap, ok := raw.(map[string]interface{})
		if !ok {
			return nil, "", fmt.Errorf("could not parse returns %v", raw)
		}
		firstObject := i == 0

		var props *pb.PropertiesResult
		var err error

		if usesPropertiesMessage {
			props, err = extractPropertiesAnswer(scheme, asMap, searchParams.Properties, searchParams.ClassName, searchParams.AdditionalProperties)
		} else {
			props, err = extractPropertiesAnswerDeprecated(scheme, asMap, searchParams.Properties, searchParams.ClassName, searchParams.AdditionalProperties)
		}
		if err != nil {
			return nil, "", err
		}

		additionalProps, generativeGroupResults, err := extractAdditionalProps(asMap, searchParams.AdditionalProperties, firstObject, fromGroup)
		if err != nil {
			return nil, "", err
		}

		if generativeGroupResultsReturn == "" && generativeGroupResults != "" {
			generativeGroupResultsReturn = generativeGroupResults
		}

		result := &pb.SearchResult{
			Properties: props,
			Metadata:   additionalProps,
		}

		results[i] = result
	}
	return results, generativeGroupResultsReturn, nil
}

func extractAdditionalProps(asMap map[string]any, additionalPropsParams additional.Properties, firstObject, fromGroup bool) (*pb.MetadataResult, string, error) {
	generativeSearchRaw, generativeSearchEnabled := additionalPropsParams.ModuleParams["generate"]
	_, rerankEnabled := additionalPropsParams.ModuleParams["rerank"]

	metadata := &pb.MetadataResult{}
	if additionalPropsParams.ID && !generativeSearchEnabled && !rerankEnabled && !fromGroup {
		idRaw, ok := asMap["id"]
		if !ok {
			return nil, "", errors.New("could not extract get id in additional prop")
		}

		idStrfmt, ok := idRaw.(strfmt.UUID)
		if !ok {
			return nil, "", errors.New("could not extract format id in additional prop")
		}
		metadata.Id = idStrfmt.String()
		hexInteger, success := new(big.Int).SetString(strings.Replace(metadata.Id, "-", "", -1), 16)
		if !success {
			return nil, "", fmt.Errorf("failed to parse hex string to integer")
		}
		metadata.IdAsBytes = hexInteger.Bytes()
	}
	_, ok := asMap["_additional"]
	if !ok {
		return metadata, "", nil
	}

	var additionalPropertiesMap map[string]interface{}
	if !fromGroup {
		additionalPropertiesMap = asMap["_additional"].(map[string]interface{})
	} else {
		addPropertiesGroup := asMap["_additional"].(*additional.GroupHitAdditional)
		additionalPropertiesMap = make(map[string]interface{}, 3)
		additionalPropertiesMap["id"] = addPropertiesGroup.ID
		additionalPropertiesMap["vector"] = addPropertiesGroup.Vector
		additionalPropertiesMap["distance"] = addPropertiesGroup.Distance
	}
	generativeGroupResults := ""
	// id is part of the _additional map in case of generative search, group, & rerank - don't aks me why
	if additionalPropsParams.ID && (generativeSearchEnabled || fromGroup || rerankEnabled) {
		idRaw, ok := additionalPropertiesMap["id"]
		if !ok {
			return nil, "", errors.New("could not extract get id generative in additional prop")
		}

		idStrfmt, ok := idRaw.(strfmt.UUID)
		if !ok {
			return nil, "", errors.New("could not format id generative in additional prop")
		}
		metadata.Id = idStrfmt.String()
	}

	if generativeSearchEnabled {
		var generateFmt *models.GenerateResult

		generate, ok := additionalPropertiesMap["generate"]
		if !ok {
			generateFmt = &models.GenerateResult{}
		} else {
			generateFmt, ok = generate.(*models.GenerateResult)
			if !ok {
				return nil, "", errors.New("could not cast generative result additional prop")
			}
		}

		generativeSearch, ok := generativeSearchRaw.(*generative.Params)
		if !ok {
			return nil, "", errors.New("could not cast generative search params")
		}
		if generativeSearch.Prompt != nil && generateFmt.SingleResult == nil {
			return nil, "", errors.New("No results for generative search despite a search request. Is a generative module enabled?")
		}

		if generateFmt.Error != nil {
			return nil, "", generateFmt.Error
		}

		if generateFmt.SingleResult != nil && *generateFmt.SingleResult != "" {
			metadata.Generative = *generateFmt.SingleResult
			metadata.GenerativePresent = true
		}

		// grouped results are only added to the first object for GQL reasons
		// however, reranking can result in a different order, so we need to check every object
		// recording the result if it's present assuming that it is at least somewhere and will be caught
		if generateFmt.GroupedResult != nil && *generateFmt.GroupedResult != "" {
			generativeGroupResults = *generateFmt.GroupedResult
		}
	}

	if rerankEnabled {
		rerank, ok := additionalPropertiesMap["rerank"]
		if !ok {
			return nil, "", errors.New("No results for rerank despite a search request. Is a the rerank module enabled?")
		}
		rerankFmt, ok := rerank.([]*models.RankResult)
		if !ok {
			return nil, "", errors.New("could not cast rerank result additional prop")
		}
		metadata.RerankScore = *rerankFmt[0].Score
		metadata.RerankScorePresent = true
	}

	// additional properties are only present for certain searches/configs => don't return an error if not available
	if additionalPropsParams.Vector {
		vector, ok := additionalPropertiesMap["vector"]
		if ok {
			vectorfmt, ok2 := vector.([]float32)
			if ok2 {
				metadata.Vector = vectorfmt // deprecated, remove in a bit
				metadata.VectorBytes = byteops.Float32ToByteVector(vectorfmt)
			}
		}
	}

	if additionalPropsParams.Certainty {
		metadata.CertaintyPresent = false
		certainty, ok := additionalPropertiesMap["certainty"]
		if ok {
			certaintyfmt, ok2 := certainty.(float64)
			if ok2 {
				metadata.Certainty = float32(certaintyfmt)
				metadata.CertaintyPresent = true
			}
		}
	}

	if additionalPropsParams.Distance {
		metadata.DistancePresent = false
		distance, ok := additionalPropertiesMap["distance"]
		if ok {
			distancefmt, ok2 := distance.(float32)
			if ok2 {
				metadata.Distance = distancefmt
				metadata.DistancePresent = true
			}
		}
	}

	if additionalPropsParams.CreationTimeUnix {
		metadata.CreationTimeUnixPresent = false
		creationtime, ok := additionalPropertiesMap["creationTimeUnix"]
		if ok {
			creationtimefmt, ok2 := creationtime.(int64)
			if ok2 {
				metadata.CreationTimeUnix = creationtimefmt
				metadata.CreationTimeUnixPresent = true
			}
		}
	}

	if additionalPropsParams.LastUpdateTimeUnix {
		metadata.LastUpdateTimeUnixPresent = false
		lastUpdateTime, ok := additionalPropertiesMap["lastUpdateTimeUnix"]
		if ok {
			lastUpdateTimefmt, ok2 := lastUpdateTime.(int64)
			if ok2 {
				metadata.LastUpdateTimeUnix = lastUpdateTimefmt
				metadata.LastUpdateTimeUnixPresent = true
			}
		}
	}

	if additionalPropsParams.ExplainScore {
		metadata.ExplainScorePresent = false
		explainScore, ok := additionalPropertiesMap["explainScore"]
		if ok {
			explainScorefmt, ok2 := explainScore.(string)
			if ok2 {
				metadata.ExplainScore = explainScorefmt
				metadata.ExplainScorePresent = true
			}
		}
	}

	if additionalPropsParams.Score {
		metadata.ScorePresent = false
		score, ok := additionalPropertiesMap["score"]
		if ok {
			scorefmt, ok2 := score.(float32)
			if ok2 {
				metadata.Score = scorefmt
				metadata.ScorePresent = true
			}
		}
	}

	if additionalPropsParams.IsConsistent {
		isConsistent, ok := additionalPropertiesMap["isConsistent"]
		if ok {
			isConsistentfmt, ok2 := isConsistent.(bool)
			if ok2 {
				metadata.IsConsistent = &isConsistentfmt
				metadata.IsConsistentPresent = true
			}
		}
	}

	return metadata, generativeGroupResults, nil
}

func extractGroup(raw any, searchParams dto.GetParams, scheme schema.Schema, usesMarshalling bool) (*pb.GroupByResult, string, error) {
	generativeSearchRaw, generativeSearchEnabled := searchParams.AdditionalProperties.ModuleParams["generate"]
	_, rerankEnabled := searchParams.AdditionalProperties.ModuleParams["rerank"]
	asMap, ok := raw.(map[string]interface{})
	if !ok {
		return nil, "", fmt.Errorf("cannot parse result %v", raw)
	}
	add, ok := asMap["_additional"]
	if !ok {
		return nil, "", fmt.Errorf("_additional is required for groups %v", asMap)
	}
	addAsMap, ok := add.(map[string]interface{})
	if !ok {
		return nil, "", fmt.Errorf("cannot parse _additional %v", add)
	}
	groupRaw, ok := addAsMap["group"]
	if !ok {
		return nil, "", fmt.Errorf("group is not present %v", addAsMap)
	}
	group, ok := groupRaw.(*additional.Group)
	if !ok {
		return nil, "", fmt.Errorf("cannot parse _additional %v", groupRaw)
	}

	ret := &pb.GroupByResult{
		Name:            group.GroupedBy.Value,
		MaxDistance:     group.MaxDistance,
		MinDistance:     group.MinDistance,
		NumberOfObjects: int64(group.Count),
	}

	groupedGenerativeResults := ""
	if generativeSearchEnabled {
		var generateFmt *models.GenerateResult

		generate, ok := addAsMap["generate"]
		if !ok {
			generateFmt = &models.GenerateResult{}
		} else {
			generateFmt, ok = generate.(*models.GenerateResult)
			if !ok {
				return nil, "", errors.New("could not cast generative result additional prop")
			}
		}

		generativeSearch, ok := generativeSearchRaw.(*generative.Params)
		if !ok {
			return nil, "", errors.New("could not cast generative search params")
		}
		if generativeSearch.Prompt != nil && generateFmt.SingleResult == nil {
			return nil, "", errors.New("No results for generative search despite a search request. Is a generative module enabled?")
		}

		if generateFmt.Error != nil {
			return nil, "", generateFmt.Error
		}

		if generateFmt.SingleResult != nil && *generateFmt.SingleResult != "" {
			ret.Generative = &pb.GenerativeReply{Result: *generateFmt.SingleResult}
		}

		// grouped results are only added to the first object for GQL reasons
		// however, reranking can result in a different order, so we need to check every object
		// recording the result if it's present assuming that it is at least somewhere and will be caught
		if generateFmt.GroupedResult != nil && *generateFmt.GroupedResult != "" {
			groupedGenerativeResults = *generateFmt.GroupedResult
		}
	}

	if rerankEnabled {
		rerankRaw, ok := addAsMap["rerank"]
		if !ok {
			return nil, "", fmt.Errorf("rerank is not present %v", addAsMap)
		}
		rerank, ok := rerankRaw.([]*models.RankResult)
		if !ok {
			return nil, "", fmt.Errorf("cannot parse rerank %v", rerankRaw)
		}
		ret.Rerank = &pb.RerankReply{
			Score: *rerank[0].Score,
		}
	}

	// group results does not support more additional properties
	searchParams.AdditionalProperties = additional.Properties{
		ID:       searchParams.AdditionalProperties.ID,
		Vector:   searchParams.AdditionalProperties.Vector,
		Distance: searchParams.AdditionalProperties.Distance,
	}

	// group objects are returned as a different type than normal results ([]map[string]interface{} vs []interface). As
	// the normal path is used much more often than groupBy, convert the []map[string]interface{} to []interface{}, even
	// though we cast it to map[string]interface{} in the extraction function.
	// This way we only do a copy for groupBy and not for the standard code-path which is used more often
	returnObjectsUntyped := make([]interface{}, len(group.Hits))
	for i := range returnObjectsUntyped {
		returnObjectsUntyped[i] = group.Hits[i]
	}

	objects, _, err := extractObjectsToResults(returnObjectsUntyped, searchParams, scheme, true, usesMarshalling)
	if err != nil {
		return nil, "", errors.Wrap(err, "extracting hits from group")
	}

	ret.Objects = objects

	return ret, groupedGenerativeResults, nil
}

func extractPropertiesAnswerDeprecated(scheme schema.Schema, results map[string]interface{}, properties search.SelectProperties, className string, additionalPropsParams additional.Properties) (*pb.PropertiesResult, error) {
	nonRefProps := make(map[string]interface{}, 0)
	refProps := make([]*pb.RefPropertiesResult, 0)
	objProps := make([]*pb.ObjectProperties, 0)
	objArrayProps := make([]*pb.ObjectArrayProperties, 0)
	for _, prop := range properties {
		propRaw, ok := results[prop.Name]
		if !ok {
			continue
		}
		if prop.IsPrimitive {
			nonRefProps[prop.Name] = propRaw
			continue
		}
		if prop.IsObject {
			nested, err := scheme.GetProperty(schema.ClassName(className), schema.PropertyName(prop.Name))
			if err != nil {
				return nil, errors.Wrap(err, "getting property")
			}
			singleObj, ok := propRaw.(map[string]interface{})
			if ok {
				extractedNestedProp, err := extractPropertiesNested(scheme, singleObj, prop, className, &Property{Property: nested})
				if err != nil {
					return nil, errors.Wrap(err, "extracting nested properties")
				}
				objProps = append(objProps, &pb.ObjectProperties{
					PropName: prop.Name,
					Value:    extractedNestedProp,
				})
				continue
			}
			arrayObjs, ok := propRaw.([]interface{})
			if ok {
				extractedNestedProps := make([]*pb.ObjectPropertiesValue, 0, len(arrayObjs))
				for _, obj := range arrayObjs {
					singleObj, ok := obj.(map[string]interface{})
					if !ok {
						continue
					}
					extractedNestedProp, err := extractPropertiesNested(scheme, singleObj, prop, className, &Property{Property: nested})
					if err != nil {
						return nil, err
					}
					extractedNestedProps = append(extractedNestedProps, extractedNestedProp)
				}
				objArrayProps = append(objArrayProps,
					&pb.ObjectArrayProperties{
						PropName: prop.Name,
						Values:   extractedNestedProps,
					},
				)
				continue
			}
		}
		refs, ok := propRaw.([]interface{})
		if !ok {
			continue
		}
		extractedRefProps := make([]*pb.PropertiesResult, 0, len(refs))
		for _, ref := range refs {
			refLocal, ok := ref.(search.LocalRef)
			if !ok {
				continue
			}
			extractedRefProp, err := extractPropertiesAnswerDeprecated(scheme, refLocal.Fields, prop.Refs[0].RefProperties, refLocal.Class, additionalPropsParams)
			if err != nil {
				continue
			}
			additionalProps, _, err := extractAdditionalProps(refLocal.Fields, prop.Refs[0].AdditionalProperties, false, false)
			if err != nil {
				return nil, err
			}
			extractedRefProp.Metadata = additionalProps
			extractedRefProps = append(extractedRefProps, extractedRefProp)
		}

		refProp := pb.RefPropertiesResult{PropName: prop.Name, Properties: extractedRefProps}
		refProps = append(refProps, &refProp)
	}
	props := pb.PropertiesResult{}
	if len(nonRefProps) > 0 {
		outProps := pb.ObjectPropertiesValue{}
		if err := extractArrayTypesRoot(scheme, className, nonRefProps, &outProps); err != nil {
			return nil, errors.Wrap(err, "extracting non-primitive types")
		}
		newStruct, err := structpb.NewStruct(nonRefProps)
		if err != nil {
			return nil, errors.Wrap(err, "creating non-ref-prop struct")
		}
		props.NonRefProperties = newStruct
		props.IntArrayProperties = outProps.IntArrayProperties
		props.NumberArrayProperties = outProps.NumberArrayProperties
		props.TextArrayProperties = outProps.TextArrayProperties
		props.BooleanArrayProperties = outProps.BooleanArrayProperties
		props.ObjectProperties = outProps.ObjectProperties
		props.ObjectArrayProperties = outProps.ObjectArrayProperties
	}
	if len(refProps) > 0 {
		props.RefProps = refProps
	}
	if len(objProps) > 0 {
		props.ObjectProperties = objProps
	}
	if len(objArrayProps) > 0 {
		props.ObjectArrayProperties = objArrayProps
	}

	props.TargetCollection = className
	return &props, nil
}

func extractPropertiesAnswer(scheme schema.Schema, results map[string]interface{}, properties search.SelectProperties, className string, additionalPropsParams additional.Properties) (*pb.PropertiesResult, error) {
	nonRefProps := &pb.Properties{
		Fields: make(map[string]*pb.Value, 0),
	}
	refProps := make([]*pb.RefPropertiesResult, 0)
	class := scheme.GetClass(schema.ClassName(className))
	for _, prop := range properties {
		propRaw, ok := results[prop.Name]
		if !ok {
			continue
		}
		if prop.IsPrimitive {
			dataType, err := schema.GetPropertyDataType(class, prop.Name)
			if err != nil {
				return nil, errors.Wrap(err, "getting primitive property datatype")
			}
			value, err := NewPrimitiveValue(propRaw, *dataType)
			if err != nil {
				return nil, errors.Wrapf(err, "creating primitive value for %v", prop.Name)
			}
			nonRefProps.Fields[prop.Name] = value
			continue
		}
		if prop.IsObject {
			nested, err := scheme.GetProperty(schema.ClassName(className), schema.PropertyName(prop.Name))
			if err != nil {
				return nil, errors.Wrap(err, "getting nested property")
			}
			value, err := NewNestedValue(propRaw, schema.DataType(nested.DataType[0]), &Property{Property: nested}, prop)
			if err != nil {
				return nil, errors.Wrap(err, "creating object value")
			}
			nonRefProps.Fields[prop.Name] = value
			continue
		}
		refs, ok := propRaw.([]interface{})
		if !ok {
			continue
		}
		extractedRefProps := make([]*pb.PropertiesResult, 0, len(refs))
		for _, ref := range refs {
			refLocal, ok := ref.(search.LocalRef)
			if !ok {
				continue
			}
			extractedRefProp, err := extractPropertiesAnswer(scheme, refLocal.Fields, prop.Refs[0].RefProperties, refLocal.Class, additionalPropsParams)
			if err != nil {
				continue
			}
			additionalProps, _, err := extractAdditionalProps(refLocal.Fields, prop.Refs[0].AdditionalProperties, false, false)
			if err != nil {
				return nil, err
			}
			extractedRefProp.Metadata = additionalProps
			extractedRefProps = append(extractedRefProps, extractedRefProp)
		}

		refProp := pb.RefPropertiesResult{PropName: prop.Name, Properties: extractedRefProps}
		refProps = append(refProps, &refProp)
	}
	props := pb.PropertiesResult{}
	if len(nonRefProps.Fields) != 0 {
		props.NonRefProps = nonRefProps
	}
	if len(refProps) != 0 {
		props.RefProps = refProps
	}
	props.TargetCollection = className
	return &props, nil
}

func extractPropertiesNested[P schema.PropertyInterface](scheme schema.Schema, results map[string]interface{}, property search.SelectProperty, className string, parent P) (*pb.ObjectPropertiesValue, error) {
	primitiveProps := make(map[string]interface{}, 0)
	objProps := make([]*pb.ObjectProperties, 0)
	objArrayProps := make([]*pb.ObjectArrayProperties, 0)
	for _, prop := range property.Props {
		propRaw, ok := results[prop.Name]
		if !ok {
			continue
		}
		if prop.IsPrimitive {
			primitiveProps[prop.Name] = propRaw
			continue
		}
		if prop.IsObject {
			var err error
			objProps, objArrayProps, err = extractObjectProperties(scheme, propRaw, prop, className, parent, objProps, objArrayProps)
			if err != nil {
				return nil, err
			}
		}
	}
	props := pb.ObjectPropertiesValue{}
	if len(primitiveProps) > 0 {
		if err := extractArrayTypesNested(scheme, className, primitiveProps, &props, parent); err != nil {
			return nil, errors.Wrap(err, "extracting non-primitive types")
		}
		newStruct, err := structpb.NewStruct(primitiveProps)
		if err != nil {
			return nil, errors.Wrap(err, "creating non-ref-prop struct")
		}
		props.NonRefProperties = newStruct
	}
	if len(objProps) > 0 {
		props.ObjectProperties = objProps
	}
	if len(objArrayProps) > 0 {
		props.ObjectArrayProperties = objArrayProps
	}
	return &props, nil
}

func extractObjectProperties[P schema.PropertyInterface](scheme schema.Schema, propRaw interface{}, property search.SelectProperty, className string, parent P, objProps []*pb.ObjectProperties, objArrayProps []*pb.ObjectArrayProperties) ([]*pb.ObjectProperties, []*pb.ObjectArrayProperties, error) {
	prop, ok := propRaw.(map[string]interface{})
	if ok {
		objProp, err := extractObjectSingleProperties(scheme, prop, property, className, parent)
		if err != nil {
			return objProps, objArrayProps, err
		}
		objProps = append(objProps, objProp)
	}
	propArray, ok := propRaw.([]interface{})
	if ok {
		objArrayProp, err := extractObjectArrayProperties(scheme, propArray, property, className, parent)
		if err != nil {
			return objProps, objArrayProps, err
		}
		objArrayProps = append(objArrayProps, objArrayProp)
	}
	return objProps, objArrayProps, nil
}

func extractObjectSingleProperties[P schema.PropertyInterface](scheme schema.Schema, prop map[string]interface{}, property search.SelectProperty, className string, parent P) (*pb.ObjectProperties, error) {
	nested, err := schema.GetNestedPropertyByName(parent, property.Name)
	if err != nil {
		return nil, errors.Wrap(err, "getting property")
	}
	extractedNestedProp, err := extractPropertiesNested(scheme, prop, property, className, &NestedProperty{NestedProperty: nested})
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("extracting nested properties from %v", nested))
	}
	return &pb.ObjectProperties{
		PropName: property.Name,
		Value:    extractedNestedProp,
	}, nil
}

func extractObjectArrayProperties[P schema.PropertyInterface](scheme schema.Schema, propObjs []interface{}, property search.SelectProperty, className string, parent P) (*pb.ObjectArrayProperties, error) {
	extractedNestedProps := make([]*pb.ObjectPropertiesValue, 0, len(propObjs))
	for _, objRaw := range propObjs {
		nested, err := schema.GetNestedPropertyByName(parent, property.Name)
		if err != nil {
			return nil, errors.Wrap(err, "getting property")
		}
		obj, ok := objRaw.(map[string]interface{})
		if !ok {
			continue
		}
		extractedNestedProp, err := extractPropertiesNested(scheme, obj, property, className, &NestedProperty{NestedProperty: nested})
		if err != nil {
			return nil, errors.Wrap(err, "extracting nested properties")
		}
		extractedNestedProps = append(extractedNestedProps, extractedNestedProp)
	}
	return &pb.ObjectArrayProperties{
		PropName: property.Name,
		Values:   extractedNestedProps,
	}, nil
}

func extractArrayTypesRoot(scheme schema.Schema, className string, rawProps map[string]interface{}, props *pb.ObjectPropertiesValue) error {
	dataTypes := make(map[string]*schema.DataType, 0)
	for propName := range rawProps {
		dataType, err := schema.GetPropertyDataType(scheme.GetClass(schema.ClassName(className)), propName)
		if err != nil {
			return err
		}
		dataTypes[propName] = dataType
	}
	return extractArrayTypes(scheme, rawProps, props, dataTypes)
}

func extractArrayTypesNested[P schema.PropertyInterface](scheme schema.Schema, className string, rawProps map[string]interface{}, props *pb.ObjectPropertiesValue, parent P) error {
	dataTypes := make(map[string]*schema.DataType, 0)
	for propName := range rawProps {
		dataType, err := schema.GetNestedPropertyDataType(parent, propName)
		if err != nil {
			return err
		}
		dataTypes[propName] = dataType
	}
	return extractArrayTypes(scheme, rawProps, props, dataTypes)
}

// slices cannot be part of a grpc struct, so we need to handle each of them separately
func extractArrayTypes(scheme schema.Schema, rawProps map[string]interface{}, props *pb.ObjectPropertiesValue, dataTypes map[string]*schema.DataType) error {
	for propName, prop := range rawProps {
		dataType := dataTypes[propName]
		switch *dataType {
		case schema.DataTypeIntArray:
			propIntAsFloat, ok := prop.([]float64)
			if !ok {
				emptyArr, ok := prop.([]interface{})
				if ok && len(emptyArr) == 0 {
					continue
				}
				return fmt.Errorf("property %v with datatype %v needs to be []float64, got %T", propName, dataType, prop)
			}
			propInt := make([]int64, len(propIntAsFloat))
			for i := range propIntAsFloat {
				propInt[i] = int64(propIntAsFloat[i])
			}
			if props.IntArrayProperties == nil {
				props.IntArrayProperties = make([]*pb.IntArrayProperties, 0)
			}
			props.IntArrayProperties = append(props.IntArrayProperties, &pb.IntArrayProperties{PropName: propName, Values: propInt})
			delete(rawProps, propName)
		case schema.DataTypeNumberArray:
			propFloat, ok := prop.([]float64)
			if !ok {
				emptyArr, ok := prop.([]interface{})
				if ok && len(emptyArr) == 0 {
					continue
				}
				return fmt.Errorf("property %v with datatype %v needs to be []float64, got %T", propName, dataType, prop)
			}

			if props.NumberArrayProperties == nil {
				props.NumberArrayProperties = make([]*pb.NumberArrayProperties, 0)
			}
			props.NumberArrayProperties = append(
				props.NumberArrayProperties,
				&pb.NumberArrayProperties{PropName: propName, ValuesBytes: byteops.Float64ToByteVector(propFloat), Values: propFloat},
			)
			delete(rawProps, propName)
		case schema.DataTypeStringArray, schema.DataTypeTextArray, schema.DataTypeDateArray, schema.DataTypeUUIDArray:
			propString, ok := prop.([]string)
			if !ok {
				emptyArr, ok := prop.([]interface{})
				if ok && len(emptyArr) == 0 {
					continue
				}
				return fmt.Errorf("property %v with datatype %v needs to be []string, got %T", propName, dataType, prop)
			}
			if props.TextArrayProperties == nil {
				props.TextArrayProperties = make([]*pb.TextArrayProperties, 0)
			}
			props.TextArrayProperties = append(props.TextArrayProperties, &pb.TextArrayProperties{PropName: propName, Values: propString})
			delete(rawProps, propName)
		case schema.DataTypeBooleanArray:
			propBool, ok := prop.([]bool)
			if !ok {
				emptyArr, ok := prop.([]interface{})
				if ok && len(emptyArr) == 0 {
					continue
				}
				return fmt.Errorf("property %v with datatype %v needs to be []bool, got %T", propName, dataType, prop)
			}
			if props.BooleanArrayProperties == nil {
				props.BooleanArrayProperties = make([]*pb.BooleanArrayProperties, 0)
			}
			props.BooleanArrayProperties = append(props.BooleanArrayProperties, &pb.BooleanArrayProperties{PropName: propName, Values: propBool})
			delete(rawProps, propName)
		default:
			_, isArray := schema.IsArrayType(*dataType)
			if isArray {
				return fmt.Errorf("property %v with array type not handled %v", propName, dataType)
			}
		}
	}
	return nil
}
