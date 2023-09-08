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

	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"

	"github.com/weaviate/weaviate/entities/schema"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/search"
	pb "github.com/weaviate/weaviate/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

func searchResultsToProto(res []any, start time.Time, searchParams dto.GetParams, scheme schema.Schema) (*pb.SearchReply, error) {
	tookSeconds := float64(time.Since(start)) / float64(time.Second)
	out := &pb.SearchReply{
		Took:    float32(tookSeconds),
		Results: make([]*pb.SearchResult, len(res)),
	}

	for i, raw := range res {

		firstObject := i == 0
		asMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		props, err := extractPropertiesAnswer(scheme, asMap, searchParams.Properties, searchParams.ClassName, searchParams.AdditionalProperties)
		if err != nil {
			return nil, err
		}

		additionalProps, generativeGroupResults, err := extractAdditionalProps(asMap, searchParams.AdditionalProperties, firstObject)
		if err != nil {
			return nil, err
		}

		if firstObject {
			out.GenerativeGroupedResult = generativeGroupResults
		}

		result := &pb.SearchResult{
			Properties:           props,
			AdditionalProperties: additionalProps,
		}

		out.Results[i] = result
	}

	return out, nil
}

func extractAdditionalProps(asMap map[string]any, additionalPropsParams additional.Properties, firstObject bool) (*pb.ResultAdditionalProps, string, error) {
	err := errors.New("could not extract additional prop")
	_, generativeSearchEnabled := additionalPropsParams.ModuleParams["generate"]

	additionalProps := &pb.ResultAdditionalProps{}
	if additionalPropsParams.ID && !generativeSearchEnabled {
		idRaw, ok := asMap["id"]
		if !ok {
			return nil, "", errors.Wrap(err, "get id")
		}

		idStrfmt, ok := idRaw.(strfmt.UUID)
		if !ok {
			return nil, "", errors.Wrap(err, "format id")
		}
		additionalProps.Id = idStrfmt.String()
	}
	_, ok := asMap["_additional"]
	if !ok {
		return additionalProps, "", nil
	}

	additionalPropertiesMap := asMap["_additional"].(map[string]interface{})
	generativeGroupResults := ""
	// id is part of the _additional map in case of generative search - don't aks me why
	if generativeSearchEnabled {
		idRaw, ok := additionalPropertiesMap["id"]
		if !ok {
			return nil, "", errors.Wrap(err, "get id generative")
		}

		idStrfmt, ok := idRaw.(strfmt.UUID)
		if !ok {
			return nil, "", errors.Wrap(err, "format id generative")
		}
		additionalProps.Id = idStrfmt.String()

		generate, ok := additionalPropertiesMap["generate"]
		if ok { // does not always have content, for example with grouped results only the first object has an entry
			generateFmt, ok := generate.(*models.GenerateResult)
			if !ok {
				return nil, "", errors.Wrap(err, "cast generative result")
			}
			if generateFmt.SingleResult != nil && *generateFmt.SingleResult != "" {
				additionalProps.Generative = *generateFmt.SingleResult
				additionalProps.GenerativePresent = true
			}

			// grouped results are only added to the first object for GQL reasons
			if firstObject && generateFmt.GroupedResult != nil && *generateFmt.GroupedResult != "" {
				generativeGroupResults = *generateFmt.GroupedResult
			}
		}
	}

	// additional properties are only present for certain searches/configs => don't return an error if not available
	if additionalPropsParams.Vector {
		vector, ok := additionalPropertiesMap["vector"]
		if ok {
			vectorfmt, ok2 := vector.([]float32)
			if ok2 {
				additionalProps.Vector = vectorfmt
			}
		}
	}

	if additionalPropsParams.Certainty {
		additionalProps.CertaintyPresent = false
		certainty, ok := additionalPropertiesMap["certainty"]
		if ok {
			certaintyfmt, ok2 := certainty.(float64)
			if ok2 {
				additionalProps.Certainty = float32(certaintyfmt)
				additionalProps.CertaintyPresent = true
			}
		}
	}

	if additionalPropsParams.Distance {
		additionalProps.DistancePresent = false
		distance, ok := additionalPropertiesMap["distance"]
		if ok {
			distancefmt, ok2 := distance.(float32)
			if ok2 {
				additionalProps.Distance = distancefmt
				additionalProps.DistancePresent = true
			}
		}
	}

	if additionalPropsParams.CreationTimeUnix {
		additionalProps.CreationTimeUnixPresent = false
		creationtime, ok := additionalPropertiesMap["creationTimeUnix"]
		if ok {
			creationtimefmt, ok2 := creationtime.(int64)
			if ok2 {
				additionalProps.CreationTimeUnix = creationtimefmt
				additionalProps.CreationTimeUnixPresent = true
			}
		}
	}

	if additionalPropsParams.LastUpdateTimeUnix {
		additionalProps.LastUpdateTimeUnixPresent = false
		lastUpdateTime, ok := additionalPropertiesMap["lastUpdateTimeUnix"]
		if ok {
			lastUpdateTimefmt, ok2 := lastUpdateTime.(int64)
			if ok2 {
				additionalProps.LastUpdateTimeUnix = lastUpdateTimefmt
				additionalProps.LastUpdateTimeUnixPresent = true
			}
		}
	}

	if additionalPropsParams.ExplainScore {
		additionalProps.ExplainScorePresent = false
		explainScore, ok := additionalPropertiesMap["explainScore"]
		if ok {
			explainScorefmt, ok2 := explainScore.(string)
			if ok2 {
				additionalProps.ExplainScore = explainScorefmt
				additionalProps.ExplainScorePresent = true
			}
		}
	}

	if additionalPropsParams.Score {
		additionalProps.ScorePresent = false
		score, ok := additionalPropertiesMap["score"]
		if ok {
			scorefmt, ok2 := score.(float32)
			if ok2 {
				additionalProps.Score = scorefmt
				additionalProps.ScorePresent = true
			}
		}
	}

	if additionalPropsParams.IsConsistent {
		isConsistent, ok := additionalPropertiesMap["isConsistent"]
		if ok {
			isConsistentfmt, ok2 := isConsistent.(bool)
			if ok2 {
				additionalProps.IsConsistent = &isConsistentfmt
			}
		}
	}

	return additionalProps, generativeGroupResults, nil
}

func extractPropertiesAnswer(scheme schema.Schema, results map[string]interface{}, properties search.SelectProperties, className string, additionalPropsParams additional.Properties) (*pb.ResultProperties, error) {
	props := pb.ResultProperties{}
	nonRefProps := make(map[string]interface{}, 0)
	refProps := make([]*pb.ReturnRefProperties, 0)
	for _, prop := range properties {
		propRaw, ok := results[prop.Name]
		if !ok {
			continue
		}
		if prop.IsPrimitive {
			nonRefProps[prop.Name] = propRaw
			continue
		}
		refs, ok := propRaw.([]interface{})
		if !ok {
			continue
		}
		extractedRefProps := make([]*pb.ResultProperties, 0, len(refs))
		for _, ref := range refs {
			refLocal, ok := ref.(search.LocalRef)
			if !ok {
				continue
			}
			extractedRefProp, err := extractPropertiesAnswer(scheme, refLocal.Fields, prop.Refs[0].RefProperties, refLocal.Class, additionalPropsParams)
			if err != nil {
				continue
			}
			additionalProps, _, err := extractAdditionalProps(refLocal.Fields, prop.Refs[0].AdditionalProperties, false)
			if err != nil {
				return nil, err
			}
			extractedRefProp.Metadata = additionalProps
			extractedRefProps = append(extractedRefProps, extractedRefProp)
		}

		refProp := pb.ReturnRefProperties{PropName: prop.Name, Properties: extractedRefProps}
		refProps = append(refProps, &refProp)
	}

	if len(nonRefProps) > 0 {
		if err := extractArrayTypes(scheme, className, &props, nonRefProps); err != nil {
			return nil, err
		}
		newStruct, err := structpb.NewStruct(nonRefProps)
		if err != nil {
			return nil, errors.Wrap(err, "creating ref-prop struct")
		}
		props.NonRefProperties = newStruct
	}
	if len(refProps) > 0 {
		props.RefProps = refProps
	}

	props.ClassName = className

	return &props, nil
}

// slices cannot be part of a grpc struct, so we need to handle each of them separately
func extractArrayTypes(scheme schema.Schema, className string, props *pb.ResultProperties, nonRefProps map[string]interface{}) error {
	class := scheme.GetClass(schema.ClassName(className))
	for propName, prop := range nonRefProps {
		dataType, err := schema.GetPropertyDataType(class, propName)
		if err != nil {
			return err
		}

		switch *dataType {
		case schema.DataTypeIntArray:
			propIntAsFloat, ok := prop.([]float64)
			if !ok {
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
			delete(nonRefProps, propName)
		case schema.DataTypeNumberArray:
			propIntAsFloat, ok := prop.([]float64)
			if !ok {
				return fmt.Errorf("property %v with datatype %v needs to be []float64, got %T", propName, dataType, prop)
			}
			if props.NumberArrayProperties == nil {
				props.NumberArrayProperties = make([]*pb.NumberArrayProperties, 0)
			}
			props.NumberArrayProperties = append(props.NumberArrayProperties, &pb.NumberArrayProperties{PropName: propName, Values: propIntAsFloat})
			delete(nonRefProps, propName)
		case schema.DataTypeStringArray, schema.DataTypeTextArray, schema.DataTypeDateArray, schema.DataTypeUUIDArray:
			propString, ok := prop.([]string)
			if !ok {
				return fmt.Errorf("property %v with datatype %v needs to be []string, got %T", propName, dataType, prop)
			}
			if props.TextArrayProperties == nil {
				props.TextArrayProperties = make([]*pb.TextArrayProperties, 0)
			}
			props.TextArrayProperties = append(props.TextArrayProperties, &pb.TextArrayProperties{PropName: propName, Values: propString})
			delete(nonRefProps, propName)
		case schema.DataTypeBooleanArray:
			propBool, ok := prop.([]bool)
			if !ok {
				return fmt.Errorf("property %v with datatype %v needs to be []bool, got %T", propName, dataType, prop)
			}
			if props.BooleanArrayProperties == nil {
				props.BooleanArrayProperties = make([]*pb.BooleanArrayProperties, 0)
			}
			props.BooleanArrayProperties = append(props.BooleanArrayProperties, &pb.BooleanArrayProperties{PropName: propName, Values: propBool})
			delete(nonRefProps, propName)
		default:
			_, isArray := schema.IsArrayType(*dataType)
			if isArray {
				return fmt.Errorf("property %v with array type not handled %v", propName, dataType)
			}
			continue
		}

	}
	return nil
}
