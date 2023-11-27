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

package v1

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/search"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func searchResultsToProto(res []interface{}, start time.Time, searchParams dto.GetParams, scheme schema.Schema) (*pb.SearchReply, error) {
	tookSeconds := float64(time.Since(start)) / float64(time.Second)
	out := &pb.SearchReply{
		Took: float32(tookSeconds),
	}

	if searchParams.GroupBy != nil {
		out.GroupByResults = make([]*pb.GroupByResult, len(res))
		for i, raw := range res {
			group, generativeGroupResponse, err := extractGroup(raw, searchParams, scheme)
			if err != nil {
				return nil, err
			}
			out.GenerativeGroupedResult = &generativeGroupResponse
			out.GroupByResults[i] = group
		}
	} else {
		objects, generativeGroupResponse, err := extractObjectsToResults(res, searchParams, scheme, false)
		if err != nil {
			return nil, err
		}
		out.GenerativeGroupedResult = &generativeGroupResponse
		out.Results = objects
	}
	return out, nil
}

func extractObjectsToResults(res []interface{}, searchParams dto.GetParams, scheme schema.Schema, fromGroup bool) ([]*pb.SearchResult, string, error) {
	results := make([]*pb.SearchResult, len(res))
	generativeGroupResultsReturn := ""
	for i, raw := range res {
		asMap, ok := raw.(map[string]interface{})
		if !ok {
			return nil, "", fmt.Errorf("could not parse returns %v", raw)
		}
		firstObject := i == 0

		props, err := extractPropertiesAnswer(scheme, asMap, searchParams.Properties, searchParams.ClassName, searchParams.AdditionalProperties)
		if err != nil {
			return nil, "", err
		}

		additionalProps, generativeGroupResults, err := extractAdditionalProps(asMap, searchParams.AdditionalProperties, firstObject, fromGroup)
		if err != nil {
			return nil, "", err
		}

		if firstObject && generativeGroupResults != "" {
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
	err := errors.New("could not extract additional prop")
	_, generativeSearchEnabled := additionalPropsParams.ModuleParams["generate"]

	additionalProps := &pb.MetadataResult{}
	if additionalPropsParams.ID && !generativeSearchEnabled && !fromGroup {
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
	// id is part of the _additional map in case of generative search - don't aks me why
	if additionalPropsParams.ID && (generativeSearchEnabled || fromGroup) {
		idRaw, ok := additionalPropertiesMap["id"]
		if !ok {
			return nil, "", errors.Wrap(err, "get id generative")
		}

		idStrfmt, ok := idRaw.(strfmt.UUID)
		if !ok {
			return nil, "", errors.Wrap(err, "format id generative")
		}
		additionalProps.Id = idStrfmt.String()
	}

	if generativeSearchEnabled {
		generate, ok := additionalPropertiesMap["generate"]
		if !ok && firstObject {
			return nil, "", errors.Wrap(err,
				"No results for generative search despite a search request. Is a the generative module enabled?",
			)
		}

		if ok { // does not always have content, for example with grouped results only the first object has an entry
			generateFmt, ok := generate.(*models.GenerateResult)
			if !ok {
				return nil, "", errors.Wrap(err, "cast generative result")
			}
			if generateFmt.Error != nil {
				return nil, "", generateFmt.Error
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
				additionalProps.IsConsistentPresent = true
			}
		}
	}

	return additionalProps, generativeGroupResults, nil
}

func extractGroup(raw any, searchParams dto.GetParams, scheme schema.Schema) (*pb.GroupByResult, string, error) {
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

	objects, groupedGenerativeResults, err := extractObjectsToResults(returnObjectsUntyped, searchParams, scheme, true)
	if err != nil {
		return nil, "", errors.Wrap(err, "extracting hits from group")
	}

	ret := &pb.GroupByResult{Name: group.GroupedBy.Value, MaxDistance: group.MaxDistance, MinDistance: group.MinDistance, NumberOfObjects: int64(group.Count), Objects: objects}

	return ret, groupedGenerativeResults, nil
}

func extractPropertiesAnswer(scheme schema.Schema, results map[string]interface{}, properties search.SelectProperties, className string, additionalPropsParams additional.Properties) (*pb.PropertiesResult, error) {
	nonRefProps := &structpb.Struct{
		Fields: make(map[string]*structpb.Value, 0),
	}
	refProps := make([]*pb.RefPropertiesResult, 0)
	for _, prop := range properties {
		propRaw, ok := results[prop.Name]
		if !ok {
			continue
		}
		if prop.IsPrimitive || prop.IsObject {
			b, err := json.Marshal(propRaw)
			if err != nil {
				return nil, errors.Wrap(err, "marshalling non-ref value")
			}
			value := &structpb.Value{}
			err = protojson.Unmarshal(b, value)
			if err != nil {
				return nil, errors.Wrap(err, "creating non-ref value")
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
		props.NonRefProperties = nonRefProps
	}
	if len(refProps) != 0 {
		props.RefProps = refProps
	}
	props.TargetCollection = className
	return &props, nil
}
