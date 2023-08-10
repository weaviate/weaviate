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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	pb "github.com/weaviate/weaviate/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

func searchResultsToProto(res []any, start time.Time, searchParams dto.GetParams) (*pb.SearchReply, error) {
	tookSeconds := float64(time.Since(start)) / float64(time.Second)
	out := &pb.SearchReply{
		Took:    float32(tookSeconds),
		Results: make([]*pb.SearchResult, len(res)),
	}

	for i, raw := range res {
		asMap, ok := raw.(map[string]any)
		if !ok {
			continue
		}

		props, err := extractPropertiesAnswer(asMap, searchParams.Properties, searchParams.ClassName, searchParams.AdditionalProperties)
		if err != nil {
			continue
		}

		additionalProps, err := extractAdditionalProps(asMap, searchParams.AdditionalProperties)
		if err != nil {
			continue
		}

		result := &pb.SearchResult{
			Properties:           props,
			AdditionalProperties: additionalProps,
		}

		out.Results[i] = result
	}

	return out, nil
}

func extractAdditionalProps(asMap map[string]any, additionalPropsParams additional.Properties) (*pb.ResultAdditionalProps, error) {
	err := errors.New("could not extract additional prop")
	additionalProps := &pb.ResultAdditionalProps{}
	if additionalPropsParams.ID {
		idRaw, ok := asMap["id"]
		if !ok {
			return nil, err
		}

		idStrfmt, ok := idRaw.(strfmt.UUID)
		if !ok {
			return nil, err
		}
		additionalProps.Id = idStrfmt.String()
	}
	_, ok := asMap["_additional"]
	if !ok {
		return additionalProps, nil
	}

	additionalPropertiesMap := asMap["_additional"].(map[string]interface{})

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

	return additionalProps, nil
}

func extractPropertiesAnswer(results map[string]interface{}, properties search.SelectProperties, class string, additionalPropsParams additional.Properties) (*pb.ResultProperties, error) {
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
			extractedRefProp, err := extractPropertiesAnswer(refLocal.Fields, prop.Refs[0].RefProperties, refLocal.Class, additionalPropsParams)
			if err != nil {
				continue
			}
			additionalProps, err := extractAdditionalProps(refLocal.Fields, prop.Refs[0].AdditionalProperties)
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
		newStruct, err := structpb.NewStruct(nonRefProps)
		if err != nil {
			return nil, errors.Wrap(err, "creating ref-prop struct")
		}
		props.NonRefProperties = newStruct
	}
	if len(refProps) > 0 {
		props.RefProps = refProps
	}

	props.ClassName = class

	return &props, nil
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

func extractPropertiesRequest(reqProps *pb.Properties, scheme schema.Schema, className string) []search.SelectProperty {
	var props []search.SelectProperty
	if reqProps == nil {
		return props
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
				return nil
			}

			// use datatype of the reference property to get the name of the linked class
			linkedClass := schemaProp.DataType[0]

			props = append(props, search.SelectProperty{
				Name:        prop.ReferenceProperty,
				IsPrimitive: false,
				Refs: []search.SelectClass{{
					ClassName:            linkedClass,
					RefProperties:        extractPropertiesRequest(prop.LinkedProperties, scheme, linkedClass),
					AdditionalProperties: extractAdditionalPropsForRefs(prop.Metadata),
				}},
			})
		}
	}

	return props
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
