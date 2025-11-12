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

package nearText

import (
	"fmt"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/graphqlutil"
	"github.com/weaviate/weaviate/entities/dto"
)

// ExtractNearText arguments, such as "concepts", "moveTo", "moveAwayFrom",
// "limit", etc.
func (g *GraphQLArgumentsProvider) extractNearTextFn(source map[string]interface{}) (interface{}, *dto.TargetCombination, error) {
	args := NearTextParams{
		Values: extractConceptStrings(source),
	}

	args.Autocorrect = optionalBool(source, "autocorrect")
	applyAutocorrect(&args, g)

	if err := assignOptionalLimit(&args, source["limit"]); err != nil {
		return nil, nil, err
	}

	if certainty, ok := optionalFloat(source, "certainty"); ok {
		args.Certainty = certainty
	}

	if distance, ok := optionalFloat(source, "distance"); ok {
		args.Distance = distance
		args.WithDistance = true
	}

	assignOptionalMovement(&args.MoveTo, source, "moveTo")
	assignOptionalMovement(&args.MoveAwayFrom, source, "moveAwayFrom")
	args.Network = optionalBool(source, "network")

	targetVectors, combination, err := common_filters.ExtractTargets(source)
	if err != nil {
		return nil, nil, err
	}
	args.TargetVectors = targetVectors

	return &args, combination, nil
}

func extractConceptStrings(source map[string]interface{}) []string {
	keywords := source["concepts"].([]interface{})
	values := make([]string, len(keywords))
	for i, value := range keywords {
		values[i] = value.(string)
	}
	return values
}

func applyAutocorrect(args *NearTextParams, provider *GraphQLArgumentsProvider) {
	if !args.Autocorrect || provider.nearTextTransformer == nil {
		return
	}
	if transformedValues, err := provider.nearTextTransformer.Transform(args.Values); err == nil {
		args.Values = transformedValues
	}
}

func assignOptionalLimit(args *NearTextParams, raw interface{}) error {
	if raw == nil {
		return nil
	}
	limit, err := graphqlutil.ToInt(raw)
	if err != nil {
		return fmt.Errorf("invalid limit: %w", err)
	}
	args.Limit = limit
	return nil
}

func optionalFloat(source map[string]interface{}, key string) (float64, bool) {
	raw, ok := source[key]
	if !ok {
		return 0, false
	}
	value, ok := raw.(float64)
	return value, ok
}

func optionalBool(source map[string]interface{}, key string) bool {
	raw, ok := source[key]
	if !ok {
		return false
	}
	value, _ := raw.(bool)
	return value
}

func assignOptionalMovement(target *ExploreMove, source map[string]interface{}, key string) {
	raw, ok := source[key]
	if !ok {
		return
	}
	*target = extractMovement(raw)
}

func extractMovement(input interface{}) ExploreMove {
	// the type is fixed through gql config, no need to catch incorrect type
	// assumption, all fields are required so we don't need to check for their
	// presence
	moveToMap := input.(map[string]interface{})
	res := ExploreMove{}
	res.Force = float32(moveToMap["force"].(float64))

	keywords, ok := moveToMap["concepts"].([]interface{})
	if ok {
		res.Values = make([]string, len(keywords))
		for i, value := range keywords {
			res.Values[i] = value.(string)
		}
	}

	objects, ok := moveToMap["objects"].([]interface{})
	if ok {
		res.Objects = make([]ObjectMove, len(objects))
		for i, value := range objects {
			v, ok := value.(map[string]interface{})
			if ok {
				if v["id"] != nil {
					res.Objects[i].ID = v["id"].(string)
				}
				if v["beacon"] != nil {
					res.Objects[i].Beacon = v["beacon"].(string)
				}
			}
		}
	}

	return res
}
