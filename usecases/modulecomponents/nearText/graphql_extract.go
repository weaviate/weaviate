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

package nearText

// ExtractNearText arguments, such as "concepts", "moveTo", "moveAwayFrom",
// "limit", etc.
func (g *GraphQLArgumentsProvider) extractNearTextFn(source map[string]interface{}) interface{} {
	var args NearTextParams

	// keywords is a required argument, so we don't need to check for its existing
	keywords := source["concepts"].([]interface{})
	args.Values = make([]string, len(keywords))
	for i, value := range keywords {
		args.Values[i] = value.(string)
	}

	// autocorrect is an optional arg, so it could be nil
	autocorrect, ok := source["autocorrect"]
	if ok {
		args.Autocorrect = autocorrect.(bool)
	}

	// if there's text transformer present and autocorrect set to true
	// perform text transformation operation
	if args.Autocorrect && g.nearTextTransformer != nil {
		if transformedValues, err := g.nearTextTransformer.Transform(args.Values); err == nil {
			args.Values = transformedValues
		}
	}

	// limit is an optional arg, so it could be nil
	limit, ok := source["limit"]
	if ok {
		// the type is fixed through gql config, no need to catch incorrect type
		// assumption
		args.Limit = limit.(int)
	}

	certainty, ok := source["certainty"]
	if ok {
		args.Certainty = certainty.(float64)
	}

	distance, ok := source["distance"]
	if ok {
		args.Distance = distance.(float64)
		args.WithDistance = true
	}

	// moveTo is an optional arg, so it could be nil
	moveTo, ok := source["moveTo"]
	if ok {
		args.MoveTo = extractMovement(moveTo)
	}

	// network is an optional arg, so it could be nil
	network, ok := source["network"]
	if ok {
		args.Network = network.(bool)
	}

	// moveAwayFrom is an optional arg, so it could be nil
	moveAwayFrom, ok := source["moveAwayFrom"]
	if ok {
		args.MoveAwayFrom = extractMovement(moveAwayFrom)
	}

	return &args
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
