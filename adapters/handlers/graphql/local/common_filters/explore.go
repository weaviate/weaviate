package common_filters

import "github.com/semi-technologies/weaviate/usecases/traverser"

// ExtractExplore arguments, such as "concepts", "moveTo", "moveAwayFrom",
// "limit", etc.
func ExtractExplore(source map[string]interface{}) traverser.ExploreParams {
	var args traverser.ExploreParams

	// keywords is a required argument, so we don't need to check for its existing
	keywords := source["concepts"].([]interface{})
	args.Values = make([]string, len(keywords), len(keywords))
	for i, value := range keywords {
		args.Values[i] = value.(string)
	}

	// limit is an optional arg, so it could be nil
	limit, ok := source["limit"]
	if ok {
		// the type is fixed through gql config, no need to catch incorrect type
		// assumption
		args.Limit = limit.(int)
	}

	// moveTo is an optional arg, so it could be nil
	moveTo, ok := source["moveTo"]
	if ok {
		args.MoveTo = extractMovement(moveTo)
	}

	// moveAwayFrom is an optional arg, so it could be nil
	moveAwayFrom, ok := source["moveAwayFrom"]
	if ok {
		args.MoveAwayFrom = extractMovement(moveAwayFrom)
	}

	return args
}

func extractMovement(input interface{}) traverser.ExploreMove {
	// the type is fixed through gql config, no need to catch incorrect type
	// assumption, all fields are required so we don't need to check for their
	// presence
	moveToMap := input.(map[string]interface{})
	res := traverser.ExploreMove{}
	res.Force = float32(moveToMap["force"].(float64))

	keywords := moveToMap["concepts"].([]interface{})
	res.Values = make([]string, len(keywords), len(keywords))
	for i, value := range keywords {
		res.Values[i] = value.(string)
	}

	return res
}
