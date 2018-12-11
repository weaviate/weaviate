package network_get

import "fmt"

type FiltersPerInstance map[string]interface{}

// FiltersForNetworkInstances takes the global filters from a network filter
// parameter and splits it up into individual filter queries arranged by
// instance
func FiltersForNetworkInstances(args map[string]interface{}) (FiltersPerInstance, error) {
	resultSet := FiltersPerInstance{}

	where, ok := args["where"]
	if !ok {
		// no where clause means no filters and nothing to do
		return resultSet, nil
	}

	whereMap, ok := where.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected where to be a map, but was %#v", where)
	}

	operands, err := operandsFromWhere(whereMap)
	if err != nil {
		return resultSet, err
	}

	operand := operands[0]

	parsedPath, err := parsePathAndExtractInstance(operand)
	if err != nil {
		return resultSet, err
	}

	operand["path"] = parsedPath.path
	resultSet[parsedPath.instance] = map[string]interface{}{
		"where": map[string]interface{}{
			"operator": whereMap["operator"],
			"operands": []map[string]interface{}{operand},
		},
	}

	return resultSet, nil
}

func operandsFromWhere(where map[string]interface{}) ([]map[string]interface{}, error) {
	operands, ok := where["operands"]
	if !ok {
		return nil, fmt.Errorf("expected where to have field operands")
	}

	operandsMapSlice, ok := operands.([]map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected where.operands to be a []map, but was %#v", operands)
	}

	return operandsMapSlice, nil
}

type pathAndInstance struct {
	path     []string
	instance string
}

func parsePathAndExtractInstance(operand map[string]interface{}) (pathAndInstance, error) {
	result := pathAndInstance{}
	path, ok := operand["path"]
	if !ok {
		return result, fmt.Errorf("expected operand to have a field path")
	}

	pathFragments, ok := path.([]string)
	if !ok {
		return result, fmt.Errorf("expected where.operands[].path to be a []string, but was %#v", path)
	}

	if len(pathFragments) < 4 {
		return result, fmt.Errorf(
			"path must have at least four elements in the form of\n"+
				"[<instanceName>, <kind, e.g. thing/action>, <Class, e.g. City>, <property, e.g. population>]\n"+
				"got got only length %d on path\n%#v", len(pathFragments), pathFragments)
	}

	result.instance = pathFragments[0]
	result.path = pathFragments[1:]
	return result, nil
}
