package network_get

import "fmt"

// FiltersPerInstance holds individual "where" filters per instance name
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

	for _, operand := range operands {
		parsedPath, err := parsePathAndExtractInstance(operand)
		if err != nil {
			return resultSet, err
		}

		operand["path"] = parsedPath.path
		instance, ok := resultSet[parsedPath.instance]
		if ok {
			resultSet[parsedPath.instance] = mergeInstanceWithNewOperand(instance, operand)
		} else {
			resultSet[parsedPath.instance] = map[string]interface{}{
				"where": map[string]interface{}{
					"operator": whereMap["operator"],
					"operands": []map[string]interface{}{operand},
				},
			}
		}
	}

	return resultSet, nil
}

func operandsFromWhere(where map[string]interface{}) ([]map[string]interface{}, error) {
	operands, ok := where["operands"]
	if !ok {
		return nil, fmt.Errorf("expected where to have field operands")
	}

	operandsSlice, ok := operands.([]interface{})
	if !ok {
		return nil, fmt.Errorf("expected where.operands to be a slice, but was %#v", operands)
	}

	operandsMapSlice := make([]map[string]interface{}, len(operandsSlice), len(operandsSlice))
	for i, operand := range operandsSlice {
		operandMap, ok := operand.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected where.operands[] to be a map, but was %#v", operands)
		}

		operandsMapSlice[i] = operandMap
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

	pathFragmentsUntyped, ok := path.([]interface{})
	if !ok {
		return result, fmt.Errorf("expected where.operands[].path to be a slice, but was %#v", path)
	}

	pathFragments := make([]string, len(pathFragmentsUntyped), len(pathFragmentsUntyped))
	for i, fragment := range pathFragmentsUntyped {
		fragmentString, ok := fragment.(string)
		if !ok {
			return result, fmt.Errorf("expected where.operands[].path[] to be a string, but was %#v", path)
		}

		pathFragments[i] = fragmentString
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

func mergeInstanceWithNewOperand(instance interface{}, newOperand map[string]interface{}) map[string]interface{} {
	// all type assertions are considered safe, because
	// we only use constructs we've created ourselves
	where := instance.(map[string]interface{})["where"]
	operands := where.(map[string]interface{})["operands"].([]map[string]interface{})
	newOperands := append(operands, newOperand)
	instance.(map[string]interface{})["where"].(map[string]interface{})["operands"] = newOperands
	return instance.(map[string]interface{})
}
