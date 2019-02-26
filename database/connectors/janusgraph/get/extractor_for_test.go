package get

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
)

func extractResult(input interface{}, path []interface{}) (interface{}, error) {

	if len(path) == 0 {
		return input, nil
	}

	currentPath := path[0]
	switch v := currentPath.(type) {
	case int:
		return handleSlice(input, v, path)
	case string:
		return handleMap(input, v, path)
	default:
		return nil, fmt.Errorf("invalid type for path segment: %T", currentPath)
	}
}

func handleSlice(input interface{}, i int, path []interface{}) (interface{}, error) {
	inputSlice, ok := input.([]interface{})
	if !ok {
		return nil, fmt.Errorf("input is not a slice: %T", input)
	}

	return extractResult(inputSlice[i], path[1:])
}

func handleMap(input interface{}, field string, path []interface{}) (interface{}, error) {
	switch v := input.(type) {
	case map[string]interface{}:
		return extractResult(v[field], path[1:])
	case get.LocalRef:
		switch field {
		case "Fields":
			return extractResult(v.Fields, path[1:])
		case "AtClass":
			return extractResult(v.AtClass, path[1:])
		default:
			return nil, fmt.Errorf("only 'Fields' or 'AtClass' supported on type get.LocalRef, got: %s", field)
		}
	default:
		return nil, fmt.Errorf("unsupported type: %T", input)
	}
}
