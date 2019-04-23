/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package get

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/get"
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
		case "Class":
			return extractResult(v.Class, path[1:])
		default:
			return nil, fmt.Errorf("only 'Fields' or 'Class' supported on type get.LocalRef, got: %s", field)
		}
	default:
		return nil, fmt.Errorf("unsupported type: %T", input)
	}
}
