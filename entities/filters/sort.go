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

package filters

// Sort contains path and order (asc, desc) information
type Sort struct {
	Path  []string `json:"path"`
	Order string   `json:"order"`
}

// ExtractSortFromArgs gets the sort parameters
func ExtractSortFromArgs(in []interface{}) []Sort {
	var args []Sort

	for i := range in {
		sortFilter, ok := in[i].(map[string]interface{})
		if ok {
			var path []string
			pathParam, ok := sortFilter["path"].([]interface{})
			if ok {
				path = make([]string, len(pathParam))
				for i, value := range pathParam {
					path[i] = value.(string)
				}
			}
			var order string
			orderParam, ok := sortFilter["order"]
			if ok {
				order = orderParam.(string)
			}
			args = append(args, Sort{path, order})
		}
	}

	return args
}
