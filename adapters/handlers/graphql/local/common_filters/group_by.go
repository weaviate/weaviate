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

package common_filters

import "github.com/weaviate/weaviate/entities/searchparams"

// ExtractGroupBy
func ExtractGroupBy(source map[string]interface{}) searchparams.GroupBy {
	var args searchparams.GroupBy

	p, ok := source["path"]
	if ok {
		rawSlice := p.([]interface{})
		if len(rawSlice) == 1 {
			args.Property = rawSlice[0].(string)
		}
	}

	groups := source["groups"]
	if groups != nil {
		args.Groups = int(groups.(int))
	}

	objectsPerGroup := source["objectsPerGroup"]
	if objectsPerGroup != nil {
		args.ObjectsPerGroup = int(objectsPerGroup.(int))
	}

	return args
}
