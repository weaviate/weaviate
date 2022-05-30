//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package common_filters

import "github.com/semi-technologies/weaviate/entities/searchparams"

// ExtractNearObject arguments, such as "vector" and "certainty"
func ExtractNearObject(source map[string]interface{}) searchparams.NearObject {
	var args searchparams.NearObject

	id, ok := source["id"]
	if ok {
		args.ID = id.(string)
	}

	beacon, ok := source["beacon"]
	if ok {
		args.Beacon = beacon.(string)
	}

	certainty, ok := source["certainty"]
	if ok {
		args.Certainty = certainty.(float64)
	}

	return args
}
