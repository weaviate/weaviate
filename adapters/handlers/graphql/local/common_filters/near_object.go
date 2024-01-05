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

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/searchparams"
)

// ExtractNearObject arguments, such as "vector" and "certainty"
func ExtractNearObject(source map[string]interface{}) (searchparams.NearObject, error) {
	var args searchparams.NearObject

	id, ok := source["id"]
	if ok {
		args.ID = id.(string)
	}

	beacon, ok := source["beacon"]
	if ok {
		args.Beacon = beacon.(string)
	}

	certainty, certaintyOK := source["certainty"]
	if certaintyOK {
		args.Certainty = certainty.(float64)
	}

	distance, distanceOK := source["distance"]
	if distanceOK {
		args.Distance = distance.(float64)
		args.WithDistance = true
	}

	if certaintyOK && distanceOK {
		return searchparams.NearObject{},
			fmt.Errorf("cannot provide distance and certainty")
	}

	return args, nil
}
