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

package refcache

import (
	"strings"

	"github.com/weaviate/weaviate/entities/search"
)

func getGroupSelectProperties(properties search.SelectProperties) search.SelectProperties {
	var selectGroupHitsProperties search.SelectProperties
	for _, prop := range properties {
		if strings.HasPrefix(prop.Name, "_additional:group:hits") {
			selectGroupHitsProperties = append(selectGroupHitsProperties, search.SelectProperty{
				Name:            strings.Replace(prop.Name, "_additional:group:hits:", "", 1),
				IsPrimitive:     prop.IsPrimitive,
				IncludeTypeName: prop.IncludeTypeName,
				Refs:            prop.Refs,
			})
		}
	}
	return selectGroupHitsProperties
}
