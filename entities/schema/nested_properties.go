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

package schema

import "github.com/weaviate/weaviate/entities/models"

// merges nPropsExt with nPropsBase
// returns new slice without changing input ones
// and bool indicating whether there was changes done comparing to base slice
func MergeRecursivelyNestedProperties(nPropsBase, nPropsExt []*models.NestedProperty,
) ([]*models.NestedProperty, bool) {
	merged := false
	nProps := make([]*models.NestedProperty, len(nPropsBase), len(nPropsBase)+len(nPropsExt))
	copy(nProps, nPropsBase)

	existingIndexMap := map[string]int{}
	for index := range nProps {
		existingIndexMap[nProps[index].Name] = index
	}

	for _, nProp := range nPropsExt {
		index, exists := existingIndexMap[nProp.Name]
		if !exists {
			existingIndexMap[nProp.Name] = len(nProps)
			nProps = append(nProps, nProp)
			merged = true
		} else if _, isNested := AsNested(nProps[index].DataType); isNested {
			if mergedProps, mergedNested := MergeRecursivelyNestedProperties(nProps[index].NestedProperties,
				nProp.NestedProperties,
			); mergedNested {
				nPropCopy := *nProps[index]
				nProps[index] = &nPropCopy
				nProps[index].NestedProperties = mergedProps
				merged = true
			}
		}
	}

	return nProps, merged
}
