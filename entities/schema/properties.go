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

// DedupProperties removes from newProps slice properties already present in oldProps slice.
// If property of nested type (object/object[]) is present in both slices,
// diff is calculated to contain only those nested properties that are missing from
// old property model
func DedupProperties(oldProps, newProps []*models.Property) []*models.Property {
	uniqueProps := make([]*models.Property, 0, len(newProps))

	oldPropsByName := make(map[string]int, len(oldProps))
	for idx := range oldProps {
		oldPropsByName[oldProps[idx].Name] = idx
	}
	uniquePropsByName := make(map[string]int, len(newProps))
	markedToDiff := make([]string, 0, len(newProps))

	for _, newProp := range newProps {
		propName := LowercaseFirstLetter(newProp.Name)

		uniqueIdx, uniqueExists := uniquePropsByName[propName]
		if !uniqueExists {
			oldIdx, oldExists := oldPropsByName[propName]
			if !oldExists {
				uniquePropsByName[propName] = len(uniqueProps)
				uniqueProps = append(uniqueProps, newProp)
			} else {
				oldProp := oldProps[oldIdx]
				if _, isNested := AsNested(oldProp.DataType); isNested {
					mergedNestedProps, merged := MergeRecursivelyNestedProperties(
						oldProp.NestedProperties, newProp.NestedProperties)
					if merged {
						oldPropCopy := *oldProp
						oldPropCopy.NestedProperties = mergedNestedProps
						uniquePropsByName[propName] = len(uniqueProps)
						uniqueProps = append(uniqueProps, &oldPropCopy)

						markedToDiff = append(markedToDiff, propName)
					}
				}
			}
		} else {
			uniqueProp := uniqueProps[uniqueIdx]
			if _, isNested := AsNested(uniqueProp.DataType); isNested {
				mergedNestedProps, merged := MergeRecursivelyNestedProperties(
					uniqueProp.NestedProperties, newProp.NestedProperties)
				if merged {
					uniquePropCopy := *uniqueProp
					uniquePropCopy.NestedProperties = mergedNestedProps
					uniqueProps[uniqueIdx] = &uniquePropCopy
				}
			}
		}
	}

	for _, propName := range markedToDiff {
		uniqueIdx := uniquePropsByName[propName]
		oldIdx := oldPropsByName[propName]

		diffNestedProps := DiffRecursivelyNestedProperties(
			oldProps[oldIdx].NestedProperties, uniqueProps[uniqueIdx].NestedProperties)
		uniqueProps[uniqueIdx].NestedProperties = diffNestedProps
	}

	return uniqueProps
}
