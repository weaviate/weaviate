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

// Merges nestPropsNew with nestPropsOld
// Returns new slice without changing input ones and
// bool indicating whether merged slice is different than the old one
func MergeRecursivelyNestedProperties(nestPropsOld, nestPropsNew []*models.NestedProperty,
) ([]*models.NestedProperty, bool) {
	merged := false
	nestPropsMerged := make([]*models.NestedProperty, len(nestPropsOld), len(nestPropsOld)+len(nestPropsNew))
	copy(nestPropsMerged, nestPropsOld)

	existingIndexMap := map[string]int{}
	for i := range nestPropsMerged {
		existingIndexMap[nestPropsMerged[i].Name] = i
	}

	for _, nestPropNew := range nestPropsNew {
		i, exists := existingIndexMap[nestPropNew.Name]
		if !exists {
			existingIndexMap[nestPropNew.Name] = len(nestPropsMerged)
			nestPropsMerged = append(nestPropsMerged, nestPropNew)
			merged = true
		} else if _, isNested := AsNested(nestPropsMerged[i].DataType); isNested {
			if recurNestProps, recurMerged := MergeRecursivelyNestedProperties(
				nestPropsMerged[i].NestedProperties,
				nestPropNew.NestedProperties,
			); recurMerged {
				nestPropCopy := *nestPropsMerged[i]
				nestPropCopy.NestedProperties = recurNestProps

				nestPropsMerged[i] = &nestPropCopy
				merged = true
			}
		}
	}

	return nestPropsMerged, merged
}

// Determines diff between nestPropsNew and nestPropsOld slices
func DiffRecursivelyNestedProperties(nestPropsOld, nestPropsNew []*models.NestedProperty,
) []*models.NestedProperty {
	nestPropsDiff := make([]*models.NestedProperty, 0, len(nestPropsNew))

	existingIndexMap := map[string]int{}
	for i := range nestPropsOld {
		existingIndexMap[nestPropsOld[i].Name] = i
	}

	for _, nestPropNew := range nestPropsNew {
		i, exists := existingIndexMap[nestPropNew.Name]
		if !exists {
			existingIndexMap[nestPropNew.Name] = len(nestPropsDiff)
			nestPropsDiff = append(nestPropsDiff, nestPropNew)
		} else if _, isNested := AsNested(nestPropsOld[i].DataType); isNested {
			if recurNestProps := DiffRecursivelyNestedProperties(
				nestPropsOld[i].NestedProperties,
				nestPropNew.NestedProperties,
			); len(recurNestProps) > 0 {
				nestPropCopy := *nestPropsOld[i]
				nestPropCopy.NestedProperties = recurNestProps

				existingIndexMap[nestPropCopy.Name] = len(nestPropsDiff)
				nestPropsDiff = append(nestPropsDiff, &nestPropCopy)
			}
		}
	}

	return nestPropsDiff
}
