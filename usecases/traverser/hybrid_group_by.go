//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package traverser

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func (e *Explorer) groupSearchResults(ctx context.Context, sr search.Results, groupBy *searchparams.GroupBy) (search.Results, error) {
	groupsOrdered := []string{}
	groups := map[string][]search.Result{}

	for _, result := range sr {
		prop_i := result.Object().Properties
		prop := prop_i.(map[string]interface{})
		rawValue := prop[groupBy.Property]

		values, err := extractGroupByValues(rawValue)
		if err != nil {
			return nil, fmt.Errorf("failed to extract groupBy values for property %s: %w", groupBy.Property, err)
		}

		if len(values) == 0 {
			continue
		}

		// Process all values for this result, but stop if we hit the groups limit
		skipResult := false
		for _, val := range values {
			if skipResult {
				break
			}

			current, groupExists := groups[val]
			if len(current) >= groupBy.ObjectsPerGroup {
				continue
			}

			if !groupExists && len(groups) >= groupBy.Groups {
				skipResult = true
				break
			}

			groups[val] = append(current, result)

			if !groupExists {
				// this group doesn't exist add it to the ordered list
				groupsOrdered = append(groupsOrdered, val)
			}
		}
	}

	out := make(search.Results, 0, len(sr))
	for i, groupValue := range groupsOrdered {
		groupMembers := groups[groupValue]

		// Use the first result but create a new AdditionalProperties to avoid sharing
		first := groupMembers[0]
		first.AdditionalProperties = models.AdditionalProperties{}

		hits := make([]map[string]interface{}, len(groupMembers))

		for j, groupMember := range groupMembers {
			props := map[string]interface{}{}
			for k, v := range groupMember.Object().Properties.(map[string]interface{}) {
				props[k] = v
			}
			props["_additional"] = &additional.GroupHitAdditional{
				ID:       groupMember.ID,
				Distance: groupMember.Dist,
				Vector:   groupMember.Vector,
				Vectors:  groupMember.Vectors,
			}
			hits[j] = props
		}

		group := &additional.Group{
			ID: i,
			GroupedBy: &additional.GroupedBy{
				Value: groupValue,
				Path:  []string{groupBy.Property},
			},
			Count:       len(hits),
			Hits:        hits,
			MinDistance: first.Dist,
			MaxDistance: first.Dist,
		}

		// add group
		first.AdditionalProperties["group"] = group

		out = append(out, first)
	}

	return out, nil
}

// extractGroupByValues extracts string values from various property types for grouping.
// It handles:
// - string: returns as single-element slice
// - []string: returns all elements
// - []interface{}: converts each element to string if possible
// - other types: returns empty slice (skips grouping)
func extractGroupByValues(rawValue interface{}) ([]string, error) {
	if rawValue == nil {
		return []string{}, nil
	}

	switch v := rawValue.(type) {
	case string:
		return []string{v}, nil
	case []string:
		return v, nil
	case []interface{}:
		result := make([]string, 0, len(v))
		for i, item := range v {
			if str, ok := item.(string); ok {
				result = append(result, str)
			} else {
				return nil, fmt.Errorf("array element at index %d is not a string: %T", i, item)
			}
		}
		return result, nil
	default:
		// Skip non-string/non-array properties for grouping
		return []string{}, nil
	}
}
