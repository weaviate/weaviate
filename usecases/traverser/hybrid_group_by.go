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

package traverser

import (
	"context"

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
		val, ok := prop[groupBy.Property].(string)

		if !ok {
			continue
		}

		current, groupExists := groups[val]
		if len(current) >= groupBy.ObjectsPerGroup {
			continue
		}

		if !groupExists && len(groups) >= groupBy.Groups {
			continue
		}

		groups[val] = append(current, result)

		if !groupExists {
			// this group doesn't exist add it to the ordered list
			groupsOrdered = append(groupsOrdered, val)
		}
	}

	out := make(search.Results, 0, len(sr))
	for i, groupValue := range groupsOrdered {
		groupMembers := groups[groupValue]
		first := groupMembers[0]

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
		if first.AdditionalProperties == nil {
			first.AdditionalProperties = models.AdditionalProperties{}
		}
		first.AdditionalProperties["group"] = group

		out = append(out, first)
	}

	return out, nil
}
