//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package journey

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/documents"
)

// GroupBySingleAndMultiShardTests creates the Document and Passage classes
// returned by classes (single- or multi-shard) and runs the groupBy journey.
func GroupBySingleAndMultiShardTests(t *testing.T, weaviateEndpoint string,
	classes func(multishard bool) []*models.Class,
) {
	if weaviateEndpoint != "" {
		helper.SetupClient(weaviateEndpoint)
	}
	// helper methods
	getGroup := func(value interface{}) map[string]interface{} {
		group := value.(map[string]interface{})["_additional"].(map[string]interface{})["group"].(map[string]interface{})
		return group
	}
	getGroupHits := func(t *testing.T, group map[string]interface{}) (string, []string, []float64) {
		ids := []string{}
		distances := []float64{}
		hits := group["hits"].([]interface{})
		for _, hit := range hits {
			additional := hit.(map[string]interface{})["_additional"].(map[string]interface{})
			ids = append(ids, additional["id"].(string))
			distance, err := additional["distance"].(json.Number).Float64()
			require.NoError(t, err)
			distances = append(distances, distance)
		}
		groupedBy := group["groupedBy"].(map[string]interface{})
		groupedByValue := groupedBy["value"].(string)
		return groupedByValue, ids, distances
	}
	getContents := func(t *testing.T, group map[string]interface{}) []string {
		result := []string{}
		hits := group["hits"].([]interface{})
		for _, hit := range hits {
			content, ok := hit.(map[string]interface{})["content"].(string)
			require.True(t, ok, "hits{content} cannot be empty")
			result = append(result, content)
		}
		return result
	}
	// test methods
	create := func(t *testing.T, multishard bool) {
		for _, class := range classes(multishard) {
			helper.CreateClass(t, class)
		}
		for _, obj := range documents.Objects() {
			helper.CreateObject(t, obj)
			helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
		}

		// wait for the objects to be indexed
		// TODO: remove this sleep when we have a better way to determine when the objects are indexed
		time.Sleep(3 * time.Second)
	}
	groupBy := func(t *testing.T, groupsCount, objectsPerGroup int) {
		query := `
		{
			Get{
				Passage(
					nearObject:{
						id: "00000000-0000-0000-0000-000000000001"
					}
					groupBy:{
						path:["ofDocument"]
						groups:%v
						objectsPerGroup:%v
					}
				){
					_additional{
						id
						group{
							groupedBy{value}
							count
							maxDistance
							minDistance
							hits {
								content
								_additional{
									id
									distance
								}
							}
						}
					}
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, fmt.Sprintf(query, groupsCount, objectsPerGroup))
		groups := result.Get("Get", "Passage").AsSlice()

		require.Len(t, groups, groupsCount)

		// the hits of a group are ordered by distance, which depends on the
		// vectorizer, so only the members of a group are pinned
		groupedBy1 := `weaviate://localhost/Document/00000000-0000-0000-0000-000000000011`
		groupedBy2 := `weaviate://localhost/Document/00000000-0000-0000-0000-000000000012`
		expectedResults := map[string][]string{
			groupedBy1: {
				documents.PassageIDs[0].String(),
				documents.PassageIDs[1].String(),
				documents.PassageIDs[2].String(),
				documents.PassageIDs[3].String(),
				documents.PassageIDs[4].String(),
				documents.PassageIDs[5].String(),
			},
			groupedBy2: {
				documents.PassageIDs[6].String(),
				documents.PassageIDs[7].String(),
			},
		}
		groupsOrder := []string{groupedBy1, groupedBy2}

		for i, current := range groups {
			group := getGroup(current)
			groupedBy, ids, distances := getGroupHits(t, group)
			assert.Equal(t, groupsOrder[i], groupedBy)
			require.NotEmpty(t, ids)
			if i == 0 {
				// the searched object itself is the closest hit
				assert.Equal(t, documents.PassageIDs[0].String(), ids[0])
			}
			assert.IsNonDecreasing(t, distances)
			assert.Subset(t, expectedResults[groupedBy], ids)
			assert.Len(t, ids, min(objectsPerGroup, len(expectedResults[groupedBy])))
			contents := getContents(t, group)
			for _, content := range contents {
				assert.True(t, strings.HasPrefix(content, "Content of Passage"))
			}
		}
	}
	delete := func(t *testing.T) {
		helper.DeleteClass(t, documents.Passage)
		helper.DeleteClass(t, documents.Document)
	}
	// tests
	tests := []struct {
		name                    string
		multishard              bool
		groups, objectsPerGroup int
	}{
		{
			name:            "single shard - 2 groups 10 objects per group",
			multishard:      false,
			groups:          2,
			objectsPerGroup: 10,
		},
		{
			name:            "multi shard -  2 groups 10 objects per group",
			multishard:      true,
			groups:          2,
			objectsPerGroup: 10,
		},
		{
			name:            "single shard -  1 groups 1 objects per group",
			multishard:      false,
			groups:          1,
			objectsPerGroup: 1,
		},
		{
			name:            "multi shard -  1 groups 1 objects per group",
			multishard:      true,
			groups:          1,
			objectsPerGroup: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("create", func(t *testing.T) {
				create(t, tt.multishard)
			})
			t.Run("group by", func(t *testing.T) {
				groupBy(t, tt.groups, tt.objectsPerGroup)
			})
			t.Run("delete", func(t *testing.T) {
				delete(t)
				// The delete is not strongly consistent but the create method will use a strongly consistent check to
				// ensure that the class doesn't exists. Introduce the sleep to ensure we don't fail creating the class
				// due to consistency issue on the subsequent run
				time.Sleep(500 * time.Millisecond)
			})
		})
	}
}
