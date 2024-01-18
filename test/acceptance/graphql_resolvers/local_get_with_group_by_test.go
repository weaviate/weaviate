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

package test

import (
	"testing"

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/journey"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
)

func groupByObjects(t *testing.T) {
	t.Run("group by: people by city", func(t *testing.T) {
		getGroup := func(value interface{}) map[string]interface{} {
			group := value.(map[string]interface{})["_additional"].(map[string]interface{})["group"].(map[string]interface{})
			return group
		}
		getGroupHits := func(group map[string]interface{}) (string, []string) {
			result := []string{}
			hits := group["hits"].([]interface{})
			for _, hit := range hits {
				additional := hit.(map[string]interface{})["_additional"].(map[string]interface{})
				result = append(result, additional["id"].(string))
			}
			groupedBy := group["groupedBy"].(map[string]interface{})
			groupedByValue := groupedBy["value"].(string)
			return groupedByValue, result
		}
		query := `
		{
			Get{
				Person(
					nearObject:{
						id: "8615585a-2960-482d-b19d-8bee98ade52c"
					}
					groupBy:{
						path:["livesIn"]
						groups:4
						objectsPerGroup: 10
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
								_additional {
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
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		groups := result.Get("Get", "Person").AsSlice()

		require.Len(t, groups, 4)

		expectedResults := map[string][]string{}

		groupedBy1 := `weaviate://localhost/City/8f5f8e44-d348-459c-88b1-c1a44bb8f8be`
		expectedGroup1 := []string{
			"8615585a-2960-482d-b19d-8bee98ade52c",
			"3ef44474-b5e5-455d-91dc-d917b5b76165",
			"15d222c9-8c36-464b-bedb-113faa1c1e4c",
		}
		expectedResults[groupedBy1] = expectedGroup1

		groupedBy2 := `weaviate://localhost/City/9b9cbea5-e87e-4cd0-89af-e2f424fd52d6`
		expectedGroup2 := []string{
			"3ef44474-b5e5-455d-91dc-d917b5b76165",
			"15d222c9-8c36-464b-bedb-113faa1c1e4c",
		}
		expectedResults[groupedBy2] = expectedGroup2

		groupedBy3 := `weaviate://localhost/City/6ffb03f8-a853-4ec5-a5d8-302e45aaaf13`
		expectedGroup3 := []string{
			"15d222c9-8c36-464b-bedb-113faa1c1e4c",
		}
		expectedResults[groupedBy3] = expectedGroup3

		groupedBy4 := ""
		expectedGroup4 := []string{
			"5d0fa6ee-21c4-4b46-a735-f0208717837d",
		}
		expectedResults[groupedBy4] = expectedGroup4

		groupsOrder := []string{groupedBy1, groupedBy2, groupedBy4, groupedBy3}
		for i, current := range groups {
			group := getGroup(current)
			groupedBy, ids := getGroupHits(group)
			assert.Equal(t, groupsOrder[i], groupedBy)
			assert.ElementsMatch(t, expectedResults[groupedBy], ids)
		}
	})

	t.Run("group by: passages by documents", func(t *testing.T) {
		journey.GroupBySingleAndMultiShardTests(t, "")
	})
}
