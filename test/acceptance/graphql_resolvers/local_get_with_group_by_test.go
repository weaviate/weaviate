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
	"fmt"
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

func groupByBm25(t *testing.T) {
	t.Run("group by: companies by city bm25", func(t *testing.T) {

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
				CompanyGroup(
					bm25:{
						query:"Inc Apple Microsoft"
					}
					groupBy:{
						path:["city"]
						groups:4
						objectsPerGroup: 10
					}
				){
					_additional{
						group{
							id
							groupedBy{value path}
							count
							maxDistance
							minDistance
							hits {
								name city
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
		groups := result.Get("Get", "CompanyGroup").AsSlice()
		fmt.Printf("groups: %+v\n", groups)

		require.Len(t, groups, 3)

		group1 := getGroup(groups[0])
		groupby, hits := getGroupHits(group1)

		t.Logf("groupby: %s, hits: %+v\n", groupby, hits)
		require.Equal(t, "dusseldorf", groupby)
		require.Len(t, hits, 2)
		require.Equal(t, hits[0], "1fa3b21e-ca4f-4db7-a432-7fc6a23c534d")
		require.Equal(t, hits[1], "1b2cfdba-d4ba-4cf8-abda-e719ef35ac33")

		group2 := getGroup(groups[1])
		groupby, hits = getGroupHits(group2)
		t.Logf("groupby: %s, hits: %+v\n", groupby, hits)
		require.Equal(t, "berlin", groupby)
		require.Len(t, hits, 2)
		require.Equal(t, hits[0], "177fec91-1292-4928-8f53-f0ff49c76900")
		require.Equal(t, hits[1], "1343f51d-7e05-4084-bd66-d504db3b6bec")

		group3 := getGroup(groups[2])
		groupby, hits = getGroupHits(group3)
		t.Logf("groupby: %s, hits: %+v\n", groupby, hits)
		require.Equal(t, "amsterdam", groupby)
		require.Len(t, hits, 3)
		require.Equal(t, hits[0], "171d2b4c-3da1-4684-9c5e-aabd2a4f2998")
		require.Equal(t, hits[1], "1f75ed97-39dd-4294-bff7-ecabd7923062")
		require.Equal(t, hits[2], "1c2e21fc-46fe-4999-b41c-a800595129af")

	})

}



func groupByHybridBm25(t *testing.T) {
	t.Run("group by: companies by city hybrid bm25", func(t *testing.T) {

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
				CompanyGroup(
					hybrid:{
						query:"Inc Apple Microsoft"
					}
					groupBy:{
						path:["city"]
						groups:4
						objectsPerGroup: 10
					}
				){
					_additional{
						group{
							id
							groupedBy{value path}
							count
							maxDistance
							minDistance
							hits {
								name city
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
		groups := result.Get("Get", "CompanyGroup").AsSlice()
		fmt.Printf("groups: %+v\n", groups)

		require.Len(t, groups, 3)

		group1 := getGroup(groups[2])
		groupby, hits := getGroupHits(group1)

		t.Logf("groupby: %s, hits: %+v\n", groupby, hits)
		require.Equal(t, "dusseldorf", groupby)
		require.Len(t, hits, 3)
		require.Equal(t, hits[0], "1fa3b21e-ca4f-4db7-a432-7fc6a23c534d")
		require.Equal(t, hits[1], "1b2cfdba-d4ba-4cf8-abda-e719ef35ac33")

		group2 := getGroup(groups[0])
		groupby, hits = getGroupHits(group2)
		t.Logf("groupby: %s, hits: %+v\n", groupby, hits)
		require.Equal(t, "berlin", groupby)
		require.Len(t, hits, 3)
		require.Equal(t, hits[0], "177fec91-1292-4928-8f53-f0ff49c76900")
		require.Equal(t, hits[1], "1343f51d-7e05-4084-bd66-d504db3b6bec")

		group3 := getGroup(groups[1])
		groupby, hits = getGroupHits(group3)
		t.Logf("groupby: %s, hits: %+v\n", groupby, hits)
		require.Equal(t, "amsterdam", groupby)
		require.Len(t, hits, 3)
		require.Equal(t, hits[0], "1f75ed97-39dd-4294-bff7-ecabd7923062")
		require.Equal(t, hits[1], "1c2e21fc-46fe-4999-b41c-a800595129af")
		require.Equal(t, hits[2], "171d2b4c-3da1-4684-9c5e-aabd2a4f2998")

	})

}
