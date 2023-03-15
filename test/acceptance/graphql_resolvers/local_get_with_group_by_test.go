//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"fmt"
	"testing"

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	"github.com/weaviate/weaviate/test/helper/sample-schema/documents"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
)

func groupByObjects(t *testing.T) {
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
		groupValue := group["groupValue"].(string)
		return groupValue, result
	}

	t.Run("group by: people by city", func(t *testing.T) {
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
							groupValue
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

		groupValue1 := `{"beacon":"weaviate://localhost/City/8f5f8e44-d348-459c-88b1-c1a44bb8f8be"}`
		expectedGroup1 := []string{
			"8615585a-2960-482d-b19d-8bee98ade52c",
			"3ef44474-b5e5-455d-91dc-d917b5b76165",
			"15d222c9-8c36-464b-bedb-113faa1c1e4c",
		}
		expectedResults[groupValue1] = expectedGroup1

		groupValue2 := `{"beacon":"weaviate://localhost/City/9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"}`
		expectedGroup2 := []string{
			"3ef44474-b5e5-455d-91dc-d917b5b76165",
			"15d222c9-8c36-464b-bedb-113faa1c1e4c",
		}
		expectedResults[groupValue2] = expectedGroup2

		groupValue3 := `{"beacon":"weaviate://localhost/City/6ffb03f8-a853-4ec5-a5d8-302e45aaaf13"}`
		expectedGroup3 := []string{
			"15d222c9-8c36-464b-bedb-113faa1c1e4c",
		}
		expectedResults[groupValue3] = expectedGroup3

		groupValue4 := ""
		expectedGroup4 := []string{
			"5d0fa6ee-21c4-4b46-a735-f0208717837d",
		}
		expectedResults[groupValue4] = expectedGroup4

		groupsOrder := []string{groupValue1, groupValue2, groupValue4, groupValue3}
		for i, current := range groups {
			group := getGroup(current)
			groupValue, ids := getGroupHits(group)
			assert.Equal(t, groupsOrder[i], groupValue)
			assert.ElementsMatch(t, expectedResults[groupValue], ids)
		}
	})

	t.Run("group by: passages by documents", func(t *testing.T) {
		create := func(t *testing.T, multishard bool) {
			for _, class := range documents.ClassesContextionaryVectorizer(multishard) {
				createObjectClass(t, class)
			}
			for _, obj := range documents.Objects() {
				helper.CreateObject(t, obj)
				helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
			}
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
								groupValue
								count
								maxDistance
								minDistance
								hits {
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

			expectedResults := map[string][]string{}

			groupValue1 := `{"beacon":"weaviate://localhost/Document/00000000-0000-0000-0000-000000000011"}`
			expectedGroup1 := []string{
				documents.PassageIDs[0].String(),
				documents.PassageIDs[5].String(),
				documents.PassageIDs[4].String(),
				documents.PassageIDs[3].String(),
				documents.PassageIDs[2].String(),
				documents.PassageIDs[1].String(),
			}
			expectedResults[groupValue1] = expectedGroup1

			groupValue2 := `{"beacon":"weaviate://localhost/Document/00000000-0000-0000-0000-000000000012"}`
			expectedGroup2 := []string{
				documents.PassageIDs[6].String(),
				documents.PassageIDs[7].String(),
			}
			expectedResults[groupValue2] = expectedGroup2

			groupsOrder := []string{groupValue1, groupValue2}

			for i, current := range groups {
				group := getGroup(current)
				groupValue, ids := getGroupHits(group)
				assert.Equal(t, groupsOrder[i], groupValue)
				for j := range ids {
					assert.Equal(t, expectedResults[groupValue][j], ids[j])
				}
			}
		}
		delete := func(t *testing.T) {
			deleteObjectClass(t, documents.Passage)
			deleteObjectClass(t, documents.Document)
		}

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
				})
			})
		}
	})
}
