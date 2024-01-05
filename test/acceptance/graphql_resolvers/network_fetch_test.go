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

// func Test_NetworkFetch(t *testing.T) {
// 	result := AssertGraphQL(t, helper.RootAuth, `
//     {
// 			Network {
// 				Fetch {
// 					Things(where: {
// 						class: {
// 							name: "bestclass"
// 							certainty: 0.8
// 							keywords: [{value: "foo", weight: 0.9}]
// 						},
// 						properties: {
// 							name: "bestproperty"
// 							certainty: 0.8
// 							keywords: [{value: "bar", weight: 0.9}]
// 							operator: Equal
// 							valueString: "some-value"
// 						},
// 					}) {
// 						beacon certainty
// 					}
// 				}
// 			}
// 		}`,
// 	)

// 	results := result.Get("Network", "Fetch", "Things").Result
// 	expected := []interface{}{
// 		map[string]interface{}{
// 			"beacon":    "weaviate://RemoteWeaviateForAcceptanceTest/things/c2b94c9a-fea2-4f9a-ae40-6d63534633f7",
// 			"certainty": json.Number("0.5"),
// 		},
// 		map[string]interface{}{
// 			"beacon":    "weaviate://RemoteWeaviateForAcceptanceTest/things/32fc9b12-00b8-46b2-962d-63c1f352e090",
// 			"certainty": json.Number("0.7"),
// 		},
// 	}
// 	assert.Equal(t, expected, results)
// }

// func Test_NetworkFetchFuzzy(t *testing.T) {
// 	result := AssertGraphQL(t, helper.RootAuth, `
//     {
// 			Network {
// 				Fetch {
// 					Fuzzy(value:"something", certainty: 0.5) {
// 						beacon certainty
// 					}
// 				}
// 			}
// 		}`,
// 	)

// 	results := result.Get("Network", "Fetch", "Fuzzy").Result
// 	expected := []interface{}{
// 		map[string]interface{}{
// 			"beacon":    "weaviate://RemoteWeaviateForAcceptanceTest/things/61c21951-3460-4189-86ad-884a17b70c16",
// 			"certainty": json.Number("0.5"),
// 		},
// 	}
// 	assert.Equal(t, expected, results)
// }
