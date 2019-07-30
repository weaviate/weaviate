//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

// import (
// 	"encoding/json"
// 	"testing"

// 	"github.com/semi-technologies/weaviate/test/acceptance/helper"
// 	"github.com/stretchr/testify/assert"
// )

// func TestNetworkAggregate(t *testing.T) {
// 	result := AssertGraphQL(t, helper.RootAuth, `
// 		{
// 			Network {
// 				Aggregate{
// 					RemoteWeaviateForAcceptanceTest {
// 						Things {
// 							Instruments(groupBy:["name"]) {
// 								volume {
// 									count
// 								}
// 							}
// 						}
// 					}
// 				}
// 			}
// 		}
// 	`)

// 	volume := result.Get("Network", "Aggregate", "RemoteWeaviateForAcceptanceTest", "Things", "Instruments").Result
// 	expected := []interface{}{
// 		map[string]interface{}{
// 			"volume": map[string]interface{}{
// 				"count": json.Number("82"),
// 			},
// 		},
// 	}

// 	assert.Equal(t, expected, volume)
// }
