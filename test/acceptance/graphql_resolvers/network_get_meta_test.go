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

// import (
// 	"encoding/json"
// 	"testing"

// 	"github.com/weaviate/weaviate/test/helper"
// 	"github.com/stretchr/testify/assert"
// )

// func TestNetworkMeta(t *testing.T) {
// 	result := AssertGraphQL(t, helper.RootAuth, `
// 		{
// 			Network {
// 				Meta{
// 					RemoteWeaviateForAcceptanceTest {
// 						Things {
// 							Instruments {
// 								volume {
// 									maximum
// 									minimum
// 									mean
// 								}
// 							}
// 						}
// 					}
// 				}
// 			}
// 		}
// 	`)

// 	volume := result.Get("Network", "Meta", "RemoteWeaviateForAcceptanceTest", "Things", "Instruments", "volume").Result
// 	expected := map[string]interface{}{
// 		"mean":    json.Number("82"),
// 		"maximum": json.Number("110"),
// 		"minimum": json.Number("65"),
// 	}
// 	assert.Equal(t, expected, volume)
// }
