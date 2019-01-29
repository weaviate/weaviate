/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
)

func TestNetworkGetMeta(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, `
		{
			Network {
				GetMeta{
					RemoteWeaviateForAcceptanceTest {
						Things {
							Instruments {
								volume {
									highest
									lowest
									average
								}
							}
						}
					}
				}
			}
		}
	`)

	volume := result.Get("Network", "GetMeta", "RemoteWeaviateForAcceptanceTest", "Things", "Instruments", "volume").Result
	fmt.Print("\n\n\n")
	spew.Dump(result.Result)
	fmt.Print("\n\n\n")
	expected := map[string]interface{}{
		"average": json.Number("82"),
		"highest": json.Number("110"),
		"lowest":  json.Number("65"),
	}
	assert.Equal(t, expected, volume)
}
