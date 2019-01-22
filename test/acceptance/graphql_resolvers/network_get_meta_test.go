package test

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
