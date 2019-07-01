/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocal_Fetch_LargeCities(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, `
		{
			Local {
				Fetch {
					Things(where: {
						class: {
							name: "town",
							certainty: 0.5
						},
						properties: {
							name: "population",
							operator: GreaterThan,
							valueInt: 1800000,
							certainty: 1
						}
					}) {
						beacon
					}
				}
			}
		}
	`)

	t.Run("finds exactly one result", func(t *testing.T) {
		beacons := result.Get("Local", "Fetch", "Things").Result
		expectedLen := 1 // only Berlin
		assert.Len(t, beacons, expectedLen)
	})
}

func TestLocal_Fetch_SmallCities(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, `
		{
			Local {
				Fetch {
					Things(where: {
						class: {
							name: "town",
							certainty: 0.5
						},
						properties: {
							name: "population",
							operator: LessThanEqual,
							valueInt: 1800000,
							certainty: 1
						}
					}) {
						beacon
					}
				}
			}
		}
	`)

	t.Run("finds exactly one result", func(t *testing.T) {
		beacons := result.Get("Local", "Fetch", "Things").Result
		expectedLen := 3 // Amsterdam, Rotterdam, Dusselsorf
		assert.Len(t, beacons, expectedLen)
	})
}

func TestLocal_FetchFuzzy_FavorableCities(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, `
		{
			Local {
				Fetch {
					Fuzzy (value:"good", certainty: 0.4) {
						beacon
					}
				}
			}
		}
	`)

	t.Run("finds exactly one result", func(t *testing.T) {
		results := result.Get("Local", "Fetch", "Fuzzy").Result
		expectedLen := 1
		require.Len(t, results, expectedLen)

		entry := results.([]interface{})[0]
		beacon := entry.(map[string]interface{})["beacon"].(string)
		pathSegments := strings.Split(assertParseURL(t, beacon).Path, "/")
		require.Len(t, pathSegments, 3)

		kind, uuid := pathSegments[1], pathSegments[2]
		require.Equal(t, "things", kind)
		thing := assertGetThing(t, strfmt.UUID(uuid))
		name := thing.Schema.(map[string]interface{})["name"].(string)
		assert.Equal(t, "Amsterdam", name)
	})
}

func TestLocal_FetchFuzzy_UnfavorableCities(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, `
		{
			Local {
				Fetch {
					Fuzzy (value:"poor", certainty: 0.4) {
						beacon
					}
				}
			}
		}
	`)

	t.Run("finds exactly one result", func(t *testing.T) {
		results := result.Get("Local", "Fetch", "Fuzzy").Result
		expectedLen := 1
		require.Len(t, results, expectedLen)

		entry := results.([]interface{})[0]
		beacon := entry.(map[string]interface{})["beacon"].(string)
		pathSegments := strings.Split(assertParseURL(t, beacon).Path, "/")
		require.Len(t, pathSegments, 3)

		kind, uuid := pathSegments[1], pathSegments[2]
		require.Equal(t, "things", kind)
		thing := assertGetThing(t, strfmt.UUID(uuid))
		name := thing.Schema.(map[string]interface{})["name"].(string)
		assert.Equal(t, "Berlin", name)
	})
}

func assertParseURL(t *testing.T, input string) *url.URL {
	res, err := url.Parse(input)
	require.Nil(t, err, "url parsing should not error")
	return res
}
