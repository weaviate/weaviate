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
 */
package test

import (
	"encoding/json"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func TestLocalGetSimple(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, "{ Local { Get { Things { City { name } } } } }")
	cities := result.Get("Local", "Get", "Things", "City").AsSlice()

	// Generated with
	// cat fixtures/data.json | jq '.Things[] | select(.class=="City") | .name'
	expected := []interface{}{
		map[string]interface{}{"name": "Amsterdam"},
		map[string]interface{}{"name": "Rotterdam"},
		map[string]interface{}{"name": "Berlin"},
		map[string]interface{}{"name": "Dusseldorf"},
	}

	assert.ElementsMatch(t, expected, cities)
}

func TestLocalGetRelation(t *testing.T) {
	result := AssertGraphQL(t, helper.RootAuth, "{ Local { Get { Things { City { name, InCountry { ... on Country { name } } } } } } }")
	cities := result.Get("Local", "Get", "Things", "City").AsSlice()

	// Generated with
	// cat test/acceptance/graphql_resolvers_local/fixtures/data.json | jq '.Things[] | select(.class=="City") | { "name": .name, "inCountry": { "name": .inCountry.name } }' | jq --slurp .
	// note: then titleized the ref name, and put the thing we refer to in a list.
	expected := parseJSONSlice(`[
    { "name": "Amsterdam",  "InCountry": [{ "name": "Netherlands" }] },
    { "name": "Rotterdam",  "InCountry": [{ "name": "Netherlands" }] },
    { "name": "Berlin",     "InCountry": [{ "name": "Germany" }] },
    { "name": "Dusseldorf", "InCountry": [{ "name": "Germany" }] }
  ]`)

	assert.ElementsMatch(t, expected, cities)
}

func jsonify(stuff interface{}) string {
	j, _ := json.Marshal(stuff)
	return string(j)
}
