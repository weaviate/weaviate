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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// run by setup_test.go
func gettingObjects(t *testing.T) {
	t.Run("listing cities without references", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, "{  Get { City { name } } }")
		cities := result.Get("Get", "City").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"name": "Amsterdam"},
			map[string]interface{}{"name": "Rotterdam"},
			map[string]interface{}{"name": "Berlin"},
			map[string]interface{}{"name": "Dusseldorf"},
			map[string]interface{}{"name": "Missing Island"},
			map[string]interface{}{"name": nil},
		}

		assert.ElementsMatch(t, expected, cities)
	})

	t.Run("listing cities with relations", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, "{ Get { City { name, inCountry { ... on Country { name } } } } }")
		cities := result.Get("Get", "City").AsSlice()

		expected := parseJSONSlice(`[
    { "name": "Amsterdam",  "inCountry": [{ "name": "Netherlands" }] },
    { "name": "Rotterdam",  "inCountry": [{ "name": "Netherlands" }] },
    { "name": "Berlin",     "inCountry": [{ "name": "Germany" }] },
    { "name": "Dusseldorf", "inCountry": [{ "name": "Germany" }] },
    { "name": "Missing Island", "inCountry": null },
    { "name": null, "inCountry": null }
  ]`)

		assert.ElementsMatch(t, expected, cities)
	})

	t.Run("make sure raw response contains no error key", func(t *testing.T) {
		// This test prevents a regression on gh-1535

		query := []byte(`{"query":"{ Get { City { name } } }"}`)
		res, err := http.Post(fmt.Sprintf("%s%s", helper.GetWeaviateURL(), "/v1/graphql"),
			"application/json", bytes.NewReader(query))
		require.Nil(t, err)

		defer res.Body.Close()
		var body map[string]interface{}
		err = json.NewDecoder(res.Body).Decode(&body)
		require.Nil(t, err)

		_, ok := body["errors"]
		assert.False(t, ok)

		cities := body["data"].(map[string]interface{})["Get"].(map[string]interface{})["City"].([]interface{})
		assert.Greater(t, len(cities), 0)
	})

	t.Run("listing cities with limit", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, "{  Get { City(limit: 2) { name } } }")
		cities := result.Get("Get", "City").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"name": "Rotterdam"},
			map[string]interface{}{"name": "Dusseldorf"},
		}

		assert.ElementsMatch(t, expected, cities)
	})

	t.Run("listing cities with offset and limit", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, "{  Get { City(offset: 2 limit: 2) { name } } }")
		cities := result.Get("Get", "City").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"name": "Missing Island"},
			map[string]interface{}{"name": nil},
		}

		assert.ElementsMatch(t, expected, cities)
	})

	t.Run("listing cities with offset", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, "{  Get { City(offset: 2) { name } } }")
		cities := result.Get("Get", "City").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"name": "Missing Island"},
			map[string]interface{}{"name": nil},
			map[string]interface{}{"name": "Amsterdam"},
			map[string]interface{}{"name": "Berlin"},
		}

		assert.ElementsMatch(t, expected, cities)
	})

	t.Run("listing cities with offset and limit beyond results size", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, "{  Get { City(offset: 5 limit: 10) { name } } }")
		cities := result.Get("Get", "City").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"name": "Berlin"},
		}

		assert.ElementsMatch(t, expected, cities)
	})

	t.Run("listing cities with offset beyond results size", func(t *testing.T) {
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, "{  Get { City(offset: 6) { name } } }")
		cities := result.Get("Get", "City").AsSlice()

		expected := []interface{}{}

		assert.ElementsMatch(t, expected, cities)
	})
}
