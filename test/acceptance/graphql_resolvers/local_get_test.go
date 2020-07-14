//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"encoding/json"
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

// run by setup_test.go
func gettingObjects(t *testing.T) {
	t.Run("listing cities without references", func(t *testing.T) {
		result := AssertGraphQL(t, helper.RootAuth, "{  Get { Things { City { name } } } }")
		cities := result.Get("Get", "Things", "City").AsSlice()

		expected := []interface{}{
			map[string]interface{}{"name": "Amsterdam"},
			map[string]interface{}{"name": "Rotterdam"},
			map[string]interface{}{"name": "Berlin"},
			map[string]interface{}{"name": "Dusseldorf"},
			map[string]interface{}{"name": "Null Island"},
		}

		assert.ElementsMatch(t, expected, cities)
	})

	t.Run("listing cities with relations", func(t *testing.T) {
		result := AssertGraphQL(t, helper.RootAuth, "{ Get { Things { City { name, InCountry { ... on Country { name } } } } } }")
		cities := result.Get("Get", "Things", "City").AsSlice()

		expected := parseJSONSlice(`[
    { "name": "Amsterdam",  "InCountry": [{ "name": "Netherlands" }] },
    { "name": "Rotterdam",  "InCountry": [{ "name": "Netherlands" }] },
    { "name": "Berlin",     "InCountry": [{ "name": "Germany" }] },
    { "name": "Dusseldorf", "InCountry": [{ "name": "Germany" }] },
    { "name": "Null Island", "InCountry": null }
  ]`)

		assert.ElementsMatch(t, expected, cities)
	})
}

func jsonify(stuff interface{}) string {
	j, _ := json.Marshal(stuff)
	return string(j)
}
