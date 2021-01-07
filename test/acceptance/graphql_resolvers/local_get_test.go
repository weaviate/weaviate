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
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

// run by setup_test.go
func gettingObjects(t *testing.T) {
	t.Run("listing cities without references", func(t *testing.T) {
		result := AssertGraphQL(t, helper.RootAuth, "{  Get { City { name } } }")
		cities := result.Get("Get", "City").AsSlice()

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
		result := AssertGraphQL(t, helper.RootAuth, "{ Get { City { name, inCountry { ... on Country { name } } } } }")
		cities := result.Get("Get", "City").AsSlice()

		expected := parseJSONSlice(`[
    { "name": "Amsterdam",  "inCountry": [{ "name": "Netherlands" }] },
    { "name": "Rotterdam",  "inCountry": [{ "name": "Netherlands" }] },
    { "name": "Berlin",     "inCountry": [{ "name": "Germany" }] },
    { "name": "Dusseldorf", "inCountry": [{ "name": "Germany" }] },
    { "name": "Null Island", "inCountry": null }
  ]`)

		assert.ElementsMatch(t, expected, cities)
	})
}
