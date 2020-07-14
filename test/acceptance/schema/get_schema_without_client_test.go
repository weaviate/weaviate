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
	"fmt"
	"net/http"
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testGetSchemaWithoutClient(t *testing.T) {
	res, err := http.Get(fmt.Sprintf("%s%s", helper.GetWeaviateURL(), "/v1/schema"))
	require.Nil(t, err)

	defer res.Body.Close()
	var body map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&body)
	require.Nil(t, err)

	expected := map[string]interface{}{
		"actions": map[string]interface{}{
			"type":    "action",
			"classes": []interface{}{},
		},
		"things": map[string]interface{}{
			"type": "thing",
			"classes": []interface{}{
				map[string]interface{}{
					"class":              "YellowCars",
					"properties":         (interface{})(nil),
					"vectorizeClassName": true,
				},
			},
		},
	}

	assert.Equal(t, expected, body)
}
