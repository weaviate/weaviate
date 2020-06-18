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
