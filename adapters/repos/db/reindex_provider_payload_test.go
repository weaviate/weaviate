//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractReindexTaskCollection(t *testing.T) {
	t.Run("well-formed payload returns class name", func(t *testing.T) {
		payload, err := json.Marshal(ReindexTaskPayload{
			MigrationType: ReindexTypeChangeTokenization,
			Collection:    "ProductCatalog",
		})
		assert.NoError(t, err)

		got, ok := ExtractReindexTaskCollection(payload)
		assert.True(t, ok)
		assert.Equal(t, "ProductCatalog", got)
	})

	t.Run("payload missing collection is rejected", func(t *testing.T) {
		payload, err := json.Marshal(ReindexTaskPayload{
			MigrationType: ReindexTypeEnableRangeable,
		})
		assert.NoError(t, err)

		got, ok := ExtractReindexTaskCollection(payload)
		assert.False(t, ok)
		assert.Equal(t, "", got)
	})

	t.Run("unparseable payload is rejected", func(t *testing.T) {
		got, ok := ExtractReindexTaskCollection([]byte("not json"))
		assert.False(t, ok)
		assert.Equal(t, "", got)
	})

	t.Run("payload with extra fields still parses", func(t *testing.T) {
		// ReindexTaskPayload may gain fields over time; the extractor must
		// stay forwards-compatible with unknown JSON keys.
		got, ok := ExtractReindexTaskCollection([]byte(
			`{"collection":"Foo","migrationType":"change-tokenization","futureField":42}`))
		assert.True(t, ok)
		assert.Equal(t, "Foo", got)
	})
}

// Adding a payload field must force a decision on whether GET /v1/tasks may show
// it to a caller without tenant read.
func TestTaskPayloadFieldsAreClassifiedForTasksList(t *testing.T) {
	tests := []struct {
		payload any
		visible []string
		hidden  []string
	}{
		{
			payload: ReindexTaskPayload{},
			visible: ReindexTaskVisiblePayloadFields,
			hidden:  []string{"properties", "tenants"},
		},
		{
			payload: DropVectorIndexTaskPayload{},
			visible: DropVectorIndexTaskVisiblePayloadFields,
			hidden:  []string{"targets", "unitToNode", "unitToShard", "cleanedShards", "deferredShards"},
		},
	}
	for _, tt := range tests {
		payloadType := reflect.TypeOf(tt.payload)
		t.Run(payloadType.Name(), func(t *testing.T) {
			fields := make([]string, 0, payloadType.NumField())
			for i := 0; i < payloadType.NumField(); i++ {
				name, _, _ := strings.Cut(payloadType.Field(i).Tag.Get("json"), ",")
				require.NotEmpty(t, name, "field %s needs a json name", payloadType.Field(i).Name)
				fields = append(fields, name)
			}

			require.ElementsMatch(t, fields, append(append([]string{}, tt.visible...), tt.hidden...),
				"each payload field must be in exactly one of the visible or hidden lists")
		})
	}
}
