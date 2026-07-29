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

package restcompat

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestJSONProducer_ClassEmitsDerivedAsyncEnabled(t *testing.T) {
	original := asyncReplicationGloballyDisabled.Load()
	t.Cleanup(func() { asyncReplicationGloballyDisabled.Store(original) })

	cases := []struct {
		name             string
		factor           int64
		globallyDisabled bool
		want             bool
	}{
		{"factor 1 always false", 1, false, false},
		{"factor 1 globally disabled stays false", 1, true, false},
		{"factor 3 default true", 3, false, true},
		{"factor 3 global override wins", 3, true, false},
		{"factor 0 (omitted) stays false", 0, false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			SetAsyncReplicationGloballyDisabled(tc.globallyDisabled)
			class := &models.Class{
				Class:             "Foo",
				ReplicationConfig: &models.ReplicationConfig{Factor: tc.factor},
			}

			buf := encode(t, class)
			var got struct {
				Class             string `json:"class"`
				ReplicationConfig struct {
					Factor       int64 `json:"factor"`
					AsyncEnabled bool  `json:"asyncEnabled"`
				} `json:"replicationConfig"`
			}
			require.NoError(t, json.Unmarshal(buf, &got))
			assert.Equal(t, "Foo", got.Class)
			assert.Equal(t, tc.factor, got.ReplicationConfig.Factor)
			assert.Equal(t, tc.want, got.ReplicationConfig.AsyncEnabled,
				"factor=%d globalDisabled=%v", tc.factor, tc.globallyDisabled)
		})
	}
}

func TestJSONProducer_SchemaWrapsEveryClass(t *testing.T) {
	original := asyncReplicationGloballyDisabled.Load()
	t.Cleanup(func() { asyncReplicationGloballyDisabled.Store(original) })
	SetAsyncReplicationGloballyDisabled(false)

	schema := &models.Schema{
		Classes: []*models.Class{
			{Class: "A", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
			{Class: "B", ReplicationConfig: &models.ReplicationConfig{Factor: 3}},
		},
	}

	buf := encode(t, schema)
	var got struct {
		Classes []struct {
			Class             string `json:"class"`
			ReplicationConfig struct {
				Factor       int64 `json:"factor"`
				AsyncEnabled bool  `json:"asyncEnabled"`
			} `json:"replicationConfig"`
		} `json:"classes"`
	}
	require.NoError(t, json.Unmarshal(buf, &got))
	require.Len(t, got.Classes, 2)
	assert.Equal(t, "A", got.Classes[0].Class)
	assert.Equal(t, int64(1), got.Classes[0].ReplicationConfig.Factor)
	assert.False(t, got.Classes[0].ReplicationConfig.AsyncEnabled)
	assert.Equal(t, "B", got.Classes[1].Class)
	assert.Equal(t, int64(3), got.Classes[1].ReplicationConfig.Factor)
	assert.True(t, got.Classes[1].ReplicationConfig.AsyncEnabled)
}

func TestJSONProducer_PassesThroughUnrelatedTypes(t *testing.T) {
	type errPayload struct {
		Error []map[string]string `json:"error"`
	}
	payload := &errPayload{Error: []map[string]string{{"message": "boom"}}}

	buf := encode(t, payload)
	assert.NotContains(t, string(buf), "asyncEnabled")
}

func TestJSONProducer_NilReplicationConfig(t *testing.T) {
	original := asyncReplicationGloballyDisabled.Load()
	t.Cleanup(func() { asyncReplicationGloballyDisabled.Store(original) })
	SetAsyncReplicationGloballyDisabled(false)

	class := &models.Class{Class: "NoRep"}
	buf := encode(t, class)
	assert.NotContains(t, string(buf), "replicationConfig")
	assert.NotContains(t, string(buf), "asyncEnabled")
}

func TestJSONProducer_NilPayload(t *testing.T) {
	var class *models.Class
	buf := encode(t, class)
	assert.JSONEq(t, "null", string(buf))
}

func TestJSONProducer_SchemaPreservesClassesNilness(t *testing.T) {
	// models.Schema.Classes has no omitempty, so nil must marshal as
	// "classes":null while an empty (non-nil) slice marshals as
	// "classes":[].
	t.Run("nil slice stays null", func(t *testing.T) {
		buf := encode(t, &models.Schema{Classes: nil})
		assert.Contains(t, string(buf), `"classes":null`)
	})
	t.Run("empty slice stays []", func(t *testing.T) {
		buf := encode(t, &models.Schema{Classes: []*models.Class{}})
		assert.Contains(t, string(buf), `"classes":[]`)
		assert.NotContains(t, string(buf), `"classes":null`)
	})
}

func encode(t *testing.T, v interface{}) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, NewJSONProducer().Produce(&buf, v))
	// Trim the trailing newline the base producer appends.
	return bytes.TrimRight(buf.Bytes(), "\n")
}
