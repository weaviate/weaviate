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

package internal

import (
	"encoding/json"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testToolName = "my-tool"

func makeTestSchema(props map[string]any) json.RawMessage {
	schema := map[string]any{"type": "object", "properties": props}
	b, _ := json.Marshal(schema)
	return b
}

func schemaProps(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var schema map[string]any
	require.NoError(t, json.Unmarshal(raw, &schema))
	return schema["properties"].(map[string]any)
}

func propDesc(props map[string]any, name string) string {
	return props[name].(map[string]any)["description"].(string)
}

func makeTestConfigs(cfg ToolConfig) map[string]ToolConfig {
	return map[string]ToolConfig{testToolName: cfg}
}

func TestGetDescription(t *testing.T) {
	configs := makeTestConfigs(ToolConfig{Description: "custom desc"})

	tests := []struct {
		name     string
		configs  map[string]ToolConfig
		toolName string
		expected string
	}{
		{"returns custom description when present", configs, testToolName, "custom desc"},
		{"returns default when tool not in config", configs, "other-tool", "default"},
		{"returns default when configs is nil", nil, testToolName, "default"},
		{"returns default when description is empty", makeTestConfigs(ToolConfig{}), testToolName, "default"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, GetDescription(tc.configs, tc.toolName, "default"))
		})
	}
}

func TestApplySchemaDescriptions(t *testing.T) {
	stringProp := func(desc string) map[string]any {
		return map[string]any{"type": "string", "description": desc}
	}

	t.Run("overrides argument descriptions", func(t *testing.T) {
		tool := mcp.Tool{
			RawInputSchema: makeTestSchema(map[string]any{
				"query":           stringProp("original"),
				"collection_name": stringProp("original"),
			}),
		}
		ApplySchemaDescriptions(&tool, testToolName, makeTestConfigs(ToolConfig{
			Arguments: map[string]string{"query": "custom query description"},
		}))

		props := schemaProps(t, tool.RawInputSchema)
		assert.Equal(t, "custom query description", propDesc(props, "query"))
		assert.Equal(t, "original", propDesc(props, "collection_name"))
	})

	t.Run("no-op when tool not in config", func(t *testing.T) {
		original := makeTestSchema(map[string]any{"query": stringProp("original")})
		tool := mcp.Tool{RawInputSchema: json.RawMessage(append([]byte{}, original...))}

		ApplySchemaDescriptions(&tool, "other-tool", makeTestConfigs(ToolConfig{
			Arguments: map[string]string{"query": "custom"},
		}))
		assert.JSONEq(t, string(original), string(tool.RawInputSchema))
	})

	t.Run("no-op when configs is nil", func(t *testing.T) {
		original := makeTestSchema(map[string]any{"query": stringProp("original")})
		tool := mcp.Tool{RawInputSchema: json.RawMessage(append([]byte{}, original...))}

		ApplySchemaDescriptions(&tool, testToolName, nil)
		assert.JSONEq(t, string(original), string(tool.RawInputSchema))
	})

	t.Run("no-op when schema is nil", func(t *testing.T) {
		tool := mcp.Tool{}
		ApplySchemaDescriptions(&tool, testToolName, makeTestConfigs(ToolConfig{
			Arguments: map[string]string{"query": "custom"},
		}))
		assert.Nil(t, tool.RawInputSchema)
	})

	t.Run("ignores arguments not in schema", func(t *testing.T) {
		tool := mcp.Tool{
			RawInputSchema: makeTestSchema(map[string]any{"query": stringProp("original")}),
		}
		ApplySchemaDescriptions(&tool, testToolName, makeTestConfigs(ToolConfig{
			Arguments: map[string]string{"nonexistent": "should be ignored"},
		}))

		props := schemaProps(t, tool.RawInputSchema)
		assert.Equal(t, "original", propDesc(props, "query"))
		_, exists := props["nonexistent"]
		assert.False(t, exists)
	})
}

func TestToToolConfigMap(t *testing.T) {
	t.Run("returns nil for nil config", func(t *testing.T) {
		var c *Config
		assert.Nil(t, c.ToToolConfigMap())
	})

	t.Run("returns tools map", func(t *testing.T) {
		c := &Config{Tools: makeTestConfigs(ToolConfig{Description: "desc a"})}
		m := c.ToToolConfigMap()
		assert.Equal(t, "desc a", m[testToolName].Description)
	})
}
