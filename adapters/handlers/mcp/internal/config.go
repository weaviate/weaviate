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
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// ToolConfig represents the configuration for a single tool.
// Note: only tool descriptions and argument descriptions are supported because
// the MCP spec's tools/list response does not include output schemas.
type ToolConfig struct {
	Description string            `yaml:"description" json:"description"`
	Arguments   map[string]string `yaml:"arguments" json:"arguments"`
}

// Config represents the MCP server configuration from YAML or JSON
type Config struct {
	Tools map[string]ToolConfig `yaml:"tools" json:"tools"`
}

// LoadConfig loads the MCP configuration from a YAML or JSON file
// Returns nil if no config file is specified or if loading fails
func LoadConfig(logger logrus.FieldLogger, configPath string) *Config {
	if configPath == "" {
		return nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		logger.WithField("config_path", configPath).
			Errorf("failed to read MCP config file, using default tool descriptions: %v", err)
		return nil
	}

	var config Config
	ext := strings.ToLower(filepath.Ext(configPath))

	// Determine format based on file extension
	if ext == ".json" {
		if err := json.Unmarshal(data, &config); err != nil {
			logger.WithField("config_path", configPath).
				Errorf("failed to parse MCP config JSON, using default tool descriptions: %v", err)
			return nil
		}
	} else {
		// Default to YAML for .yaml, .yml, or unknown extensions
		if err := yaml.Unmarshal(data, &config); err != nil {
			logger.WithField("config_path", configPath).
				Errorf("failed to parse MCP config YAML, using default tool descriptions: %v", err)
			return nil
		}
	}

	return &config
}

// ToToolConfigMap converts the config to a map of tool names to their full config.
func (c *Config) ToToolConfigMap() map[string]ToolConfig {
	if c == nil {
		return nil
	}
	return c.Tools
}

// GetDescription returns the custom description for a tool if available in the config,
// otherwise returns the default description.
func GetDescription(configs map[string]ToolConfig, toolName, defaultDesc string) string {
	if configs != nil {
		if cfg, ok := configs[toolName]; ok && cfg.Description != "" {
			return cfg.Description
		}
	}
	return defaultDesc
}

// ApplySchemaDescriptions overrides argument property descriptions on a tool's
// input JSON schema using values from the config file.
func ApplySchemaDescriptions(tool *mcp.Tool, toolName string, configs map[string]ToolConfig) {
	if configs == nil {
		return
	}
	cfg, ok := configs[toolName]
	if !ok {
		return
	}
	if len(cfg.Arguments) > 0 && tool.RawInputSchema != nil {
		tool.RawInputSchema = overridePropertyDescriptions(tool.RawInputSchema, cfg.Arguments)
	}
}

// overridePropertyDescriptions modifies the "description" field of properties
// in a JSON schema.
func overridePropertyDescriptions(raw json.RawMessage, overrides map[string]string) json.RawMessage {
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		return raw
	}
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return raw
	}
	for name, desc := range overrides {
		if prop, ok := props[name].(map[string]any); ok {
			prop["description"] = desc
		}
	}
	result, err := json.Marshal(schema)
	if err != nil {
		return raw
	}
	return result
}

// AllowNullForOptionalArguments lets every optional argument in the tool's
// input schema be null, which the handlers treat as not set. Required
// arguments reject null.
func AllowNullForOptionalArguments(tool *mcp.Tool) {
	var schema map[string]any
	if err := json.Unmarshal(tool.RawInputSchema, &schema); err != nil {
		return
	}
	setNullable(schema)
	if raw, err := json.Marshal(schema); err == nil {
		tool.RawInputSchema = raw
	}
}

// setNullable allows null for the optional properties of an object schema and
// removes it from the required ones, including nested objects and array items.
func setNullable(schema map[string]any) {
	props, _ := schema["properties"].(map[string]any)
	required, _ := schema["required"].([]any)
	for name, p := range props {
		prop, ok := p.(map[string]any)
		if !ok {
			continue
		}
		setNullable(prop)
		if items, ok := prop["items"].(map[string]any); ok {
			setNullable(items)
		}
		setNull(prop, !slices.Contains(required, any(name)))
	}
}

// setNull adds null to a property's types, and to its enum if it has one, or
// removes it from both.
func setNull(prop map[string]any, allowed bool) {
	var types []any
	switch t := prop["type"].(type) {
	case string:
		types = []any{t}
	case []any:
		types = slices.Clone(t)
	default:
		return
	}
	types = slices.DeleteFunc(types, func(v any) bool { return v == "null" })
	if allowed {
		types = append(types, "null")
	}
	if len(types) == 1 {
		prop["type"] = types[0]
	} else {
		prop["type"] = types
	}
	if enum, ok := prop["enum"].([]any); ok {
		enum = slices.DeleteFunc(slices.Clone(enum), func(v any) bool { return v == nil })
		if allowed {
			enum = append(enum, nil)
		}
		prop["enum"] = enum
	}
}
