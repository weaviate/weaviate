//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package mcp

import (
	"os"

	"gopkg.in/yaml.v3"
)

// ToolConfig represents the configuration for a single tool
type ToolConfig struct {
	Description string `yaml:"description"`
}

// Config represents the MCP server configuration from YAML
type Config struct {
	Tools map[string]ToolConfig `yaml:"tools"`
}

// LoadConfig loads the MCP configuration from a YAML file
// Returns nil if no config file is specified or if loading fails
func LoadConfig() *Config {
	configPath := os.Getenv("MCP_SERVER_CONFIG_PATH")
	if configPath == "" {
		return nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		// Silently return nil if file cannot be read
		// The server will use default descriptions
		return nil
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		// Silently return nil if YAML is invalid
		// The server will use default descriptions
		return nil
	}

	return &config
}

// GetToolDescription returns the custom description for a tool if configured,
// otherwise returns the default description
func (c *Config) GetToolDescription(toolName, defaultDescription string) string {
	if c == nil {
		return defaultDescription
	}

	if toolConfig, exists := c.Tools[toolName]; exists && toolConfig.Description != "" {
		return toolConfig.Description
	}

	return defaultDescription
}

// ToDescriptionMap converts the config to a simple map of tool names to descriptions
// This is useful for passing to subpackages
func (c *Config) ToDescriptionMap() map[string]string {
	if c == nil {
		return nil
	}

	result := make(map[string]string)
	for toolName, toolConfig := range c.Tools {
		if toolConfig.Description != "" {
			result[toolName] = toolConfig.Description
		}
	}
	return result
}
