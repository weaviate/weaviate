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
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// ToolConfig represents the configuration for a single tool
type ToolConfig struct {
	Description string `yaml:"description" json:"description"`
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
		logger.WithError(err).
			WithField("config_path", configPath).
			Error("failed to read MCP config file")
		return nil
	}

	var config Config
	ext := strings.ToLower(filepath.Ext(configPath))

	// Determine format based on file extension
	if ext == ".json" {
		if err := json.Unmarshal(data, &config); err != nil {
			logger.WithError(err).
				WithField("config_path", configPath).
				Error("failed to parse MCP config JSON")
			return nil
		}
	} else {
		// Default to YAML for .yaml, .yml, or unknown extensions
		if err := yaml.Unmarshal(data, &config); err != nil {
			logger.WithError(err).
				WithField("config_path", configPath).
				Error("failed to parse MCP config YAML")
			return nil
		}
	}

	return &config
}

// ToDescriptionMap converts the config to a simple map of tool names to descriptions
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

// GetDescription returns the custom description for a tool if available in the descriptions map,
// otherwise returns the default description. This is a helper for tool registration.
func GetDescription(descriptions map[string]string, toolName, defaultDesc string) string {
	if descriptions != nil {
		if customDesc, ok := descriptions[toolName]; ok {
			return customDesc
		}
	}
	return defaultDesc
}
