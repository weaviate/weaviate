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

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"gopkg.in/yaml.v3"
)

const (
	descTestAPIKey  = "admin-key"
	descTestAPIUser = "admin"
)

// repoRoot returns the absolute path to the repository root.
func repoRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
}

// defaultDescriptions maps tool names to their hardcoded default descriptions.
var defaultDescriptions = map[string]string{
	"weaviate-collections-get-config": "Retrieves collection configuration(s). If collection_name is provided, returns only that collection's config. Otherwise returns all collections.",
	"weaviate-tenants-list":           "Lists the tenants of a collection in the database.",
	"weaviate-query-hybrid":           "Performs hybrid search (vector + keyword) for data in a collection.",
	"weaviate-objects-upsert":         "Upserts (inserts or updates) one or more objects into a collection in batch. Supports batch operations for efficient bulk inserts and updates.",
}

// toolConfigFile mirrors the config file structure for parsing.
type toolConfigFile struct {
	Tools map[string]struct {
		Description string `yaml:"description" json:"description"`
	} `yaml:"tools" json:"tools"`
}

// loadExpectedDescriptions parses the given config file and returns a map
// of tool names to their expected descriptions.
func loadExpectedDescriptions(t *testing.T, configFileName string) map[string]string {
	t.Helper()

	root := repoRoot()
	configPath := filepath.Join(root, "tools", "dev", configFileName)

	data, err := os.ReadFile(configPath)
	require.NoError(t, err, "config file %q should be readable at %s", configFileName, configPath)

	var config toolConfigFile
	if strings.HasSuffix(configFileName, ".json") {
		err = json.Unmarshal(data, &config)
	} else {
		err = yaml.Unmarshal(data, &config)
	}
	require.NoError(t, err, "config file %q should parse successfully", configFileName)

	descriptions := make(map[string]string, len(config.Tools))
	for name, tc := range config.Tools {
		if tc.Description != "" {
			descriptions[name] = tc.Description
		}
	}
	require.NotEmpty(t, descriptions, "config file %s should contain at least one tool description", configFileName)
	return descriptions
}

// startMCPWithConfig starts a Weaviate testcontainer with RBAC, API key auth,
// MCP enabled, and the given config file copied into the container.
// It returns the MCP URL and a cleanup function.
func startMCPWithConfig(t *testing.T, configFileName string) (mcpURL string, cleanup func()) {
	t.Helper()
	ctx := context.Background()

	root := repoRoot()
	hostPath := filepath.Join(root, "tools", "dev", configFileName)
	containerPath := fmt.Sprintf("/app/tools/dev/%s", configFileName)

	compose, err := docker.New().
		WithWeaviate().
		WithRBAC().
		WithApiKey().
		WithUserApiKey(descTestAPIUser, descTestAPIKey).
		WithRbacRoots(descTestAPIUser).
		WithMCP().
		WithMCPConfigFile(hostPath, containerPath).
		Start(ctx)
	require.NoError(t, err)

	mcpURL = "http://" + compose.GetWeaviate().GetEndpoint(docker.MCP)
	cleanup = func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Logf("failed to terminate test containers: %v", err)
		}
	}
	return mcpURL, cleanup
}

// verifyToolDescriptions connects to the MCP server, lists tools, and asserts
// that each tool's description matches the expected config description and
// differs from the hardcoded default.
func verifyToolDescriptions(t *testing.T, mcpURL, apiKey string, expectedDescriptions map[string]string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := client.NewStreamableHttpClient(mcpURL,
		transport.WithHTTPHeaders(map[string]string{
			"Authorization": "Bearer " + apiKey,
		}),
	)
	require.NoError(t, err)

	_, err = c.Initialize(ctx, mcp.InitializeRequest{})
	require.NoError(t, err)

	result, err := c.ListTools(ctx, mcp.ListToolsRequest{})
	require.NoError(t, err)
	require.NotNil(t, result)

	toolDescriptions := make(map[string]string, len(result.Tools))
	for _, tool := range result.Tools {
		toolDescriptions[tool.Name] = tool.Description
	}

	for toolName, expectedDesc := range expectedDescriptions {
		actualDesc, found := toolDescriptions[toolName]
		require.True(t, found, "tool %q should be present in tools/list response", toolName)
		assert.Equal(t, expectedDesc, actualDesc,
			"tool %q description should match the config file", toolName)

		if defaultDesc, hasDefault := defaultDescriptions[toolName]; hasDefault {
			assert.NotEqual(t, defaultDesc, actualDesc,
				"tool %q description should differ from the hardcoded default (config loading may have failed)", toolName)
		}
	}
}

func TestToolsList(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := client.NewStreamableHttpClient(testMCPURL,
		transport.WithHTTPHeaders(map[string]string{
			"Authorization": "Bearer " + testAPIKey,
		}),
	)
	require.Nil(t, err)

	_, err = c.Initialize(ctx, mcp.InitializeRequest{})
	require.Nil(t, err)

	result, err := c.ListTools(ctx, mcp.ListToolsRequest{})
	require.Nil(t, err)
	require.NotNil(t, result)

	// Collect tool names and descriptions
	toolNames := make([]string, len(result.Tools))
	toolDescsByName := make(map[string]string, len(result.Tools))
	for i, tool := range result.Tools {
		toolNames[i] = tool.Name
		toolDescsByName[tool.Name] = tool.Description
	}

	// Verify all expected tools are present
	expectedTools := []string{
		"weaviate-collections-get-config",
		"weaviate-tenants-list",
		"weaviate-query-hybrid",
		"weaviate-objects-upsert",
	}
	for _, expected := range expectedTools {
		assert.Contains(t, toolNames, expected, "tool %q should be discoverable", expected)
	}

	// Verify each tool has a non-empty description and input schema
	for _, tool := range result.Tools {
		assert.NotEmpty(t, tool.Name, "tool should have a name")
		assert.NotEmpty(t, tool.Description, "tool %q should have a description", tool.Name)
	}

	// Verify descriptions match the config file (loaded via volume mount).
	// The docker-compose-mcp-test.yml mounts config.mcp.json into the container.
	expectedDescs := loadExpectedDescriptions(t, "config.mcp.json")
	for toolName, expectedDesc := range expectedDescs {
		actualDesc, found := toolDescsByName[toolName]
		if !found {
			continue // tool presence already checked above
		}
		assert.Equal(t, expectedDesc, actualDesc,
			"tool %q description should match config.mcp.json", toolName)
	}
}

func TestToolDescriptionsFromJSONConfig(t *testing.T) {
	expected := loadExpectedDescriptions(t, "config.mcp.json")
	mcpURL, cleanup := startMCPWithConfig(t, "config.mcp.json")
	defer cleanup()

	verifyToolDescriptions(t, mcpURL, descTestAPIKey, expected)
}

func TestToolDescriptionsFromYAMLConfig(t *testing.T) {
	expected := loadExpectedDescriptions(t, "config.mcp.yaml")
	mcpURL, cleanup := startMCPWithConfig(t, "config.mcp.yaml")
	defer cleanup()

	verifyToolDescriptions(t, mcpURL, descTestAPIKey, expected)
}
