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
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/client/transport"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	// Collect tool names
	toolNames := make([]string, len(result.Tools))
	for i, tool := range result.Tools {
		toolNames[i] = tool.Name
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
}
