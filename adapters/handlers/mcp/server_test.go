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
	"testing"

	mcplib "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/metrics"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// TestToolFilter_WriteDisabled pins that a tools/call for a write tool still
// reaches the handler and gets the write-disabled hint.
func TestToolFilter_WriteDisabled(t *testing.T) {
	logger, _ := test.NewNullLogger()
	composer := func(string, []string) (*models.Principal, error) { return &models.Principal{}, nil }
	authHandler := auth.NewAuth(false, composer, &authorization.DummyAuthorizer{}, nil)
	creator := create.NewWeaviateCreator(authHandler, nil, logger, func() bool { return false })

	s := &MCPServer{
		server:         server.NewMCPServer("test", "0", server.WithToolCapabilities(true)),
		creator:        creator,
		writeToolNames: map[string]bool{},
	}
	writeTools := create.Tools(creator, nil, nil)
	for _, tool := range writeTools {
		s.writeToolNames[tool.Tool.Name] = true
	}
	s.server.AddTools(writeTools...)
	s.server.AddTools(server.ServerTool{
		Tool: mcplib.NewTool("read-tool"),
		Handler: func(context.Context, mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
			return mcplib.NewToolResultText("ok"), nil
		},
	})
	s.registerToolFilter()

	handle := func(t *testing.T, body string) string {
		t.Helper()
		raw, err := json.Marshal(s.server.HandleMessage(context.Background(), []byte(body)))
		require.NoError(t, err)
		return string(raw)
	}

	t.Run("tools/call reaches the handler", func(t *testing.T) {
		called := handle(t, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"weaviate-objects-upsert","arguments":{"collection_name":"Things","objects":[{"properties":{}}]}}}`)
		require.Contains(t, called, "write access is disabled")
		require.NotContains(t, called, "not found")
	})
}

// TestToolsListedMetric pins that the tools-listed counter tracks tools/list
// requests only. The tool filter also runs for every tools/call, which used
// to count each read-tool call as a listing.
func TestToolsListedMetric(t *testing.T) {
	logger, _ := test.NewNullLogger()
	composer := func(string, []string) (*models.Principal, error) { return &models.Principal{}, nil }
	authHandler := auth.NewAuth(false, composer, &authorization.DummyAuthorizer{}, nil)
	writeEnabled := func() bool { return true }
	reg := prometheus.NewRegistry()
	m := metrics.New(reg, writeEnabled)
	creator := create.NewWeaviateCreator(authHandler, nil, logger, writeEnabled)

	s := &MCPServer{
		server: server.NewMCPServer("test", "0",
			server.WithToolCapabilities(true),
			server.WithHooks(listMetricsHooks(m, writeEnabled)),
		),
		creator:        creator,
		metrics:        m,
		writeToolNames: map[string]bool{},
	}
	s.server.AddTools(server.ServerTool{
		Tool: mcplib.NewTool("read-tool"),
		Handler: func(context.Context, mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
			return mcplib.NewToolResultText("ok"), nil
		},
	})
	s.registerToolFilter()

	listedTotal := func(t *testing.T) float64 {
		t.Helper()
		families, err := reg.Gather()
		require.NoError(t, err)
		total := 0.0
		for _, mf := range families {
			if mf.GetName() == "weaviate_mcp_tools_listed_total" {
				for _, metric := range mf.GetMetric() {
					total += metric.GetCounter().GetValue()
				}
			}
		}
		return total
	}

	const callBody = `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read-tool","arguments":{}}}`
	const listBody = `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`

	steps := []struct {
		name       string
		body       string
		wantListed float64
	}{
		{"read-tool call does not count", callBody, 0},
		{"tools/list counts", listBody, 1},
		{"another call still does not count", callBody, 1},
		{"another tools/list counts again", listBody, 2},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			raw, err := json.Marshal(s.server.HandleMessage(context.Background(), []byte(step.body)))
			require.NoError(t, err)
			require.NotContains(t, string(raw), `"error"`)
			require.Equal(t, step.wantListed, listedTotal(t))
		})
	}
}

// TestToolInputSchemas pins that every tool advertises its arguments; the
// schema generator leaves the schema empty when it cannot build one.
func TestToolInputSchemas(t *testing.T) {
	var tools []server.ServerTool
	tools = append(tools, search.Tools(nil, nil, nil)...)
	tools = append(tools, read.Tools(nil, nil, nil)...)
	tools = append(tools, create.Tools(nil, nil, nil)...)
	byName := map[string]mcplib.Tool{}
	for _, tool := range tools {
		byName[tool.Tool.Name] = tool.Tool
	}

	cases := []struct {
		name       string
		properties []string
		required   []string
	}{
		{
			name: "weaviate-query-hybrid",
			properties: []string{
				"query", "collection_name", "tenant_name", "alpha", "limit", "target_vectors",
				"target_properties", "return_properties", "return_metadata", "filters",
			},
			required: []string{"query", "collection_name"},
		},
		{name: "weaviate-collections-get-config", properties: []string{"collection_name"}},
		{name: "weaviate-tenants-list", properties: []string{"collection_name"}, required: []string{"collection_name"}},
		{
			name:       "weaviate-objects-upsert",
			properties: []string{"collection_name", "tenant_name", "objects"},
			required:   []string{"collection_name", "objects"},
		},
	}
	require.Len(t, byName, len(cases))

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tool, ok := byName[tc.name]
			require.True(t, ok)
			var schema struct {
				Type       string         `json:"type"`
				Properties map[string]any `json:"properties"`
				Required   []string       `json:"required"`
			}
			require.NoError(t, json.Unmarshal(tool.RawInputSchema, &schema), "tool %q has no input schema", tc.name)
			require.Equal(t, "object", schema.Type)
			require.Len(t, schema.Properties, len(tc.properties))
			for _, name := range tc.properties {
				require.Contains(t, schema.Properties, name)
			}
			require.ElementsMatch(t, tc.required, schema.Required)
		})
	}
}
