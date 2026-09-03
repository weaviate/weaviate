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

	t.Run("tools/call reaches the handler", func(t *testing.T) {
		called := handleMessage(t, s, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"weaviate-objects-upsert","arguments":{"collection_name":"Things","objects":[{"properties":{}}]}}}`)
		require.Contains(t, called, "write access is disabled")
		require.NotContains(t, called, "not found")
	})
}

// handleMessage runs one raw JSON-RPC message through the server and returns
// the marshaled response.
func handleMessage(t *testing.T, s *MCPServer, body string) string {
	t.Helper()
	raw, err := json.Marshal(s.server.HandleMessage(context.Background(), []byte(body)))
	require.NoError(t, err)
	return string(raw)
}

// TestInputSchemaValidation pins that tool calls are checked against the
// advertised input schema before the handler runs: unknown keys and missing
// required arguments are rejected, valid calls still reach the handler.
func TestInputSchemaValidation(t *testing.T) {
	logger, _ := test.NewNullLogger()
	composer := func(string, []string) (*models.Principal, error) { return &models.Principal{}, nil }
	authHandler := auth.NewAuth(false, composer, &authorization.DummyAuthorizer{}, nil)
	// Write access stays off so a call that passes validation answers with the
	// deterministic write-disabled hint instead of reaching the nil manager.
	writeDisabled := func() bool { return false }
	creator := create.NewWeaviateCreator(authHandler, nil, logger, writeDisabled)

	s := &MCPServer{server: server.NewMCPServer("test", "0", serverOptions(nil, writeDisabled)...)}
	s.server.AddTools(create.Tools(creator, nil, nil)...)
	s.server.AddTools(search.Tools(nil, nil, nil)...)

	call := func(name, args string) string {
		return `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + name + `","arguments":` + args + `}}`
	}
	const validationFailed = "input schema validation failed"

	tests := []struct {
		name    string
		body    string
		want    []string
		notWant []string
	}{
		{
			name: "unknown key on upsert is rejected, not silently dropped",
			body: call("weaviate-objects-upsert", `{"collection_name":"Things","objects":[{"properties":{}}],"vector":[0.1]}`),
			want: []string{validationFailed, "vector"},
			// must fail before the handler, which would answer write-disabled
			notWant: []string{"write access is disabled"},
		},
		{
			// the original incident: "vector" instead of "vectors" was silently
			// dropped and the object re-vectorized
			name: "unknown key inside an object is rejected too",
			body: call("weaviate-objects-upsert", `{"collection_name":"Things","objects":[{"properties":{},"vector":[0.1]}]}`),
			want: []string{validationFailed, "/objects/0", "vector"},
		},
		{
			name: "missing required arguments are a schema error",
			body: call("weaviate-objects-upsert", `{}`),
			want: []string{validationFailed, "collection_name"},
		},
		{
			name:    "valid upsert call reaches the handler",
			body:    call("weaviate-objects-upsert", `{"collection_name":"Things","objects":[{"properties":{}}]}`),
			want:    []string{"write access is disabled"},
			notWant: []string{validationFailed},
		},
		{
			name: "unknown key on hybrid is rejected even with filters present",
			body: call("weaviate-query-hybrid", `{"query":"q","collection_name":"Things","filters":{"operator":"Equal","path":["title"],"valueText":"x"},"vectorz":true}`),
			want: []string{validationFailed, "vectorz"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := handleMessage(t, s, tt.body)
			for _, want := range tt.want {
				require.Contains(t, got, want)
			}
			for _, notWant := range tt.notWant {
				require.NotContains(t, got, notWant)
			}
		})
	}
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
