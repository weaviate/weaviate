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
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
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

// newTestServer builds a server with the production options and tool filter,
// the real tools and a "read-tool" that answers "ok". The real tools have no
// backing services, so tests only call them in ways that fail before one is used.
func newTestServer(t *testing.T, writeEnabled bool) (*MCPServer, *prometheus.Registry) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	composer := func(string, []string) (*models.Principal, error) { return &models.Principal{}, nil }
	authHandler := auth.NewAuth(false, composer, &authorization.DummyAuthorizer{}, nil)
	writeAccessEnabled := func() bool { return writeEnabled }
	reg := prometheus.NewRegistry()
	m := metrics.New(reg, writeAccessEnabled)
	creator := create.NewWeaviateCreator(authHandler, nil, logger, writeAccessEnabled)

	s := &MCPServer{
		server:         server.NewMCPServer("test", "0", serverOptions(m, writeAccessEnabled)...),
		creator:        creator,
		metrics:        m,
		writeToolNames: map[string]bool{},
	}
	s.server.AddTools(search.Tools(nil, nil, m)...)
	s.server.AddTools(read.Tools(nil, nil, m)...)
	writeTools := create.Tools(creator, nil, m)
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
	return s, reg
}

// handleMessage posts one JSON-RPC message to the server's HTTP handler the way
// a proxy on the same host does: over loopback, with a non-localhost Host.
func handleMessage(t *testing.T, s *MCPServer, body string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://weaviate:8080/v1/mcp", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	loopback := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}
	req = req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey, loopback))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	return w.Body.String()
}

// TestToolFilter pins that tools/list shows write tools only while write access
// is enabled.
func TestToolFilter(t *testing.T) {
	const listBody = `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`

	tests := []struct {
		name         string
		writeEnabled bool
		wantUpsert   bool
	}{
		{name: "write access enabled lists write tools", writeEnabled: true, wantUpsert: true},
		{name: "write access disabled hides write tools", writeEnabled: false, wantUpsert: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestServer(t, tt.writeEnabled)
			got := handleMessage(t, s, listBody)
			require.Contains(t, got, `"weaviate-query-hybrid"`)
			require.Equal(t, tt.wantUpsert, strings.Contains(got, `"weaviate-objects-upsert"`), got)
		})
	}
}

// TestInputSchemaValidation pins that tool calls are checked against the
// advertised input schema before the handler runs: unknown keys, missing
// required arguments and null for a required argument are rejected, while null
// for an optional argument and other valid calls reach the handler.
func TestInputSchemaValidation(t *testing.T) {
	// Write access stays off so a call that passes validation answers with the
	// write-disabled error instead of reaching the nil manager.
	s, _ := newTestServer(t, false)

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
			name:    "null for optional arguments means not set",
			body:    call("weaviate-objects-upsert", `{"collection_name":"Things","tenant_name":null,"objects":[{"properties":{},"uuid":null,"vectors":null}]}`),
			want:    []string{"write access is disabled"},
			notWant: []string{validationFailed},
		},
		{
			name:    "null for a required argument is rejected",
			body:    call("weaviate-objects-upsert", `{"collection_name":"Things","objects":null}`),
			want:    []string{validationFailed, "/objects"},
			notWant: []string{"write access is disabled"},
		},
		{
			name: "null for a required field inside an object is rejected",
			body: call("weaviate-objects-upsert", `{"collection_name":"Things","objects":[{"properties":null}]}`),
			want: []string{validationFailed, "/objects/0/properties"},
		},
		{
			// also pins that the tool filter lets the call through
			name:    "valid upsert call reaches the handler",
			body:    call("weaviate-objects-upsert", `{"collection_name":"Things","objects":[{"properties":{}}]}`),
			want:    []string{"write access is disabled"},
			notWant: []string{validationFailed, "not found"},
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

// TestToolsListedMetric pins that the tools-listed counter counts tools/list
// requests only, not tool calls.
func TestToolsListedMetric(t *testing.T) {
	s, reg := newTestServer(t, true)

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
			require.NotContains(t, handleMessage(t, s, step.body), `"error"`)
			require.Equal(t, step.wantListed, listedTotal(t))
		})
	}
}

// TestToolInputSchemas pins that every tool advertises its arguments and that
// only optional arguments accept null; the schema generator leaves the schema
// empty when it cannot build one.
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
			for name, prop := range schema.Properties {
				types, _ := prop.(map[string]any)["type"].([]any)
				require.Equal(t, !slices.Contains(tc.required, name), slices.Contains(types, any("null")),
					"argument %q should accept null only when it is optional", name)
			}
		})
	}
}
