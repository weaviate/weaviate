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

package rest

import (
	"net/http"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/weaviate/weaviate/adapters/handlers/mcp"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	mcpops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/mcp"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
)

// setupMCPHandlers always registers the MCP HTTP handlers. Whether requests are
// served is decided per-request by checking the runtime-configurable
// MCP.Enabled flag, allowing operators to toggle MCP without a restart.
func setupMCPHandlers(api *operations.WeaviateAPI, appState *state.State, objectsManager *objects.Manager) {
	reg := monitoring.NoopRegisterer
	if appState.Metrics != nil {
		reg = appState.Metrics.Registerer
	}
	gate := mcpGate{
		enabled: appState.ServerConfig.Config.MCP.Enabled.Get,
		server:  mcp.NewMCPServer(appState, objectsManager, reg).Handler(),
	}

	api.McpMcpPostHandler = mcpops.McpPostHandlerFunc(
		func(params mcpops.McpPostParams, _ *models.Principal) middleware.Responder {
			return middleware.ResponderFunc(func(w http.ResponseWriter, _ runtime.Producer) {
				gate.serve(w, params.HTTPRequest)
			})
		},
	)
	api.McpMcpGetHandler = mcpops.McpGetHandlerFunc(
		func(params mcpops.McpGetParams, _ *models.Principal) middleware.Responder {
			return middleware.ResponderFunc(func(w http.ResponseWriter, _ runtime.Producer) {
				gate.rejectGet(w, params.HTTPRequest)
			})
		},
	)
	api.McpMcpDeleteHandler = mcpops.McpDeleteHandlerFunc(
		func(params mcpops.McpDeleteParams, _ *models.Principal) middleware.Responder {
			return middleware.ResponderFunc(func(w http.ResponseWriter, _ runtime.Producer) {
				gate.serve(w, params.HTTPRequest)
			})
		},
	)
}

const (
	mcpDisabledBody        = `{"error":"MCP server is not enabled. To enable it, either set MCP_SERVER_ENABLED=true (requires restart) or set mcp_server_enabled: true in the runtime overrides YAML (no restart needed). See https://docs.weaviate.io/weaviate/mcp/mcp-server"}`
	mcpGetNotSupportedBody = `{"error":"this server does not send server-to-client notifications, so GET is not supported; send JSON-RPC requests with POST"}`
)

// mcpGate answers every /v1/mcp method with 503 while MCP is disabled at
// runtime; otherwise POST and DELETE reach the MCP server and GET is refused.
type mcpGate struct {
	enabled func() bool
	server  http.Handler
}

func (g mcpGate) serve(w http.ResponseWriter, r *http.Request) {
	if !g.enabled() {
		writeMCPDisabled(w)
		return
	}
	g.server.ServeHTTP(w, r)
}

// GET would open a notification stream this server never writes to, and some
// proxies never tear such a stream down, so it is refused with 405 instead.
func (g mcpGate) rejectGet(w http.ResponseWriter, _ *http.Request) {
	if !g.enabled() {
		writeMCPDisabled(w)
		return
	}
	w.Header().Set("Allow", "POST, DELETE")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMethodNotAllowed)
	_, _ = w.Write([]byte(mcpGetNotSupportedBody))
}

func writeMCPDisabled(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(mcpDisabledBody))
}
