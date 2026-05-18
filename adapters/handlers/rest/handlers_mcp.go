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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/adapters/handlers/mcp"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	mcpops "github.com/weaviate/weaviate/adapters/handlers/rest/operations/mcp"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/objects"
)

// setupMCPHandlers always registers the MCP HTTP handlers. Whether requests are
// served is decided per-request by checking the runtime-configurable
// MCP.Enabled flag, allowing operators to toggle MCP without a restart.
func setupMCPHandlers(api *operations.WeaviateAPI, appState *state.State, objectsManager *objects.Manager) {
	mcpServer := mcp.NewMCPServer(appState, objectsManager, prometheus.DefaultRegisterer)
	mcpHandler := mcpServer.Handler()

	serveIfEnabled := func(w http.ResponseWriter, r *http.Request) {
		if !appState.ServerConfig.Config.MCP.Enabled.Get() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"MCP server is not enabled. To enable it, either set MCP_SERVER_ENABLED=true (requires restart) or set mcp_server_enabled: true in the runtime overrides YAML (no restart needed). See https://docs.weaviate.io/weaviate/mcp/mcp-server"}`))
			return
		}
		mcpHandler.ServeHTTP(w, r)
	}

	api.McpMcpPostHandler = mcpops.McpPostHandlerFunc(
		func(params mcpops.McpPostParams, _ *models.Principal) middleware.Responder {
			return middleware.ResponderFunc(func(w http.ResponseWriter, _ runtime.Producer) {
				serveIfEnabled(w, params.HTTPRequest)
			})
		},
	)
	api.McpMcpGetHandler = mcpops.McpGetHandlerFunc(
		func(params mcpops.McpGetParams, _ *models.Principal) middleware.Responder {
			return middleware.ResponderFunc(func(w http.ResponseWriter, _ runtime.Producer) {
				serveIfEnabled(w, params.HTTPRequest)
			})
		},
	)
	api.McpMcpDeleteHandler = mcpops.McpDeleteHandlerFunc(
		func(params mcpops.McpDeleteParams, _ *models.Principal) middleware.Responder {
			return middleware.ResponderFunc(func(w http.ResponseWriter, _ runtime.Producer) {
				serveIfEnabled(w, params.HTTPRequest)
			})
		},
	)
}
