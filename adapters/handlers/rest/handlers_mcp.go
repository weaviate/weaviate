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
	"encoding/json"
	"net/http"
	"slices"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	mcplib "github.com/mark3labs/mcp-go/mcp"
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
//
// Only POST and DELETE are in the spec. This server never sends
// server-to-client notifications, so there is no event stream for GET to
// open; the router answers GET with 405 and an Allow header.
func setupMCPHandlers(api *operations.WeaviateAPI, appState *state.State, objectsManager *objects.Manager) {
	reg := monitoring.NoopRegisterer
	if appState.Metrics != nil {
		reg = appState.Metrics.Registerer
	}
	gate := mcpGate(appState.ServerConfig.Config.MCP.Enabled.Get, mcp.NewMCPServer(appState, objectsManager, reg).Handler())
	respond := func(r *http.Request) middleware.Responder {
		return middleware.ResponderFunc(func(w http.ResponseWriter, _ runtime.Producer) {
			gate.ServeHTTP(w, r)
		})
	}

	api.McpMcpPostHandler = mcpops.McpPostHandlerFunc(
		func(params mcpops.McpPostParams, _ *models.Principal) middleware.Responder {
			return respond(params.HTTPRequest)
		},
	)
	api.McpMcpDeleteHandler = mcpops.McpDeleteHandlerFunc(
		func(params mcpops.McpDeleteParams, _ *models.Principal) middleware.Responder {
			return respond(params.HTTPRequest)
		},
	)
}

const mcpDisabledBody = `{"error":"MCP server is disabled. Enable it in your cluster configuration: set MCP_SERVER_ENABLED=true or set the mcp_server_enabled runtime override. See https://docs.weaviate.io/weaviate/configuration/mcp-server"}`

// mcpGate answers 503 while MCP is disabled at runtime, 400 for a protocol
// version this server does not speak, and otherwise hands the request to the
// MCP server.
func mcpGate(enabled func() bool, server http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !enabled() {
			writeMCPError(w, http.StatusServiceUnavailable, []byte(mcpDisabledBody))
			return
		}
		// mcp-go accepts any value here; the spec asks for 400 on an unsupported one.
		if v := r.Header.Get("MCP-Protocol-Version"); v != "" && !slices.Contains(mcplib.ValidProtocolVersions, v) {
			body, _ := json.Marshal(map[string]string{"error": "unsupported MCP protocol version: " + v})
			writeMCPError(w, http.StatusBadRequest, body)
			return
		}
		server.ServeHTTP(w, r)
	})
}

func writeMCPError(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
