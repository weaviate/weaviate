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
	"github.com/weaviate/weaviate/usecases/objects"
)

func setupMCPHandlers(api *operations.WeaviateAPI, appState *state.State, objectsManager *objects.Manager) {
	if !appState.ServerConfig.Config.MCP.Enabled {
		return
	}

	mcpServer := mcp.NewMCPServer(appState, objectsManager)
	mcpHandler := mcpServer.Handler()

	api.McpMcpPostHandler = mcpops.McpPostHandlerFunc(
		func(params mcpops.McpPostParams, _ *models.Principal) middleware.Responder {
			return middleware.ResponderFunc(func(w http.ResponseWriter, _ runtime.Producer) {
				mcpHandler.ServeHTTP(w, params.HTTPRequest)
			})
		},
	)
	api.McpMcpGetHandler = mcpops.McpGetHandlerFunc(
		func(params mcpops.McpGetParams, _ *models.Principal) middleware.Responder {
			return middleware.ResponderFunc(func(w http.ResponseWriter, _ runtime.Producer) {
				mcpHandler.ServeHTTP(w, params.HTTPRequest)
			})
		},
	)
	api.McpMcpDeleteHandler = mcpops.McpDeleteHandlerFunc(
		func(params mcpops.McpDeleteParams, _ *models.Principal) middleware.Responder {
			return middleware.ResponderFunc(func(w http.ResponseWriter, _ runtime.Producer) {
				mcpHandler.ServeHTTP(w, params.HTTPRequest)
			})
		},
	)
}
