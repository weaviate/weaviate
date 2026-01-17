//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"os"
	"strings"

	"github.com/weaviate/weaviate/adapters/handlers/mcp"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func startMcpServer(server *mcp.MCPServer, state *state.State) {
	// Check if MCP server is enabled via environment variable
	enabled := os.Getenv("MCP_SERVER_ENABLED")
	if strings.ToLower(enabled) != "true" {
		state.Logger.Info("MCP server is disabled (set MCP_SERVER_ENABLED=true to enable)")
		return
	}

	state.Logger.Info("Starting MCP server on port 9000")
	enterrors.GoWrapper(func() {
		server.Serve()
	}, state.Logger)
}
