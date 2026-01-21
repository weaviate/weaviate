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
	"os"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/server"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/internal"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/objects"
)

type MCPServer struct {
	server            *server.MCPServer
	defaultCollection string
	creator           *create.WeaviateCreator
	searcher          *search.WeaviateSearcher
	reader            *read.WeaviateReader
	state             *state.State
}

func NewMCPServer(state *state.State, objectsManager *objects.Manager) (*MCPServer, error) {
	authHandler := auth.NewAuth(state)
	s := &MCPServer{
		server: server.NewMCPServer(
			"Weaviate MCP Server",
			"0.1.0",
			server.WithToolCapabilities(true),
			server.WithPromptCapabilities(true),
			server.WithResourceCapabilities(false, false),
			server.WithRecovery(),
		),
		// TODO: configurable collection name
		defaultCollection: "DefaultCollection",
		creator:           create.NewWeaviateCreator(authHandler, state.BatchManager),
		searcher:          search.NewWeaviateSearcher(authHandler, state.Traverser),
		reader:            read.NewWeaviateReader(authHandler, state.SchemaManager),
		state:             state,
	}
	s.registerTools()
	return s, nil
}

func (s *MCPServer) Serve() {
	sse := server.NewStreamableHTTPServer(s.server)
	if err := sse.Start("0.0.0.0:9000"); err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := sse.Shutdown(ctx); err != nil {
			panic(err)
		}
	}
}

func (s *MCPServer) registerTools() {
	// Load configuration for custom tool descriptions
	config := internal.LoadConfig(s.state.Logger)
	descriptions := config.ToDescriptionMap()

	s.server.AddTools(search.Tools(s.searcher)...)
	s.server.AddTools(read.Tools(s.reader)...)

	// Write access is disabled by default. It is enabled only when
	// MCP_SERVER_WRITE_ACCESS_DISABLED is set to "false" (case-insensitive).
	writeDisabled := os.Getenv("MCP_SERVER_WRITE_ACCESS_DISABLED")
	if strings.ToLower(writeDisabled) == "false" {
		s.server.AddTools(create.Tools(s.creator, descriptions)...)
	}
}
