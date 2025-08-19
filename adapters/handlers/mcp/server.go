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

package mcp

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/server"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
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
		creator:           create.NewWeaviateCreator(authHandler, objectsManager),
		searcher:          search.NewWeaviateSearcher(authHandler, state.Traverser),
		reader:            read.NewWeaviateReader(authHandler, state.SchemaManager),
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
	s.server.AddTools(create.Tools(s.creator)...)
	s.server.AddTools(search.Tools(s.searcher)...)
	s.server.AddTools(read.Tools(s.reader)...)
}
