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
	"fmt"

	"github.com/mark3labs/mcp-go/server"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/internal"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/objects"
)

type MCPServer struct {
	server   *server.MCPServer
	creator  *create.WeaviateCreator
	searcher *search.WeaviateSearcher
	reader   *read.WeaviateReader
	state    *state.State
	logger   logrus.FieldLogger
}

func NewMCPServer(state *state.State, objectsManager *objects.Manager) (*MCPServer, error) {
	authHandler := auth.NewAuth(state)
	logger := state.Logger.WithField("component", "mcp")
	s := &MCPServer{
		server: server.NewMCPServer(
			"Weaviate MCP Server",
			"0.1.0",
			server.WithToolCapabilities(true),
			server.WithResourceCapabilities(false, false),
			server.WithRecovery(),
		),
		creator:  create.NewWeaviateCreator(authHandler, state.BatchManager, logger),
		searcher: search.NewWeaviateSearcher(authHandler, state.Traverser, logger),
		reader:   read.NewWeaviateReader(authHandler, state.SchemaManager, objectsManager, logger),
		state:    state,
		logger:   logger,
	}
	s.registerTools()
	return s, nil
}

func (s *MCPServer) Serve() {
	httpServer := server.NewStreamableHTTPServer(s.server)
	addr := fmt.Sprintf("0.0.0.0:%d", s.state.ServerConfig.Config.MCP.Port)
	s.logger.WithField("addr", addr).Info("starting MCP server")
	if err := httpServer.Start(addr); err != nil {
		s.logger.WithError(err).Error("MCP server stopped")
	}
}

func (s *MCPServer) registerTools() {
	// Load configuration for custom tool descriptions
	configPath := s.state.ServerConfig.Config.MCP.ConfigPath
	config := internal.LoadConfig(s.state.Logger, configPath)
	descriptions := config.ToDescriptionMap()

	s.server.AddTools(search.Tools(s.searcher, descriptions)...)
	s.server.AddTools(read.Tools(s.reader, descriptions)...)

	// Write access is disabled by default. It is enabled only when
	// MCP_SERVER_WRITE_ACCESS_DISABLED is set to "false" (case-insensitive).
	if !s.state.ServerConfig.Config.MCP.WriteAccessDisabled {
		s.server.AddTools(create.Tools(s.creator, descriptions)...)
	}
}
