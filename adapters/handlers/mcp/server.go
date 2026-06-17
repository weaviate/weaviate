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
	"net/http"

	mcplib "github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/auth"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/internal"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/metrics"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/read"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/search"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/objects"
)

type MCPServer struct {
	server   *server.MCPServer
	creator  *create.WeaviateCreator
	searcher *search.WeaviateSearcher
	reader   *read.WeaviateReader
	state    *state.State
	logger   logrus.FieldLogger
	metrics  *metrics.MCPMetrics

	// writeToolNames is the set of tool names that require write access.
	// Used by the tool filter to hide them from tools/list when write access
	// is disabled at runtime.
	writeToolNames map[string]bool
}

func NewMCPServer(state *state.State, objectsManager *objects.Manager, reg prometheus.Registerer) *MCPServer {
	writeAccessEnabled := func() bool {
		return state.ServerConfig.Config.MCP.WriteAccessEnabled.Get()
	}

	m := metrics.New(reg, writeAccessEnabled)
	authHandler := auth.NewAuth(
		state.ServerConfig.Config.Authentication.AnonymousAccess.Enabled,
		composer.New(
			state.ServerConfig.Config.Authentication,
			state.APIKey,
			state.OIDC,
		),
		state.Authorizer,
		m,
	)
	logger := state.Logger.WithField("component", "mcp")

	s := &MCPServer{
		server: server.NewMCPServer(
			"Weaviate MCP Server",
			"0.1.0",
			server.WithToolCapabilities(true),
			server.WithResourceCapabilities(false, false),
			server.WithRecovery(),
		),
		creator:        create.NewWeaviateCreator(authHandler, state.BatchManager, logger, writeAccessEnabled),
		searcher:       search.NewWeaviateSearcher(authHandler, state.Traverser, state.SchemaManager, state.SchemaManager, state.ServerConfig.Config.Namespaces.Enabled, logger),
		reader:         read.NewWeaviateReader(authHandler, state.SchemaManager, state.SchemaManager, state.ServerConfig.Config.Namespaces.Enabled, objectsManager, logger),
		state:          state,
		logger:         logger,
		metrics:        m,
		writeToolNames: map[string]bool{},
	}
	s.registerTools()
	s.registerToolFilter()
	return s
}

func (s *MCPServer) Handler() http.Handler {
	return server.NewStreamableHTTPServer(s.server)
}

// registerToolFilter hides write tools from tools/list when write access is
// disabled at runtime. Read tools are always visible. The filter only affects
// listing — calls to disabled tools are also rejected by the tool handlers.
func (s *MCPServer) registerToolFilter() {
	server.WithToolFilter(func(ctx context.Context, tools []mcplib.Tool) []mcplib.Tool {
		writeEnabled := s.creator.IsWriteAccessEnabled()
		s.metrics.ObserveListed(writeEnabled)
		if writeEnabled {
			return tools
		}
		filtered := make([]mcplib.Tool, 0, len(tools))
		for _, tool := range tools {
			if !s.writeToolNames[tool.Name] {
				filtered = append(filtered, tool)
			}
		}
		return filtered
	})(s.server)
}

func (s *MCPServer) registerTools() {
	// Load configuration for custom tool descriptions
	configPath := s.state.ServerConfig.Config.MCP.ConfigPath
	config := internal.LoadConfig(s.state.Logger, configPath)
	configs := config.ToToolConfigMap()

	s.server.AddTools(search.Tools(s.searcher, configs, s.metrics)...)
	s.server.AddTools(read.Tools(s.reader, configs, s.metrics)...)

	// Always register write tools. Whether they are visible (in tools/list)
	// and callable is gated by the runtime-configurable
	// MCP_SERVER_WRITE_ACCESS_ENABLED flag — checked by registerToolFilter()
	// for listing, and by the tool handlers themselves for calls.
	writeTools := create.Tools(s.creator, configs, s.metrics)
	for _, t := range writeTools {
		s.writeToolNames[t.Tool.Name] = true
	}
	s.server.AddTools(writeTools...)
}
