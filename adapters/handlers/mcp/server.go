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
			state.ServerConfig.Config.Namespaces.Enabled,
			state.Logger,
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
			serverOptions(m, writeAccessEnabled)...,
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

// serverOptions is the option set the production MCP server is built with.
// Kept as a function so tests can pin the exact production wiring — notably
// that tool arguments are validated against the advertised input schemas.
func serverOptions(m *metrics.MCPMetrics, writeAccessEnabled func() bool) []server.ServerOption {
	return []server.ServerOption{
		server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, false),
		server.WithRecovery(),
		server.WithInputSchemaValidation(),
		server.WithHooks(listMetricsHooks(m, writeAccessEnabled)),
	}
}

func (s *MCPServer) Handler() http.Handler {
	// Every request is authenticated on its own and nothing is kept per session,
	// so session ids are neither issued nor required.
	return server.NewStreamableHTTPServer(s.server, server.WithStateLess(true))
}

// listMetricsHooks counts tools/list requests. The count lives in a hook, not
// in the tool filter, because the filter also runs for every tools/call.
func listMetricsHooks(m *metrics.MCPMetrics, writeAccessEnabled func() bool) *server.Hooks {
	hooks := &server.Hooks{}
	hooks.AddAfterListTools(func(context.Context, any, *mcplib.ListToolsRequest, *mcplib.ListToolsResult) {
		m.ObserveListed(writeAccessEnabled())
	})
	return hooks
}

// registerToolFilter hides write tools from tools/list when write access is
// disabled at runtime; read tools are always visible. mcp-go also runs the
// filter on every tools/call, with only the called tool, so calls must pass
// through — the tool handlers reject disabled writes themselves.
func (s *MCPServer) registerToolFilter() {
	server.WithToolFilter(func(ctx context.Context, tools []mcplib.Tool) []mcplib.Tool {
		// A single write tool is a tools/call, never a listing. Removing it here
		// would answer "tool not found"; passing it through lets the handler
		// answer with the write-disabled hint.
		if len(tools) == 1 && s.writeToolNames[tools[0].Name] {
			return tools
		}
		if s.creator.IsWriteAccessEnabled() {
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
