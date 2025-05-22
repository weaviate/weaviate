//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package mcp

import (
	"context"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/create"
	"github.com/weaviate/weaviate/adapters/handlers/mcp/query"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"google.golang.org/grpc/metadata"
)

type MCPServer struct {
	server               *server.MCPServer
	defaultCollection    string
	allowAnonymousAccess bool
	authComposer         composer.TokenFunc
	creator              *create.WeaviateCreator
	querier              *query.WeaviateQuerier
}

func NewMCPServer(state *state.State) (*MCPServer, error) {
	s := &MCPServer{
		server: server.NewMCPServer(
			"Weaviate MCP Server",
			"0.1.0",
			server.WithToolCapabilities(true),
			server.WithPromptCapabilities(true),
			server.WithResourceCapabilities(true, true),
			server.WithRecovery(),
		),
		// TODO: configurable collection name
		defaultCollection:    "DefaultCollection",
		allowAnonymousAccess: state.ServerConfig.Config.Authentication.AnonymousAccess.Enabled,
		authComposer: composer.New(
			state.ServerConfig.Config.Authentication,
			state.APIKey,
			state.OIDC,
		),
		// creator: create.NewWeaviateCreator(state),
	}
	s.registerTools()
	return s, nil
}

func (s *MCPServer) Serve() {
	sse := server.NewSSEServer(s.server)
	if err := sse.Start("localhost:9000"); err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := sse.Shutdown(ctx); err != nil {
			panic(err)
		}
	}
}

func (s *MCPServer) registerTools() {
	insertOne := mcp.NewTool(
		"weaviate-insert-one",
		mcp.WithString(
			"collection",
			mcp.Description("Name of the target collection"),
		),
		mcp.WithObject(
			"properties",
			mcp.Description("Object properties to insert"),
			mcp.Required(),
		),
	)
	query := mcp.NewTool(
		"weaviate-query",
		mcp.WithString(
			"query",
			mcp.Description("Query data within Weaviate"),
			mcp.Required(),
		),
		mcp.WithArray(
			"targetProperties",
			mcp.Description("Properties to return with the query"),
			mcp.Required(),
		),
	)

	s.server.AddTools(
		server.ServerTool{Tool: insertOne, Handler: s.insertOne},
		server.ServerTool{Tool: query, Handler: s.query},
	)
}

func (s *MCPServer) insertOne(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	principal, err := s.principalFromContext(ctx)
	if err != nil {
		return mcp.NewToolResultErrorFromErr("failed to get principal", err), nil
	}
	return s.creator.InsertOne(ctx, principal, req)
}

func (s *MCPServer) query(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	principal, err := s.principalFromContext(ctx)
	if err != nil {
		return mcp.NewToolResultErrorFromErr("failed to get principal", err), nil
	}
	return s.querier.Hybrid(ctx, principal, req)
}

// This should probably be run as part of a middleware. In the initial gRPC
// implementation there is only a single endpoint, so it's fine to run this
// straight from the endpoint. But the moment we add a second endpoint, this
// should be called from a central place. This way we can make sure it's
// impossible to forget to add it to a new endpoint.
func (s *MCPServer) principalFromContext(ctx context.Context) (*models.Principal, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return s.tryAnonymous()
	}

	// the grpc library will lowercase all md keys, so we need to make sure to
	// check a lowercase key
	authValue, ok := md["authorization"]
	if !ok {
		return s.tryAnonymous()
	}

	if len(authValue) == 0 {
		return s.tryAnonymous()
	}

	if !strings.HasPrefix(authValue[0], "Bearer ") {
		return s.tryAnonymous()
	}

	token := strings.TrimPrefix(authValue[0], "Bearer ")
	return s.authComposer(token, nil)
}

func (s *MCPServer) tryAnonymous() (*models.Principal, error) {
	if s.allowAnonymousAccess {
		return nil, nil
	}

	return s.authComposer("", nil)
}
