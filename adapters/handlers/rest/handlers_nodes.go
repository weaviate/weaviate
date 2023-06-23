//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"errors"

	"github.com/go-openapi/runtime/middleware"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/nodes"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	nodesUC "github.com/weaviate/weaviate/usecases/nodes"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type nodesHandlers struct {
	manager *nodesUC.Manager
}

func (s *nodesHandlers) getNodesStatus(params nodes.NodesGetParams, principal *models.Principal) middleware.Responder {
	nodeStatuses, err := s.manager.GetNodeStatus(params.HTTPRequest.Context(), principal, "")
	if err != nil {
		return handleGetNodesError(err)
	}

	status := &models.NodesStatusResponse{
		Nodes: nodeStatuses,
	}

	return nodes.NewNodesGetOK().WithPayload(status)
}

func (s *nodesHandlers) getNodesStatusByClass(params nodes.NodesGetClassParams, principal *models.Principal) middleware.Responder {
	nodeStatuses, err := s.manager.GetNodeStatus(params.HTTPRequest.Context(), principal, params.ClassName)
	if err != nil {
		return handleGetNodesError(err)
	}

	status := &models.NodesStatusResponse{
		Nodes: nodeStatuses,
	}

	return nodes.NewNodesGetOK().WithPayload(status)
}

func handleGetNodesError(err error) middleware.Responder {
	if errors.As(err, &enterrors.ErrNotFound{}) {
		return nodes.NewNodesGetClassNotFound().
			WithPayload(errPayloadFromSingleErr(err))
	}
	if errors.As(err, &autherrs.Forbidden{}) {
		return nodes.NewNodesGetClassForbidden().
			WithPayload(errPayloadFromSingleErr(err))
	}
	if errors.As(err, &enterrors.ErrUnprocessable{}) {
		return nodes.NewNodesGetClassUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}
	return nodes.NewNodesGetClassInternalServerError().
		WithPayload(errPayloadFromSingleErr(err))
}

func setupNodesHandlers(api *operations.WeaviateAPI,
	schemaManger *schemaUC.Manager, repo *db.DB, appState *state.State,
) {
	nodesManager := nodesUC.NewManager(appState.Logger, appState.Authorizer,
		repo, schemaManger)

	h := &nodesHandlers{nodesManager}
	api.NodesNodesGetHandler = nodes.
		NodesGetHandlerFunc(h.getNodesStatus)
	api.NodesNodesGetClassHandler = nodes.
		NodesGetClassHandlerFunc(h.getNodesStatusByClass)
}
