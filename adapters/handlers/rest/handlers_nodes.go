//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package rest

import (
	"github.com/go-openapi/runtime/middleware"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/nodes"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	enterrors "github.com/semi-technologies/weaviate/entities/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	nodesUC "github.com/semi-technologies/weaviate/usecases/nodes"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
)

type nodesHandlers struct {
	manager *nodesUC.Manager
}

func (s *nodesHandlers) getNodesStatus(params nodes.NodesGetParams, principal *models.Principal) middleware.Responder {
	nodeStatuses, err := s.manager.GetNodeStatuses(params.HTTPRequest.Context(), principal)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return nodes.NewNodesGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case enterrors.ErrUnprocessable:
			return nodes.NewNodesGetUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		case enterrors.ErrNotFound:
			return nodes.NewNodesGetNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return nodes.NewNodesGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	status := &models.NodeStatusResponse{
		Nodes: nodeStatuses,
	}

	return nodes.NewNodesGetOK().WithPayload(status)
}

func setupNodesHandlers(api *operations.WeaviateAPI,
	schemaManger *schemaUC.Manager, repo *db.DB, appState *state.State,
) {
	nodesManager := nodesUC.NewManager(appState.Logger, appState.Authorizer,
		repo, schemaManger)

	h := &nodesHandlers{nodesManager}
	api.NodesNodesGetHandler = nodes.
		NodesGetHandlerFunc(h.getNodesStatus)
}
