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
	"errors"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/cluster"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/nodes"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	nodesUC "github.com/weaviate/weaviate/usecases/nodes"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

type nodesHandlers struct {
	manager             *nodesUC.Manager
	schemaManager       namespacing.SchemaManager
	namespacesEnabled   bool
	metricRequestsTotal restApiRequestsTotal
}

func (n *nodesHandlers) getNodesStatus(params nodes.NodesGetParams, principal *models.Principal) middleware.Responder {
	output, err := verbosity.ParseOutput(params.Output)
	if err != nil {
		return nodes.NewNodesGetUnprocessableEntity().WithPayload(errPayloadFromSingleErr(principal, err))
	}

	nodeStatuses, err := n.manager.GetNodeStatus(params.HTTPRequest.Context(), principal, "", "", output)
	if err != nil {
		return n.handleGetNodesError(principal, err)
	}

	status := &models.NodesStatusResponse{
		Nodes: nodeStatuses,
	}
	// Shard.Class comes straight from i.Config.ClassName.String() (qualified
	// storage name); strip the caller's own prefix so namespaced callers
	// don't see "<ns>:" in their /v1/nodes response. No-op for global admins.
	namespacing.StripNodesStatusResponse(principal, status)

	n.metricRequestsTotal.logOk("")
	return nodes.NewNodesGetOK().WithPayload(status)
}

func (n *nodesHandlers) getNodesStatusByClass(params nodes.NodesGetClassParams, principal *models.Principal) middleware.Responder {
	output, err := verbosity.ParseOutput(params.Output)
	if err != nil {
		return nodes.NewNodesGetUnprocessableEntity().WithPayload(errPayloadFromSingleErr(principal, err))
	}

	shardName := ""
	if params.ShardName != nil {
		shardName = *params.ShardName
	}

	className, _, err := namespacing.Resolve(principal, n.schemaManager, n.namespacesEnabled, params.ClassName)
	if err != nil {
		return nodes.NewNodesGetUnprocessableEntity().WithPayload(errPayloadFromSingleErr(principal, err))
	}

	nodeStatuses, err := n.manager.GetNodeStatus(params.HTTPRequest.Context(), principal, className, shardName, output)
	if err != nil {
		return n.handleGetNodesError(principal, err)
	}

	status := &models.NodesStatusResponse{
		Nodes: nodeStatuses,
	}
	// Same reason as getNodesStatus — Shard.Class is qualified upstream.
	namespacing.StripNodesStatusResponse(principal, status)

	n.metricRequestsTotal.logOk("")
	return nodes.NewNodesGetOK().WithPayload(status)
}

func (n *nodesHandlers) getNodesStatistics(params cluster.ClusterGetStatisticsParams, principal *models.Principal) middleware.Responder {
	nodeStatistics, err := n.manager.GetNodeStatistics(params.HTTPRequest.Context(), principal)
	if err != nil {
		return n.handleGetNodesError(principal, err)
	}

	synchronized := map[string]struct{}{}
	for _, stats := range nodeStatistics {
		if stats.Status == nil || *stats.Status != models.StatisticsStatusHEALTHY {
			synchronized = nil
			break
		}
		if stats.Raft != nil {
			synchronized[stats.Raft.AppliedIndex] = struct{}{}
		}
	}

	statistics := &models.ClusterStatisticsResponse{
		Statistics:   nodeStatistics,
		Synchronized: len(synchronized) == 1,
	}

	n.metricRequestsTotal.logOk("")
	return cluster.NewClusterGetStatisticsOK().WithPayload(statistics)
}

func (n *nodesHandlers) handleGetNodesError(principal *models.Principal, err error) middleware.Responder {
	n.metricRequestsTotal.logError("", err)
	if errors.As(err, &enterrors.ErrNotFound{}) {
		return nodes.NewNodesGetClassNotFound().
			WithPayload(errPayloadFromSingleErr(principal, err))
	}
	if errors.As(err, &autherrs.Forbidden{}) {
		return nodes.NewNodesGetClassForbidden().
			WithPayload(errPayloadFromSingleErr(principal, err))
	}
	if errors.As(err, &enterrors.ErrUnprocessable{}) {
		return nodes.NewNodesGetClassUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(principal, err))
	}
	return nodes.NewNodesGetClassInternalServerError().
		WithPayload(errPayloadFromSingleErr(principal, err))
}

func setupNodesHandlers(api *operations.WeaviateAPI,
	schemaManger *schemaUC.Manager, repo *db.DB, appState *state.State,
) {
	nodesManager := nodesUC.NewManager(appState.Logger, appState.Authorizer,
		repo, schemaManger, appState.ServerConfig.Config.Authorization.Rbac, appState.ServerConfig.Config.MinimumInternalTimeout)

	h := &nodesHandlers{
		manager:             nodesManager,
		schemaManager:       schemaManger,
		namespacesEnabled:   appState.ServerConfig.Config.Namespaces.Enabled,
		metricRequestsTotal: newNodesRequestsTotal(appState.Metrics, appState.Logger),
	}
	api.NodesNodesGetHandler = nodes.
		NodesGetHandlerFunc(h.getNodesStatus)
	api.NodesNodesGetClassHandler = nodes.
		NodesGetClassHandlerFunc(h.getNodesStatusByClass)
	api.ClusterClusterGetStatisticsHandler = cluster.
		ClusterGetStatisticsHandlerFunc(h.getNodesStatistics)
}

type nodesRequestsTotal struct {
	*restApiRequestsTotalImpl
}

func newNodesRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) restApiRequestsTotal {
	return &nodesRequestsTotal{
		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{newRequestsTotalMetric(metrics, "rest"), "rest", "nodes", logger},
	}
}

func (e *nodesRequestsTotal) logError(className string, err error) {
	switch {
	case errors.As(err, &enterrors.ErrNotFound{}), errors.As(err, &enterrors.ErrUnprocessable{}):
		e.logUserError(className)
	case errors.As(err, &autherrs.Forbidden{}):
		e.logUserError(className)
	default:
		e.logServerError(className, err)
	}
}
