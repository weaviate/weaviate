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

package rest

import (
	"fmt"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/admin"
	"github.com/weaviate/weaviate/entities/models"
	ucAdmin "github.com/weaviate/weaviate/usecases/admin"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type adminHandlers struct {
	adminManager        *ucAdmin.Manager
	metricRequestsTotal restApiRequestsTotal
	clusterState        *cluster.State
}

func (s *adminHandlers) invertedIndexRebuild(params admin.AdminInvertedIndexRebuildParams, principal *models.Principal) middleware.Responder {
	// TODO rbac
	for _, classProperties := range params.Body {
		className := classProperties.ClassName
		for _, propertyName := range classProperties.PropertyNames {
			fmt.Printf("  ==> className [%s], propertyName [%s]\n", className, propertyName)
			// TODO trigger reindexing here
			// err := s.adminManager.Migrator.InvertedReindex(params.HTTPRequest.Context(), "ShardInvertedReindexTask_BrokenIndex")
			var err error = nil
			if err != nil {
				// TODO log
				return admin.NewAdminInvertedIndexRebuildInternalServerError().
					WithPayload(errPayloadFromSingleErr(err))
			}
		}
	}
	return admin.NewAdminInvertedIndexRebuildOK()
}

func (s *adminHandlers) maintenanceModeGet(params admin.AdminConfigMaintenanceModeGetParams, principal *models.Principal) middleware.Responder {
	// TODO rbac
	responsePayload := []*models.NodeMaintenanceMode{}
	for _, node := range s.clusterState.GetNodesInMaintenanceMode() {
		responsePayload = append(responsePayload, &models.NodeMaintenanceMode{
			NodeHostname: node,
			Enabled:      true,
		})
	}
	return admin.NewAdminConfigMaintenanceModeGetOK().WithPayload(responsePayload)
}

func (s *adminHandlers) maintenanceModeSet(params admin.AdminConfigMaintenanceModeSetParams, principal *models.Principal) middleware.Responder {
	// TODO rbac
	responsePayload := []*models.NodeMaintenanceMode{}
	for _, nodeMaintenanceMode := range params.Body {
		if nodeMaintenanceMode.Enabled {
			s.clusterState.AddMaintenanceNode(nodeMaintenanceMode.NodeHostname)
		} else {
			s.clusterState.RemoveMaintenanceNode(nodeMaintenanceMode.NodeHostname)
		}
		responsePayload = append(responsePayload, nodeMaintenanceMode)
	}
	return admin.NewAdminConfigMaintenanceModeSetOK().WithPayload(responsePayload)
}

func setupAdminHandlers(api *operations.WeaviateAPI, metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger, clusterState *cluster.State, adminManager *ucAdmin.Manager) {
	h := &adminHandlers{
		adminManager:        adminManager,
		metricRequestsTotal: newSchemaRequestsTotal(metrics, logger),
		clusterState:        clusterState,
	}
	api.AdminAdminInvertedIndexRebuildHandler = admin.AdminInvertedIndexRebuildHandlerFunc(h.invertedIndexRebuild)
	api.AdminAdminConfigMaintenanceModeGetHandler = admin.AdminConfigMaintenanceModeGetHandlerFunc(h.maintenanceModeGet)
	api.AdminAdminConfigMaintenanceModeSetHandler = admin.AdminConfigMaintenanceModeSetHandlerFunc(h.maintenanceModeSet)
}
