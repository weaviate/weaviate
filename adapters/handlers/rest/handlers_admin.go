package rest

import (
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/admin"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type adminHandlers struct {
	metricRequestsTotal restApiRequestsTotal
}

func (s *adminHandlers) invertedIndexRebuild(params admin.AdminInvertedIndexRebuildParams, principal *models.Principal) middleware.Responder {
	return admin.NewAdminInvertedIndexRebuildOK()
}

func setupAdminHandlers(api *operations.WeaviateAPI, metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) {
	h := &adminHandlers{
		metricRequestsTotal: newSchemaRequestsTotal(metrics, logger),
	}
	api.AdminAdminInvertedIndexRebuildHandler = admin.AdminInvertedIndexRebuildHandlerFunc(h.invertedIndexRebuild)
}
