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
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/export"
	"github.com/weaviate/weaviate/entities/models"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	ucexport "github.com/weaviate/weaviate/usecases/export"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type exportHandlers struct {
	scheduler           *ucexport.Scheduler
	metricRequestsTotal restApiRequestsTotal
	logger              logrus.FieldLogger
}

// createExport handles POST /v1/export/{backend}
func (h *exportHandlers) createExport(params export.ExportCreateParams,
	principal *models.Principal,
) middleware.Responder {
	if params.Body.ID == nil || *params.Body.ID == "" {
		return export.NewExportCreateUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(errors.New("export ID is required")))
	}

	id := *params.Body.ID
	backend := params.Backend
	include := params.Body.Include
	exclude := params.Body.Exclude

	bucket := ""
	path := ""
	if params.Body.Config != nil {
		bucket = params.Body.Config.Bucket
		path = params.Body.Config.Path
	}

	// Start export
	resp, err := h.scheduler.Export(params.HTTPRequest.Context(), principal, id, backend, include, exclude, bucket, path)
	if err != nil {
		h.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return export.NewExportCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.Is(err, ucexport.ErrExportValidation),
			errors.Is(err, ucexport.ErrExportAlreadyExists),
			errors.Is(err, ucexport.ErrExportAlreadyActive):
			return export.NewExportCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return export.NewExportCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.metricRequestsTotal.logOk("")
	return export.NewExportCreateOK().WithPayload(resp)
}

// exportStatus handles GET /v1/export/{backend}/{id}
func (h *exportHandlers) exportStatus(params export.ExportStatusParams,
	principal *models.Principal,
) middleware.Responder {
	// Extract optional bucket and path parameters
	bucket := ""
	if params.Bucket != nil {
		bucket = *params.Bucket
	}
	path := ""
	if params.Path != nil {
		path = *params.Path
	}

	status, err := h.scheduler.Status(params.HTTPRequest.Context(), principal,
		params.Backend, params.ID, bucket, path)
	if err != nil {
		h.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return export.NewExportStatusForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.Is(err, ucexport.ErrExportNotFound):
			return export.NewExportStatusNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.Is(err, ucexport.ErrExportValidation):
			return export.NewExportStatusUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return export.NewExportStatusInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.metricRequestsTotal.logOk("")
	return export.NewExportStatusOK().WithPayload(status)
}

// cancelExport handles DELETE /v1/export/{backend}/{id}
func (h *exportHandlers) cancelExport(params export.ExportCancelParams,
	principal *models.Principal,
) middleware.Responder {
	bucket := ""
	if params.Bucket != nil {
		bucket = *params.Bucket
	}
	path := ""
	if params.Path != nil {
		path = *params.Path
	}

	err := h.scheduler.Cancel(params.HTTPRequest.Context(), principal,
		params.Backend, params.ID, bucket, path)
	if err != nil {
		h.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return export.NewExportCancelForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.Is(err, ucexport.ErrExportNotFound):
			return export.NewExportCancelNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.Is(err, ucexport.ErrExportValidation):
			return export.NewExportCancelUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.Is(err, ucexport.ErrExportAlreadyFinished):
			return export.NewExportCancelConflict().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return export.NewExportCancelInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.metricRequestsTotal.logOk("")
	return export.NewExportCancelNoContent()
}

// setupExportHandlers wires up the export handlers to the API
func setupExportHandlers(api *operations.WeaviateAPI,
	scheduler *ucexport.Scheduler,
	metrics *monitoring.PrometheusMetrics,
	logger logrus.FieldLogger,
) {
	h := &exportHandlers{
		scheduler:           scheduler,
		metricRequestsTotal: newExportRequestsTotal(metrics, logger),
		logger:              logger,
	}

	api.ExportExportCreateHandler = export.ExportCreateHandlerFunc(h.createExport)
	api.ExportExportStatusHandler = export.ExportStatusHandlerFunc(h.exportStatus)
	api.ExportExportCancelHandler = export.ExportCancelHandlerFunc(h.cancelExport)
}

type exportRequestsTotal struct {
	*restApiRequestsTotalImpl
}

func newExportRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) restApiRequestsTotal {
	return &exportRequestsTotal{
		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{
			newRequestsTotalMetric(metrics, "rest"),
			"rest",
			"export",
			logger,
		},
	}
}

func (e *exportRequestsTotal) logError(className string, err error) {
	switch {
	case errors.As(err, &authzerrors.Forbidden{}),
		errors.Is(err, ucexport.ErrExportValidation),
		errors.Is(err, ucexport.ErrExportAlreadyExists),
		errors.Is(err, ucexport.ErrExportAlreadyActive):
		e.logUserError(className)
	default:
		e.logServerError(className, err)
	}
}

func (e *exportRequestsTotal) logOk(className string) {
	e.restApiRequestsTotalImpl.logOk(className)
}
