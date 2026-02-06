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
	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/exports"
	"github.com/weaviate/weaviate/entities/models"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/export"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type exportHandlers struct {
	scheduler           *export.Scheduler
	metricRequestsTotal restApiRequestsTotal
	logger              logrus.FieldLogger
}

// createExport handles POST /v1/export/{backend}
func (h *exportHandlers) createExport(params exports.ExportsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	// Extract parameters
	id := *params.Body.ID
	backend := params.Backend
	include := params.Body.Include
	exclude := params.Body.Exclude

	// Extract bucket and path from config
	bucket := ""
	path := ""
	if params.Body.Config != nil {
		bucket = params.Body.Config.Bucket
		path = params.Body.Config.Path
	}

	// Start export
	status, err := h.scheduler.Export(params.HTTPRequest.Context(), principal, id, backend, include, exclude, bucket, path)
	if err != nil {
		h.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return exports.NewExportsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return exports.NewExportsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.metricRequestsTotal.logOk("")
	return exports.NewExportsCreateOK().WithPayload(convertToExportCreateResponse(status))
}

// exportStatus handles GET /v1/export/{backend}/{id}
func (h *exportHandlers) exportStatus(params exports.ExportsStatusParams,
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
			return exports.NewExportsStatusForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return exports.NewExportsStatusNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.metricRequestsTotal.logOk("")
	return exports.NewExportsStatusOK().WithPayload(convertToExportStatusResponse(status))
}

// convertToExportCreateResponse converts internal status to API response
func convertToExportCreateResponse(status *export.ExportStatus) *models.ExportCreateResponse {
	resp := &models.ExportCreateResponse{
		ID:      status.ID,
		Backend: status.Backend,
		Path:    status.Path,
		Status:  string(status.Status),
		Classes: status.Classes,
	}

	if !status.StartedAt.IsZero() {
		resp.StartedAt = strfmt.DateTime(status.StartedAt)
	}

	if status.Error != "" {
		resp.Error = status.Error
	}

	if status.Progress != nil {
		resp.Progress = convertProgress(status.Progress)
	}

	return resp
}

// convertToExportStatusResponse converts internal status to API status response
func convertToExportStatusResponse(status *export.ExportStatus) *models.ExportStatusResponse {
	resp := &models.ExportStatusResponse{
		ID:      status.ID,
		Backend: status.Backend,
		Path:    status.Path,
		Status:  string(status.Status),
		Classes: status.Classes,
	}

	if !status.StartedAt.IsZero() {
		resp.StartedAt = strfmt.DateTime(status.StartedAt)
	}

	if status.Error != "" {
		resp.Error = status.Error
	}

	if status.Progress != nil {
		resp.Progress = convertProgress(status.Progress)
	}

	return resp
}

// convertProgress converts internal progress map to API progress map
func convertProgress(progress map[string]*export.ClassProgress) map[string]models.ClassProgress {
	result := make(map[string]models.ClassProgress)
	for className, p := range progress {
		cp := models.ClassProgress{
			Status:          string(p.Status),
			ObjectsExported: p.ObjectsExported,
		}
		if p.FileSizeBytes > 0 {
			cp.FileSizeBytes = p.FileSizeBytes
		}
		if p.Error != "" {
			cp.Error = p.Error
		}
		result[className] = cp
	}
	return result
}

// setupExportHandlers wires up the export handlers to the API
func setupExportHandlers(api *operations.WeaviateAPI,
	scheduler *export.Scheduler,
	metrics *monitoring.PrometheusMetrics,
	logger logrus.FieldLogger,
) {
	h := &exportHandlers{
		scheduler:           scheduler,
		metricRequestsTotal: newExportRequestsTotal(metrics, logger),
		logger:              logger,
	}

	api.ExportsExportsCreateHandler = exports.ExportsCreateHandlerFunc(h.createExport)
	api.ExportsExportsStatusHandler = exports.ExportsStatusHandlerFunc(h.exportStatus)
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
	case errors.As(err, &authzerrors.Forbidden{}):
		e.logUserError(className)
	default:
		e.logServerError(className, err)
	}
}

func (e *exportRequestsTotal) logOk(className string) {
	e.logUserError(className)
}
