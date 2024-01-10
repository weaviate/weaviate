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
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/backups"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type backupHandlers struct {
	manager             *ubak.Scheduler
	metricRequestsTotal restApiRequestsTotal
}

// compressionFromCfg transforms model backup config to a backup compression config
func compressionFromCfg(cfg *models.BackupConfig) ubak.Compression {
	if cfg != nil {
		return ubak.Compression{
			CPUPercentage: int(cfg.CPUPercentage),
			ChunkSize:     int(cfg.ChunkSize),
			Level:         parseCompressionLevel(*cfg.CompressionLevel),
		}
	}

	return ubak.Compression{
		Level:         ubak.DefaultCompression,
		CPUPercentage: ubak.DefaultCPUPercentage,
		ChunkSize:     ubak.DefaultChunkSize,
	}
}

func parseCompressionLevel(l string) ubak.CompressionLevel {
	switch {
	case l == models.BackupConfigCompressionLevelBestSpeed:
		return ubak.BestSpeed
	case l == models.BackupConfigCompressionLevelBestCompression:
		return ubak.BestCompression
	default:
		return ubak.DefaultCompression
	}
}

func (s *backupHandlers) createBackup(params backups.BackupsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	meta, err := s.manager.Backup(params.HTTPRequest.Context(), principal, &ubak.BackupRequest{
		ID:          params.Body.ID,
		Backend:     params.Backend,
		Include:     params.Body.Include,
		Exclude:     params.Body.Exclude,
		Compression: compressionFromCfg(params.Body.Config),
	})
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch err.(type) {
		case errors.Forbidden:
			return backups.NewBackupsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case backup.ErrUnprocessable:
			return backups.NewBackupsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return backups.NewBackupsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk("")
	return backups.NewBackupsCreateOK().WithPayload(meta)
}

func (s *backupHandlers) createBackupStatus(params backups.BackupsCreateStatusParams,
	principal *models.Principal,
) middleware.Responder {
	status, err := s.manager.BackupStatus(params.HTTPRequest.Context(), principal, params.Backend, params.ID)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch err.(type) {
		case errors.Forbidden:
			return backups.NewBackupsCreateStatusForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case backup.ErrUnprocessable:
			return backups.NewBackupsCreateStatusUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		case backup.ErrNotFound:
			return backups.NewBackupsCreateStatusNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return backups.NewBackupsCreateStatusInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	strStatus := string(status.Status)
	payload := models.BackupCreateStatusResponse{
		Status:  &strStatus,
		ID:      params.ID,
		Path:    status.Path,
		Backend: params.Backend,
		Error:   status.Err,
	}
	s.metricRequestsTotal.logOk("")
	return backups.NewBackupsCreateStatusOK().WithPayload(&payload)
}

func (s *backupHandlers) restoreBackup(params backups.BackupsRestoreParams,
	principal *models.Principal,
) middleware.Responder {
	meta, err := s.manager.Restore(params.HTTPRequest.Context(), principal, &ubak.BackupRequest{
		ID:          params.ID,
		Backend:     params.Backend,
		Include:     params.Body.Include,
		Exclude:     params.Body.Exclude,
		NodeMapping: params.Body.NodeMapping,
		Compression: compressionFromCfg(params.Body.Config),
	})
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch err.(type) {
		case errors.Forbidden:
			return backups.NewBackupsRestoreForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case backup.ErrNotFound:
			return backups.NewBackupsRestoreNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		case backup.ErrUnprocessable:
			return backups.NewBackupsRestoreUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return backups.NewBackupsRestoreInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk("")
	return backups.NewBackupsRestoreOK().WithPayload(meta)
}

func (s *backupHandlers) restoreBackupStatus(params backups.BackupsRestoreStatusParams,
	principal *models.Principal,
) middleware.Responder {
	status, err := s.manager.RestorationStatus(
		params.HTTPRequest.Context(), principal, params.Backend, params.ID)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch err.(type) {
		case errors.Forbidden:
			return backups.NewBackupsRestoreForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case backup.ErrNotFound:
			return backups.NewBackupsRestoreNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		case backup.ErrUnprocessable:
			return backups.NewBackupsRestoreUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return backups.NewBackupsRestoreInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}
	strStatus := string(status.Status)
	payload := models.BackupRestoreStatusResponse{
		Status:  &strStatus,
		ID:      params.ID,
		Path:    status.Path,
		Backend: params.Backend,
		Error:   status.Err,
	}
	s.metricRequestsTotal.logOk("")
	return backups.NewBackupsRestoreStatusOK().WithPayload(&payload)
}

func setupBackupHandlers(api *operations.WeaviateAPI,
	scheduler *ubak.Scheduler, metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger,
) {
	h := &backupHandlers{scheduler, newBackupRequestsTotal(metrics, logger)}
	api.BackupsBackupsCreateHandler = backups.
		BackupsCreateHandlerFunc(h.createBackup)
	api.BackupsBackupsCreateStatusHandler = backups.
		BackupsCreateStatusHandlerFunc(h.createBackupStatus)
	api.BackupsBackupsRestoreHandler = backups.
		BackupsRestoreHandlerFunc(h.restoreBackup)
	api.BackupsBackupsRestoreStatusHandler = backups.
		BackupsRestoreStatusHandlerFunc(h.restoreBackupStatus)
}

type backupRequestsTotal struct {
	*restApiRequestsTotalImpl
}

func newBackupRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) restApiRequestsTotal {
	return &backupRequestsTotal{
		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{newRequestsTotalMetric(metrics, "rest"), "rest", "backup", logger},
	}
}

func (e *backupRequestsTotal) logError(className string, err error) {
	switch err.(type) {
	case errors.Forbidden:
		e.logUserError(className)
	case backup.ErrUnprocessable, backup.ErrNotFound:
		e.logUserError(className)
	default:
		e.logServerError(className, err)
	}
}
