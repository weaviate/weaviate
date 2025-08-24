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
	"errors"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/backups"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type backupHandlers struct {
	manager             *ubak.Scheduler
	metricRequestsTotal restApiRequestsTotal
	logger              logrus.FieldLogger
}

// compressionFromBCfg transforms model backup config to a backup compression config
func compressionFromBCfg(cfg *models.BackupConfig) ubak.Compression {
	if cfg != nil {
		if cfg.CPUPercentage == 0 {
			cfg.CPUPercentage = ubak.DefaultCPUPercentage
		}

		if cfg.ChunkSize == 0 {
			cfg.ChunkSize = ubak.DefaultChunkSize
		}

		if cfg.CompressionLevel == "" {
			cfg.CompressionLevel = models.BackupConfigCompressionLevelDefaultCompression
		}

		return ubak.Compression{
			CPUPercentage: int(cfg.CPUPercentage),
			ChunkSize:     int(cfg.ChunkSize),
			Level:         parseCompressionLevel(cfg.CompressionLevel),
		}
	}

	return ubak.Compression{
		Level:         ubak.DefaultCompression,
		CPUPercentage: ubak.DefaultCPUPercentage,
		ChunkSize:     ubak.DefaultChunkSize,
	}
}

func compressionFromRCfg(cfg *models.RestoreConfig) ubak.Compression {
	if cfg != nil {
		if cfg.CPUPercentage == 0 {
			cfg.CPUPercentage = ubak.DefaultCPUPercentage
		}

		return ubak.Compression{
			CPUPercentage: int(cfg.CPUPercentage),
			Level:         ubak.DefaultCompression,
			ChunkSize:     ubak.DefaultChunkSize,
		}
	}

	return ubak.Compression{
		Level:         ubak.DefaultCompression,
		CPUPercentage: ubak.DefaultCPUPercentage,
		ChunkSize:     ubak.DefaultChunkSize,
	}
}

func parseCompressionLevel(l string) ubak.CompressionLevel {
	switch l {
	case models.BackupConfigCompressionLevelBestSpeed:
		return ubak.BestSpeed
	case models.BackupConfigCompressionLevelBestCompression:
		return ubak.BestCompression
	default:
		return ubak.DefaultCompression
	}
}

func (s *backupHandlers) createBackup(params backups.BackupsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	overrideBucket := ""
	overridePath := ""
	if params.Body.Config != nil {
		overrideBucket = params.Body.Config.Bucket
		overridePath = params.Body.Config.Path
	}
	meta, err := s.manager.Backup(params.HTTPRequest.Context(), principal, &ubak.BackupRequest{
		ID:          params.Body.ID,
		Backend:     params.Backend,
		Bucket:      overrideBucket,
		Path:        overridePath,
		Include:     params.Body.Include,
		Exclude:     params.Body.Exclude,
		Compression: compressionFromBCfg(params.Body.Config),
	})
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return backups.NewBackupsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.As(err, &backup.ErrUnprocessable{}):
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
	overrideBucket := ""
	if params.Bucket != nil {
		overrideBucket = *params.Bucket
	}
	overridePath := ""
	if params.Path != nil {
		overridePath = *params.Path
	}
	status, err := s.manager.BackupStatus(params.HTTPRequest.Context(), principal, params.Backend, params.ID, overrideBucket, overridePath)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return backups.NewBackupsCreateStatusForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.As(err, &backup.ErrUnprocessable{}):
			return backups.NewBackupsCreateStatusUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.As(err, &backup.ErrNotFound{}):
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
	bucket := ""
	path := ""
	roleOption := models.RestoreConfigRolesOptionsNoRestore
	userOption := models.RestoreConfigUsersOptionsNoRestore
	if params.Body.Config != nil {
		bucket = params.Body.Config.Bucket
		path = params.Body.Config.Path
		if params.Body.Config.RolesOptions != nil {
			roleOption = *params.Body.Config.RolesOptions
		}
		if params.Body.Config.UsersOptions != nil {
			userOption = *params.Body.Config.UsersOptions
		}
	}
	meta, err := s.manager.Restore(params.HTTPRequest.Context(), principal, &ubak.BackupRequest{
		ID:                params.ID,
		Backend:           params.Backend,
		Include:           params.Body.Include,
		Exclude:           params.Body.Exclude,
		NodeMapping:       params.Body.NodeMapping,
		Compression:       compressionFromRCfg(params.Body.Config),
		Bucket:            bucket,
		Path:              path,
		RbacRestoreOption: roleOption,
		UserRestoreOption: userOption,
	})
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		s.logger.WithError(err).WithField("id", params.ID).
			WithField("backend", params.Backend).
			WithField("bucket", bucket).WithField("path", path).
			Warn("failed to restore backup")
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return backups.NewBackupsRestoreForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.As(err, &backup.ErrNotFound{}):
			return backups.NewBackupsRestoreNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.As(err, &backup.ErrUnprocessable{}):
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
	var overrideBucket string
	if params.Bucket != nil {
		overrideBucket = *params.Bucket
	}
	var overridePath string
	if params.Path != nil {
		overridePath = *params.Path
	}
	status, err := s.manager.RestorationStatus(
		params.HTTPRequest.Context(), principal, params.Backend, params.ID, overrideBucket, overridePath)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return backups.NewBackupsRestoreForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.As(err, &backup.ErrNotFound{}):
			return backups.NewBackupsRestoreNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.As(err, &backup.ErrUnprocessable{}):
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

func (s *backupHandlers) cancel(params backups.BackupsCancelParams,
	principal *models.Principal,
) middleware.Responder {
	overrideBucket := ""
	if params.Bucket != nil {
		overrideBucket = *params.Bucket
	}
	overridePath := ""
	if params.Path != nil {
		overridePath = *params.Path
	}
	err := s.manager.Cancel(params.HTTPRequest.Context(), principal, params.Backend, params.ID, overrideBucket, overridePath)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return backups.NewBackupsCancelForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case errors.As(err, &backup.ErrUnprocessable{}):
			return backups.NewBackupsCancelUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return backups.NewBackupsCancelInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk("")
	return backups.NewBackupsCancelNoContent()
}

func (s *backupHandlers) list(params backups.BackupsListParams,
	principal *models.Principal,
) middleware.Responder {
	payload, err := s.manager.List(
		params.HTTPRequest.Context(), principal, params.Backend)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return backups.NewBackupsRestoreForbidden().
				WithPayload(errPayloadFromSingleErr(err))

		case errors.As(err, &backup.ErrUnprocessable{}):
			return backups.NewBackupsRestoreUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return backups.NewBackupsRestoreInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk("")
	return backups.NewBackupsListOK().WithPayload(*payload)
}

func setupBackupHandlers(api *operations.WeaviateAPI,
	scheduler *ubak.Scheduler, metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger,
) {
	h := &backupHandlers{scheduler, newBackupRequestsTotal(metrics, logger), logger}
	api.BackupsBackupsCreateHandler = backups.
		BackupsCreateHandlerFunc(h.createBackup)
	api.BackupsBackupsCreateStatusHandler = backups.
		BackupsCreateStatusHandlerFunc(h.createBackupStatus)
	api.BackupsBackupsRestoreHandler = backups.
		BackupsRestoreHandlerFunc(h.restoreBackup)
	api.BackupsBackupsRestoreStatusHandler = backups.
		BackupsRestoreStatusHandlerFunc(h.restoreBackupStatus)
	api.BackupsBackupsCancelHandler = backups.BackupsCancelHandlerFunc(h.cancel)
	api.BackupsBackupsListHandler = backups.BackupsListHandlerFunc(h.list)
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
	switch {
	case errors.As(err, &authzerrors.Forbidden{}):
		e.logUserError(className)
	case errors.As(err, &backup.ErrUnprocessable{}) || errors.As(err, &backup.ErrNotFound{}):
		e.logUserError(className)
	default:
		e.logServerError(className, err)
	}
}
