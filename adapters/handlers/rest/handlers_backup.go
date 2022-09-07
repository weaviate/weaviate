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
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/backups"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/adapters/repos/db"
	"github.com/semi-technologies/weaviate/entities/backup"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	ubak "github.com/semi-technologies/weaviate/usecases/backup"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
)

type backupHandlers struct {
	manager *ubak.Manager
}

func (s *backupHandlers) createBackup(params backups.BackupsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	req := ubak.BackupRequest{
		ID:      params.Body.ID,
		Backend: params.Backend,
		Include: params.Body.Include,
		Exclude: params.Body.Exclude,
	}
	meta, err := s.manager.Backup(params.HTTPRequest.Context(), principal, &req)
	if err != nil {
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

	return backups.NewBackupsCreateOK().WithPayload(meta)
}

func (s *backupHandlers) createBackupStatus(params backups.BackupsCreateStatusParams,
	principal *models.Principal,
) middleware.Responder {
	// TODO: update s.manager.CreateBackupStatus to fetch the target classes internally
	// Maybe not since classes are returned when restore is call the first time
	// Also this would result in keeping this data in memory over the app life time
	// I suggest to remove it from both restore and backup responses
	status, err := s.manager.BackupStatus(params.HTTPRequest.Context(), principal, params.Backend, params.ID)
	if err != nil {
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
	return backups.NewBackupsCreateStatusOK().WithPayload(status)
}

func (s *backupHandlers) restoreBackup(params backups.BackupsRestoreParams,
	principal *models.Principal,
) middleware.Responder {
	req := ubak.BackupRequest{
		ID:      params.ID,
		Backend: params.Backend,
		Include: params.Body.Include,
		Exclude: params.Body.Exclude,
	}
	meta, err := s.manager.Restore(params.HTTPRequest.Context(), principal, &req)
	if err != nil {
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

	return backups.NewBackupsRestoreOK().WithPayload(meta)
}

func (s *backupHandlers) restoreBackupStatus(params backups.BackupsRestoreStatusParams,
	principal *models.Principal,
) middleware.Responder {
	// TODO: update s.manager.RestoreBackupStatus to fetch the target classes internally
	// Maybe not since classes are returned when restore is call the first time
	// Also this would result in keeping this data in memory over the app life time
	// I suggest to remove it from both restore and backup responses
	status, err := s.manager.RestorationStatus(
		params.HTTPRequest.Context(), principal, params.Backend, params.ID)
	if err != nil {
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
	sstatus := string(status.Status)
	payload := models.BackupRestoreStatusResponse{
		Status:  &sstatus,
		ID:      params.ID,
		Path:    status.Path,
		Backend: params.Backend,
	}
	if status.Err != nil {
		payload.Error = status.Err.Error()
	}
	return backups.NewBackupsRestoreStatusOK().WithPayload(&payload)
}

func setupBackupHandlers(api *operations.WeaviateAPI,
	schemaManger *schemaUC.Manager, repo *db.DB, appState *state.State,
) {
	backupManager := ubak.NewManager(appState.Logger, appState.Authorizer,
		schemaManger, repo, appState.Modules)

	h := &backupHandlers{backupManager}
	api.BackupsBackupsCreateHandler = backups.
		BackupsCreateHandlerFunc(h.createBackup)
	api.BackupsBackupsCreateStatusHandler = backups.
		BackupsCreateStatusHandlerFunc(h.createBackupStatus)
	api.BackupsBackupsRestoreHandler = backups.
		BackupsRestoreHandlerFunc(h.restoreBackup)
	api.BackupsBackupsRestoreStatusHandler = backups.
		BackupsRestoreStatusHandlerFunc(h.restoreBackupStatus)
}
