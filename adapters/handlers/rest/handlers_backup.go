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
	entitySchema "github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	ubak "github.com/semi-technologies/weaviate/usecases/backup"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

func newSource(db *db.DB) ubak.SourceFactory {
	return &sourcer{db}
}

type sourcer struct {
	db *db.DB
}

func (sp *sourcer) SourceFactory(className string) ubak.Sourcer {
	if idx := sp.db.GetIndex(entitySchema.ClassName(className)); idx != nil {
		return idx
	}
	return nil
}

type backupHandlers struct {
	manager *ubak.Manager
}

func (s *backupHandlers) createBackup(params backups.BackupsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	// TODO: update s.manager.CreateBackup to receive list of classes
	meta, err := s.manager.CreateBackup(params.HTTPRequest.Context(), principal,
		params.Body.Include[0], params.StorageName, params.Body.ID)
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
	status, err := s.manager.CreateBackupStatus(params.HTTPRequest.Context(), principal,
		"", params.StorageName, params.ID)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return backups.NewBackupsCreateStatusForbidden().
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
	// TODO: update s.manager.RestoreBackup to receive list of classes
	meta, err := s.manager.RestoreBackup(params.HTTPRequest.Context(), principal,
		params.Body.Include[0], params.StorageName, params.ID)
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
	status, restoreError, path, err := s.manager.RestoreBackupStatus(
		params.HTTPRequest.Context(), principal, "", params.StorageName, params.ID)
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

	return backups.NewBackupsRestoreStatusOK().
		WithPayload(&models.BackupRestoreMeta{
			Status: &status,
			// TODO: fetch the target classes internally
			//       and pass them back up here to set
			Classes:     []string{},
			Error:       restoreError,
			ID:          params.ID,
			Path:        path,
			StorageName: params.StorageName,
		})
}

func setupBackupHandlers(api *operations.WeaviateAPI, schemaManger *schemaUC.Manager, repo *db.DB, appState *state.State) {
	shardingStateFunc := func(className string) *sharding.State {
		return appState.SchemaManager.ShardingState(className)
	}
	snapshotterProvider := newSource(repo)
	backupManager := ubak.NewManager(appState.Logger, appState.Authorizer,
		schemaManger, snapshotterProvider,
		appState.Modules, shardingStateFunc)

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
