//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helper

import (
	"testing"

	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/models"
)

func CreateBackup(t *testing.T, className, backend, backupID string) (*backups.BackupsCreateOK, error) {
	params := backups.NewBackupsCreateParams().
		WithBackend(backend).
		WithBody(&models.BackupCreateRequest{
			ID:      backupID,
			Include: []string{className},
		})
	return Client(t).Backups.BackupsCreate(params, nil)
}

func CreateBackupStatus(t *testing.T, backend, backupID string) (*backups.BackupsCreateStatusOK, error) {
	params := backups.NewBackupsCreateStatusParams().
		WithBackend(backend).
		WithID(backupID)
	return Client(t).Backups.BackupsCreateStatus(params, nil)
}

func RestoreBackup(t *testing.T, className, backend, backupID string, nodeMapping map[string]string) (*backups.BackupsRestoreOK, error) {
	params := backups.NewBackupsRestoreParams().
		WithBackend(backend).
		WithID(backupID).
		WithBody(&models.BackupRestoreRequest{
			Include:     []string{className},
			NodeMapping: nodeMapping,
		})
	return Client(t).Backups.BackupsRestore(params, nil)
}

func RestoreBackupStatus(t *testing.T, backend, backupID string) (*backups.BackupsRestoreStatusOK, error) {
	params := backups.NewBackupsRestoreStatusParams().
		WithBackend(backend).
		WithID(backupID)
	return Client(t).Backups.BackupsRestoreStatus(params, nil)
}
