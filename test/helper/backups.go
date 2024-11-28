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

package helper

import (
	"testing"

	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/backup"
)

func DefaultBackupConfig() *models.BackupConfig {
	return &models.BackupConfig{
		CompressionLevel: models.BackupConfigCompressionLevelDefaultCompression,
		CPUPercentage:    backup.DefaultCPUPercentage,
		ChunkSize:        128,
	}
}

func DefaultRestoreConfig() *models.RestoreConfig {
	return &models.RestoreConfig{
		CPUPercentage: backup.DefaultCPUPercentage,
	}
}

func CreateBackup(t *testing.T, cfg *models.BackupConfig, className, backend, backupID string) (*backups.BackupsCreateOK, error) {
	params := backups.NewBackupsCreateParams().
		WithBackend(backend).
		WithBody(&models.BackupCreateRequest{
			ID:      backupID,
			Include: []string{className},
			Config:  cfg,
		})
	return Client(t).Backups.BackupsCreate(params, nil)
}

func ListBackup(t *testing.T, className, backend string) (*backups.BackupsListOK, error) {
	params := backups.NewBackupsListParams().
		WithBackend(backend)
	return Client(t).Backups.BackupsList(params, nil)
}

func CancelBackup(t *testing.T, className, backend, backupID string) error {
	params := backups.NewBackupsCancelParams().
		WithBackend(backend).
		WithID(backupID)
	_, err := Client(t).Backups.BackupsCancel(params, nil)
	return err
}

func CreateBackupStatus(t *testing.T, backend, backupID string) (*backups.BackupsCreateStatusOK, error) {
	params := backups.NewBackupsCreateStatusParams().
		WithBackend(backend).
		WithID(backupID)
	return Client(t).Backups.BackupsCreateStatus(params, nil)
}

func RestoreBackup(t *testing.T, cfg *models.RestoreConfig, className, backend, backupID string, nodeMapping map[string]string) (*backups.BackupsRestoreOK, error) {
	params := backups.NewBackupsRestoreParams().
		WithBackend(backend).
		WithID(backupID).
		WithBody(&models.BackupRestoreRequest{
			Include:     []string{className},
			NodeMapping: nodeMapping,
			Config:      cfg,
		})
	return Client(t).Backups.BackupsRestore(params, nil)
}

func RestoreBackupStatus(t *testing.T, backend, backupID string) (*backups.BackupsRestoreStatusOK, error) {
	params := backups.NewBackupsRestoreStatusParams().
		WithBackend(backend).
		WithID(backupID)
	return Client(t).Backups.BackupsRestoreStatus(params, nil)
}
