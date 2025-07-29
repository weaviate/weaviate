//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package helper

import (
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	t.Logf("Creating backup with ID: %s, backend: %s, className: %s, config: %+v\n", backupID, backend, className, cfg)
	return Client(t).Backups.BackupsCreate(params, nil)
}

func CreateBackupWithAuthz(t *testing.T, cfg *models.BackupConfig, className, backend, backupID string, authInfo runtime.ClientAuthInfoWriter) (*backups.BackupsCreateOK, error) {
	params := backups.NewBackupsCreateParams().
		WithBackend(backend).
		WithBody(&models.BackupCreateRequest{
			ID:      backupID,
			Include: []string{className},
			Config:  cfg,
		})
	t.Logf("Creating backup with ID: %s, backend: %s, className: %s, config: %+v\n", backupID, backend, className, cfg)
	return Client(t).Backups.BackupsCreate(params, authInfo)
}

func ListBackup(t *testing.T, backend string) (*backups.BackupsListOK, error) {
	params := backups.NewBackupsListParams().
		WithBackend(backend)
	return Client(t).Backups.BackupsList(params, nil)
}

func ListBackupsWithAuthz(t *testing.T, backend string, authInfo runtime.ClientAuthInfoWriter) (*backups.BackupsListOK, error) {
	params := backups.NewBackupsListParams().
		WithBackend(backend)
	return Client(t).Backups.BackupsList(params, authInfo)
}

func CancelBackup(t *testing.T, backend, backupID string) error {
	params := backups.NewBackupsCancelParams().
		WithBackend(backend).
		WithID(backupID)
	_, err := Client(t).Backups.BackupsCancel(params, nil)
	return err
}

func CancelBackupWithAuthz(t *testing.T, backend, backupID string, authInfo runtime.ClientAuthInfoWriter) error {
	params := backups.NewBackupsCancelParams().
		WithBackend(backend).
		WithID(backupID)
	_, err := Client(t).Backups.BackupsCancel(params, authInfo)
	return err
}

func CreateBackupStatus(t *testing.T, backend, backupID, overrideBucket, overridePath string) (*backups.BackupsCreateStatusOK, error) {
	params := backups.NewBackupsCreateStatusParams().
		WithBackend(backend).
		WithID(backupID).
		WithBucket(&overrideBucket).
		WithPath(&overridePath)
	return Client(t).Backups.BackupsCreateStatus(params, nil)
}

func CreateBackupStatusWithAuthz(t *testing.T, backend, backupID, overrideBucket, overridePath string, authInfo runtime.ClientAuthInfoWriter) (*backups.BackupsCreateStatusOK, error) {
	params := backups.NewBackupsCreateStatusParams().
		WithBackend(backend).
		WithID(backupID).
		WithBucket(&overrideBucket).
		WithPath(&overridePath)
	return Client(t).Backups.BackupsCreateStatus(params, authInfo)
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

func RestoreBackupWithAuthz(t *testing.T, cfg *models.RestoreConfig, className, backend, backupID string, nodeMapping map[string]string, authInfo runtime.ClientAuthInfoWriter) (*backups.BackupsRestoreOK, error) {
	params := backups.NewBackupsRestoreParams().
		WithBackend(backend).
		WithID(backupID).
		WithBody(&models.BackupRestoreRequest{
			Include:     []string{className},
			NodeMapping: nodeMapping,
			Config:      cfg,
		})
	return Client(t).Backups.BackupsRestore(params, authInfo)
}

func RestoreBackupStatus(t *testing.T, backend, backupID, overrideBucket, overridePath string) (*backups.BackupsRestoreStatusOK, error) {
	params := backups.NewBackupsRestoreStatusParams().
		WithBackend(backend).
		WithID(backupID).
		WithBucket(&overrideBucket).
		WithPath(&overridePath)
	return Client(t).Backups.BackupsRestoreStatus(params, nil)
}

func RestoreBackupStatusWithAuthz(t *testing.T, backend, backupID, overrideBucket, overridePath string, authInfo runtime.ClientAuthInfoWriter) (*backups.BackupsRestoreStatusOK, error) {
	params := backups.NewBackupsRestoreStatusParams().
		WithBackend(backend).
		WithID(backupID).
		WithBucket(&overrideBucket).
		WithPath(&overridePath)
	return Client(t).Backups.BackupsRestoreStatus(params, authInfo)
}

// Expect the backup creation status to report SUCCESS within 30 seconds and 500ms polling interval.
func ExpectEventuallyCreated(t *testing.T, backupID, backend string, auth runtime.ClientAuthInfoWriter) {
	deadline := 30 * time.Second
	require.EventuallyWithTf(t, func(check *assert.CollectT) {
		var resp *backups.BackupsCreateStatusOK
		var err error
		if auth != nil {
			resp, err = CreateBackupStatusWithAuthz(t, backend, backupID, "", "", auth)
		} else {
			resp, err = CreateBackupStatus(t, backend, backupID, "", "")
		}

		require.NoError(t, err, "fetch backup create status")
		require.NotNil(t, resp.Payload, "empty response")

		status := *resp.Payload.Status
		require.NotEqualf(t, status, "FAILED", "create failed: %s", resp.Payload.Error)
		require.Equal(t, status, "SUCCESS", "backup create status")
	}, deadline, 500*time.Millisecond, "backup %s not created after %s", backupID, deadline)
}

// Expect the backup restore status to report SUCCESS within 30 seconds and 500ms polling interval.
func ExpectEventuallyRestored(t *testing.T, backupID, backend string, auth runtime.ClientAuthInfoWriter) {
	deadline := 30 * time.Second
	require.EventuallyWithTf(t, func(check *assert.CollectT) {
		var resp *backups.BackupsRestoreStatusOK
		var err error
		if auth != nil {
			resp, err = RestoreBackupStatusWithAuthz(t, backend, backupID, "", "", auth)
		} else {
			resp, err = RestoreBackupStatus(t, backend, backupID, "", "")
		}

		require.NoError(t, err, "fetch backup restore status")
		require.NotNil(t, resp.Payload, "empty response")

		status := *resp.Payload.Status
		require.NotEqualf(t, status, "FAILED", "restore failed: %s", resp.Payload.Error)
		require.Equal(t, status, "SUCCESS", "backup restore status")
	}, deadline, 500*time.Millisecond, "backup %s not restored after %s", backupID, deadline)
}
