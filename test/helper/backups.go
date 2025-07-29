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

const (
	MinPollInterval = 100 * time.Millisecond // Minimun interval for polling backup status.
	MaxDeadline     = 10 * time.Minute       // Maxium timeout for polling backup status.
)

// [backupExpectOpt.WithOptions] copies the struct, so it is safe to derive options
// from defaultBackupExpect directly:
//
//	defaultBackupExpect.WithOptions(opts)
var defaultBackupExpect = backupExpectOpt{
	Interval: 500 * time.Millisecond,
	Deadline: 30 * time.Second,
}

type backupExpectOpt struct {
	Interval time.Duration
	Deadline time.Duration
}

// WithOptions applies options to the copy of backupExpectOpt and returns it.
func (b backupExpectOpt) WithOptions(opts ...BackupExpectOpt) *backupExpectOpt {
	for _, opt := range opts {
		opt(&b)
	}
	return &b
}

type BackupExpectOpt func(*backupExpectOpt)

// Set the interval for polling backup create/restore status. Pass [helper.MinPollInterval] for rapid checks.
func WithPollInterval(d time.Duration) BackupExpectOpt {
	return func(opt *backupExpectOpt) { opt.Interval = max(d, MinPollInterval) }
}

// Set the deadline for receiving status SUCCESS. Waiting indefinitely is not allowed, use [helper.MaxDeadline] instead.
func WithDeadline(d time.Duration) BackupExpectOpt {
	return func(opt *backupExpectOpt) { opt.Deadline = min(d, MaxDeadline) }
}

// Expect creation status to report SUCCESS within 30s and with 500ms polling interval (default).
// Change polling configuration by passing [WithPollInterval] and [WithDeadline].
func ExpectBackupEventuallyCreated(t *testing.T, backupID, backend string, authz runtime.ClientAuthInfoWriter, opts ...BackupExpectOpt) {
	opt := defaultBackupExpect.WithOptions(opts...)

	require.EventuallyWithTf(t, func(check *assert.CollectT) {
		// Calling -WithAuthz with nil-auth is equivalent to using its no-authz counterpart
		resp, err := CreateBackupStatusWithAuthz(t, backend, backupID, "", "", authz)

		require.NoError(t, err, "fetch backup create status")
		require.NotNil(t, resp.Payload, "empty response")

		status := *resp.Payload.Status
		require.NotEqualf(t, status, "FAILED", "create failed: %s", resp.Payload.Error)
		require.Equal(t, status, "SUCCESS", "backup create status")
	}, opt.Deadline, opt.Interval, "backup %s not created after %s", backupID, opt.Deadline)
}

// Expect restore status to report SUCCESS within 30s and with 500ms polling interval (default).
// Change polling configuration by passing [WithPollInterval] and [WithDeadline].
func ExpectBackupEventuallyRestored(t *testing.T, backupID, backend string, authz runtime.ClientAuthInfoWriter, opts ...BackupExpectOpt) {
	opt := defaultBackupExpect.WithOptions(opts...)

	require.EventuallyWithTf(t, func(check *assert.CollectT) {
		// Calling -WithAuthz with nil-auth is equivalent to using its no-authz counterpart
		resp, err := RestoreBackupStatusWithAuthz(t, backend, backupID, "", "", authz)

		require.NoError(t, err, "fetch backup restore status")
		require.NotNil(t, resp.Payload, "empty response")

		status := *resp.Payload.Status
		require.NotEqualf(t, status, "FAILED", "restore failed: %s", resp.Payload.Error)
		require.Equal(t, status, "SUCCESS", "backup restore status")
	}, opt.Deadline, opt.Interval, "backup %s not restored after %s", backupID, opt.Deadline)
}
