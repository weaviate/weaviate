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

package helper

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/backups"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/require"
)

func CreateBackup(t *testing.T, className, storageName, snapshotID string) {
	params := backups.NewBackupsCreateParams().
		WithStorageName(storageName).
		WithBody(&models.BackupCreateRequest{
			ID:      snapshotID,
			Include: []string{className},
		})
	resp, err := Client(t).Backups.BackupsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateBackupStatus(t *testing.T, className, storageName, snapshotID string) *models.BackupCreateMeta {
	params := backups.NewBackupsCreateStatusParams().
		WithStorageName(storageName).
		WithID(snapshotID)
	resp, err := Client(t).Backups.BackupsCreateStatus(params, nil)
	if err != nil {
		t.Fatalf("expected nil err, got: %s", err.Error())
	}

	return resp.Payload
}

func RestoreBackup(t *testing.T, className, storageName, snapshotID string) {
	params := backups.NewBackupsRestoreParams().
		WithStorageName(storageName).
		WithID(snapshotID).
		WithBody(&models.BackupRestoreRequest{
			Include: []string{className},
		})
	resp, err := Client(t).Backups.BackupsRestore(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func RestoreBackupStatus(t *testing.T, className, storageName, snapshotID string) *models.BackupRestoreMeta {
	params := backups.NewBackupsRestoreStatusParams().
		WithStorageName(storageName).
		WithID(snapshotID)
	resp, err := Client(t).Backups.BackupsRestoreStatus(params, nil)
	require.Nil(t, err)

	return resp.Payload
}
