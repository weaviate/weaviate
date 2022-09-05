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
)

func CreateBackup(t *testing.T, className, storageName, snapshotID string) (*backups.BackupsCreateOK, error) {
	params := backups.NewBackupsCreateParams().
		WithStorageName(storageName).
		WithBody(&models.BackupCreateRequest{
			ID:      snapshotID,
			Include: []string{className},
		})
	return Client(t).Backups.BackupsCreate(params, nil)
}

func CreateBackupStatus(t *testing.T, storageName, snapshotID string) (*backups.BackupsCreateStatusOK, error) {
	params := backups.NewBackupsCreateStatusParams().
		WithStorageName(storageName).
		WithID(snapshotID)
	return Client(t).Backups.BackupsCreateStatus(params, nil)
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

func RestoreBackupStatus(t *testing.T, className, storageName, snapshotID string) (*backups.BackupsRestoreStatusOK, error) {
	params := backups.NewBackupsRestoreStatusParams().
		WithStorageName(storageName).
		WithID(snapshotID)
	return Client(t).Backups.BackupsRestoreStatus(params, nil)
}
