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

package backups

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
)

const (
	CS_STARTED      CreateStatus = "STARTED"
	CS_TRANSFERRING CreateStatus = "TRANSFERRING"
	CS_TRANSFERRED  CreateStatus = "TRANSFERRED"
	CS_SUCCESS      CreateStatus = "SUCCESS"
	CS_FAILED       CreateStatus = "FAILED"
)

const (
	RS_STARTED      RestoreStatus = "STARTED"
	RS_TRANSFERRING RestoreStatus = "TRANSFERRING"
	RS_TRANSFERRED  RestoreStatus = "TRANSFERRED"
	RS_SUCCESS      RestoreStatus = "SUCCESS"
	RS_FAILED       RestoreStatus = "FAILED"
)

type (
	CreateStatus  string
	RestoreStatus string
)

type CreateMeta struct {
	Path   string
	Status CreateStatus
}

type RestoreMeta struct {
	Path   string
	Status RestoreStatus
}

type BackupManager interface {
	CreateBackup(ctx context.Context, className, storageName, snapshotID string) (*CreateMeta, error)
	RestoreBackup(ctx context.Context, className, storageName, snapshotID string) (*RestoreMeta, error)
	CreateBackupStatus(ctx context.Context, className, storageName, snapshotID string) (*models.SnapshotMeta, error)
}
