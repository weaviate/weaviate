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
	"github.com/semi-technologies/weaviate/entities/snapshots"
)

type BackupManager interface {
	CreateBackup(ctx context.Context, className, storageName, snapshotID string) (*snapshots.CreateMeta, error)
	RestoreBackup(ctx context.Context, className, storageName, snapshotID string) (*snapshots.RestoreMeta, *snapshots.Snapshot, error)
	CreateBackupStatus(ctx context.Context, className, storageName, snapshotID string) (*models.SnapshotMeta, error)
	DestinationPath(storageName, className, snapshotID string) (string, error)
}
