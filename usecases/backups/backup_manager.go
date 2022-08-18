package backups

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/snapshots"
)

type BackupManager interface {
	CreateBackup(ctx context.Context, className, storageName, snapshotID string) (*snapshots.CreateMeta, error)
	RestoreBackup(ctx context.Context, className, storageName, snapshotID string) (*snapshots.RestoreMeta, error)
	CreateBackupStatus(ctx context.Context, className, storageName, snapshotID string) (*models.SnapshotMeta, error)
}
