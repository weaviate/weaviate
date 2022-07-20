package modulecapabilities

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/snapshots"
)

type SnapshotStorage interface {
	StoreSnapshot(ctx context.Context, snapshot snapshots.Snapshot) error
	RestoreSnapshot(ctx context.Context, snapshotId string) error
}
