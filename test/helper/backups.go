package helper

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/require"
)

func CreateBackup(t *testing.T, className, storageName, snapshotID string) {
	params := schema.NewSchemaObjectsSnapshotsCreateParams().
		WithClassName(className).
		WithStorageName(storageName).
		WithID(snapshotID)
	resp, err := Client(t).Schema.SchemaObjectsSnapshotsCreate(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func CreateBackupStatus(t *testing.T, className, storageName, snapshotID string) *models.SnapshotMeta {
	params := schema.NewSchemaObjectsSnapshotsCreateStatusParams().
		WithClassName(className).
		WithStorageName(storageName).
		WithID(snapshotID)
	resp, err := Client(t).Schema.SchemaObjectsSnapshotsCreateStatus(params, nil)
	require.Nil(t, err)

	return resp.Payload
}

func RestoreBackup(t *testing.T, className, storageName, snapshotID string) {
	params := schema.NewSchemaObjectsSnapshotsRestoreParams().
		WithClassName(className).
		WithStorageName(storageName).
		WithID(snapshotID)
	resp, err := Client(t).Schema.SchemaObjectsSnapshotsRestore(params, nil)
	AssertRequestOk(t, resp, err, nil)
}

func RestoreBackupStatus(t *testing.T, className, storageName, snapshotID string) *models.SnapshotRestoreMeta {
	params := schema.NewSchemaObjectsSnapshotsRestoreStatusParams().
		WithClassName(className).
		WithStorageName(storageName).
		WithID(snapshotID)
	resp, err := Client(t).Schema.SchemaObjectsSnapshotsRestoreStatus(params, nil)
	require.Nil(t, err)

	return resp.Payload
}
