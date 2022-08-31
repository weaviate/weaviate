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
	if err != nil {
		t.Fatalf("expected nil err, got: %s", err.Error())
	}

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
