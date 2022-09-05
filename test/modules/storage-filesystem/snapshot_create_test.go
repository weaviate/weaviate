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

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/backup"
	modstgfs "github.com/semi-technologies/weaviate/modules/storage-filesystem"
	moduleshelper "github.com/semi-technologies/weaviate/test/helper/modules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_FilesystemStorage_SnapshotCreate(t *testing.T) {
	t.Run("store snapshot", moduleLevelStoreSnapshot)
}

func moduleLevelStoreSnapshot(t *testing.T) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	className := "BackupClass"
	backupID := "backup_id"
	metadataFilename := "snapshot.json"

	defer moduleshelper.RemoveDir(t, backupID)

	t.Run("store snapshot in fs", func(t *testing.T) {
		stgfs := modstgfs.New()

		meta, err := stgfs.GetObject(testCtx, backupID, metadataFilename)
		assert.Nil(t, meta)
		assert.NotNil(t, err)
		assert.IsType(t, backup.ErrNotFound{}, err)

		err = stgfs.Initialize(testCtx, backupID)
		assert.Nil(t, err)

		desc := &backup.BackupDescriptor{
			StartedAt:   time.Now(),
			CompletedAt: time.Time{},
			ID:          backupID,
			Classes: []backup.ClassDescriptor{
				{
					Name: className,
				},
			},
			Status: string(backup.Started),
		}

		b, err := json.Marshal(desc)
		require.Nil(t, err)

		err = stgfs.PutObject(testCtx, backupID, metadataFilename, b)
		require.Nil(t, err)

		dest := stgfs.DestinationPath(backupID)
		expected := fmt.Sprintf("%s/%s", backupID, metadataFilename)
		assert.Equal(t, expected, dest)

		t.Run("assert snapshot meta contents", func(t *testing.T) {
			obj, err := stgfs.GetObject(testCtx, backupID, metadataFilename)
			require.Nil(t, err)

			var meta backup.BackupDescriptor
			err = json.Unmarshal(obj, &meta)
			require.Nil(t, err)
			assert.NotEmpty(t, meta.StartedAt)
			assert.Empty(t, meta.CompletedAt)
			assert.Equal(t, meta.Status, string(backup.Started))
			assert.Empty(t, meta.Error)
			assert.Len(t, meta.Classes, 1)
			assert.Equal(t, meta.Classes[0].Name, className)
			assert.Nil(t, meta.Classes[0].Error)
		})
	})

	t.Run("restore snapshot in fs", func(t *testing.T) {
		t.Skip("skip until restore is implemented")
	})
}
