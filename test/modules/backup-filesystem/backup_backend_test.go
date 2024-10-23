//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/moduletools"
	modstgfs "github.com/weaviate/weaviate/modules/backup-filesystem"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_FileSystemBackend_Start(t *testing.T) {
	filesystemBackend_Backup(t, "", "", "")
	filesystemBackend_Backup(t, "", "bucketPath", " with override") // Note:  no bucket parameter, because it is not supported by the filesystem backend
}

func filesystemBackend_Backup(t *testing.T, overrideBucket, overridePath, overrideDescription string) {
	t.Run("store backup meta"+overrideDescription, func(t *testing.T) { moduleLevelStoreBackupMeta(t, overrideBucket, overridePath, overrideDescription) })
	t.Run("copy objects"+overrideDescription, func(t *testing.T) { moduleLevelCopyObjects(t, overrideBucket, overridePath, overrideDescription) })
	t.Run("copy files"+overrideDescription, func(t *testing.T) { moduleLevelCopyFiles(t, overrideBucket, overridePath, overrideDescription) })
}

func moduleLevelStoreBackupMeta(t *testing.T, overrideBucket, overridePath, overrideDescription string) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	backupDir := t.TempDir()
	overrideDir := t.TempDir()
	if overridePath != "" {
		overridePath = overrideDir
	}
	className := "BackupClass"
	backupID := "backup_id"
	metadataFilename := "backup.json"

	t.Setenv("BACKUP_FILESYSTEM_PATH", backupDir)

	t.Run("store backup meta in fs"+overrideDescription, func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		sp := fakeStorageProvider{dataDir}
		params := moduletools.NewInitParams(sp, nil, config.Config{}, logger)

		fs := modstgfs.New()
		err := fs.Init(testCtx, params)
		require.Nil(t, err)

		t.Run("access permissions"+overrideDescription, func(t *testing.T) {
			err := fs.Initialize(testCtx, backupID, overrideBucket, overridePath)
			assert.Nil(t, err)
		})

		t.Run("backup meta does not exist yet"+overrideDescription, func(t *testing.T) {
			meta, err := fs.GetObject(testCtx, backupID, metadataFilename, overrideBucket, overridePath)
			assert.Nil(t, meta)
			assert.NotNil(t, err)
			assert.IsType(t, backup.ErrNotFound{}, err)
		})

		t.Run("put backup meta on backend"+overrideDescription, func(t *testing.T) {
			desc := &backup.BackupDescriptor{
				StartedAt:   time.Now(),
				CompletedAt: time.Time{},
				ID:          backupID,
				Classes: []backup.ClassDescriptor{
					{
						Name: className,
					},
				},
				Status:  string(backup.Started),
				Version: ubak.Version,
			}

			b, err := json.Marshal(desc)
			require.Nil(t, err)

			err = fs.PutObject(testCtx, backupID, metadataFilename, overrideBucket, overridePath, b) // Note:  no bucket parameter, because it is not supported by the filesystem backend
			require.Nil(t, err)

			dest := fs.HomeDir(backupID, overrideBucket, overridePath)
			if overridePath == "" {
				expected := fmt.Sprintf("%s/%s", backupDir, backupID)
				assert.Equal(t, expected, dest)
			} else {
				expected := fmt.Sprintf("%s/%s", overridePath, backupID)
				assert.Equal(t, expected, dest)
			}
		})
	})
}

func moduleLevelCopyObjects(t *testing.T, overrideBucket, overridePath, overrideDescription string) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	backupDir := t.TempDir()
	key := "moduleLevelCopyObjects"
	backupID := "backup_id"

	t.Setenv("BACKUP_FILESYSTEM_PATH", backupDir)

	t.Run("copy objects"+overrideDescription, func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		sp := fakeStorageProvider{dataDir}
		params := moduletools.NewInitParams(sp, nil, config.Config{}, logger)

		fs := modstgfs.New()
		err := fs.Init(testCtx, params)
		require.Nil(t, err)

		t.Run("put object to bucket"+overrideDescription, func(t *testing.T) {
			err := fs.PutObject(testCtx, backupID, key, overrideBucket, overridePath, []byte("hello"))
			assert.Nil(t, err)
		})

		t.Run("get object from bucket"+overrideDescription, func(t *testing.T) {
			meta, err := fs.GetObject(testCtx, backupID, key, overrideBucket, overridePath)
			assert.Nil(t, err)
			assert.Equal(t, []byte("hello"), meta)
		})
	})
}

func moduleLevelCopyFiles(t *testing.T, overrideBucket, overridePath, overrideDescription string) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	backupDir := t.TempDir()
	key := "moduleLevelCopyFiles"
	backupID := "backup_id"

	t.Setenv("BACKUP_FILESYSTEM_PATH", backupDir)

	t.Run("copy files"+overrideDescription, func(t *testing.T) {
		fpaths := moduleshelper.CreateTestFiles(t, dataDir)
		fpath := fpaths[0]
		expectedContents, err := os.ReadFile(fpath)
		require.Nil(t, err)
		require.NotNil(t, expectedContents)

		logger, _ := test.NewNullLogger()
		sp := fakeStorageProvider{dataDir}
		params := moduletools.NewInitParams(sp, nil, config.Config{}, logger)

		fs := modstgfs.New()
		err = fs.Init(testCtx, params)
		require.Nil(t, err)

		t.Run("verify source data path"+overrideDescription, func(t *testing.T) {
			assert.Equal(t, dataDir, fs.SourceDataPath())
		})

		t.Logf("Source data path: %s\n"+overrideDescription, fs.SourceDataPath())

		t.Run("copy file to backend", func(t *testing.T) {
			err := fs.PutObject(testCtx, backupID, key, overrideBucket, overridePath, expectedContents)
			require.Nil(t, err)

			contents, err := fs.GetObject(testCtx, backupID, key, overrideBucket, overridePath)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})

		t.Run("fetch file from backend"+overrideDescription, func(t *testing.T) {
			destPath := dataDir + "/file_0.copy.db"

			t.Logf("Calling WriteToFile with backupID: %s, key: %s, destPath: %s, override: %s\n", backupID, key, destPath, overrideBucket+"/"+overridePath)
			err := fs.WriteToFile(testCtx, backupID, key, destPath, overrideBucket, overridePath)
			t.Logf("Error: %+v", err)
			require.Nil(t, err)

			contents, err := os.ReadFile(destPath)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})
	})
}

type fakeStorageProvider struct {
	dataPath string
}

func (sp fakeStorageProvider) Storage(name string) (moduletools.Storage, error) {
	return nil, nil
}

func (sp fakeStorageProvider) DataPath() string {
	return sp.dataPath
}
