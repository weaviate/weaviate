//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/moduletools"
	mod "github.com/weaviate/weaviate/modules/backup-filesystem"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
	ubak "github.com/weaviate/weaviate/usecases/backup"
)

// Environment variable name for filesystem backup path
const envBackupFilesystemPath = "BACKUP_FILESYSTEM_PATH"

func Test_FilesystemBackend_Start(t *testing.T) {
	// Test without path override
	filesystemBackend_Backup(t, false, "", "")

	// Test with path override
	filesystemBackend_Backup(t, true, "", "testPathOverride")
}

func filesystemBackend_Backup(t *testing.T, override bool, overrideBucket, overridePath string) {
	// Create a temporary directory for backups
	backupsPath := t.TempDir()

	t.Log("setup env")
	t.Setenv(envBackupFilesystemPath, backupsPath)

	// If using path override, create the override directory
	var actualOverridePath string
	if override && overridePath != "" {
		actualOverridePath = filepath.Join(t.TempDir(), overridePath)
		err := os.MkdirAll(actualOverridePath, os.ModePerm)
		require.NoError(t, err)
	}

	t.Logf("running tests with backups path %v and path override: %v\n", backupsPath, override)
	t.Run("store backup meta",
		func(t *testing.T) {
			moduleLevelStoreBackupMeta(t, backupsPath, override, overrideBucket, actualOverridePath)
		})
	t.Run("copy objects", func(t *testing.T) {
		moduleLevelCopyObjects(t, backupsPath, override, overrideBucket, actualOverridePath)
	})
	t.Run("copy files", func(t *testing.T) {
		moduleLevelCopyFiles(t, backupsPath, override, overrideBucket, actualOverridePath)
	})
}

func moduleLevelStoreBackupMeta(t *testing.T, backupsPath string, override bool, overrideBucket, overridePath string) {
	testCtx := context.Background()

	className := "BackupClass"
	backupID := "backup_id"
	metadataFilename := "backup.json"

	t.Logf("storing metadata with override: %v", override)

	t.Run("store backup meta in filesystem", func(t *testing.T) {
		fs := mod.New()
		params := moduletools.NewMockModuleInitParams(t)
		params.EXPECT().GetLogger().Return(logrus.New())
		params.EXPECT().GetStorageProvider().Return(&fakeStorageProvider{dataPath: t.TempDir()})
		err := fs.Init(testCtx, params)
		require.Nil(t, err)

		t.Run("access permissions", func(t *testing.T) {
			err := fs.Initialize(testCtx, backupID, overrideBucket, overridePath)
			assert.Nil(t, err)
		})

		t.Run("backup meta does not exist yet", func(t *testing.T) {
			meta, err := fs.GetObject(testCtx, backupID, metadataFilename, overrideBucket, overridePath)
			assert.Nil(t, meta)
			assert.NotNil(t, err)
			t.Log(err)
			assert.IsType(t, backup.ErrNotFound{}, err)
		})

		t.Run("put backup meta in backend", func(t *testing.T) {
			desc := &backup.BackupDescriptor{
				StartedAt:   time.Now(),
				CompletedAt: time.Time{},
				ID:          backupID,
				Classes: []backup.ClassDescriptor{
					{
						Name: className,
					},
				},
				Status:  backup.Started,
				Version: ubak.Version,
			}

			b, err := json.Marshal(desc)
			require.Nil(t, err)

			err = fs.PutObject(testCtx, backupID, metadataFilename, overrideBucket, overridePath, b)
			require.Nil(t, err)

			dest := fs.HomeDir(backupID, overrideBucket, overridePath)
			if overridePath != "" {
				expected := filepath.Join(overridePath, backupID)
				assert.Equal(t, expected, dest)
			} else {
				expected := filepath.Join(backupsPath, backupID)
				assert.Equal(t, expected, dest)
			}
		})

		t.Run("assert backup meta contents", func(t *testing.T) {
			obj, err := fs.GetObject(testCtx, backupID, metadataFilename, overrideBucket, overridePath)
			require.Nil(t, err)

			var meta backup.BackupDescriptor
			err = json.Unmarshal(obj, &meta)
			require.Nil(t, err)
			assert.NotEmpty(t, meta.StartedAt)
			assert.Empty(t, meta.CompletedAt)
			assert.Equal(t, meta.Status, backup.Started)
			assert.Empty(t, meta.Error)
			assert.Len(t, meta.Classes, 1)
			assert.Equal(t, meta.Classes[0].Name, className)
			assert.Equal(t, meta.Version, ubak.Version)
			assert.Nil(t, meta.Classes[0].Error)
		})
	})
}

func moduleLevelCopyObjects(t *testing.T, backupsPath string, override bool, overrideBucket, overridePath string) {
	testCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	key := "moduleLevelCopyObjects"
	backupID := "backup_id"

	t.Logf("copy objects with override: %v", override)

	t.Run("copy objects", func(t *testing.T) {
		fs := mod.New()
		params := moduletools.NewMockModuleInitParams(t)
		params.EXPECT().GetLogger().Return(logrus.New())
		params.EXPECT().GetStorageProvider().Return(&fakeStorageProvider{dataPath: t.TempDir()})
		err := fs.Init(testCtx, params)
		require.Nil(t, err)

		t.Run("put object to filesystem", func(t *testing.T) {
			t.Logf("put object with override: %v", override)
			err := fs.PutObject(testCtx, backupID, key, overrideBucket, overridePath, []byte("hello"))
			assert.Nil(t, err, "expected nil, got: %v", err)
		})

		t.Run("get object from filesystem", func(t *testing.T) {
			t.Logf("get object with override: %v", override)
			meta, err := fs.GetObject(testCtx, backupID, key, overrideBucket, overridePath)
			assert.Nil(t, err, "expected nil, got: %v", err)
			assert.Equal(t, []byte("hello"), meta)
		})
	})
}

func moduleLevelCopyFiles(t *testing.T, backupsPath string, override bool, overrideBucket, overridePath string) {
	testCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dataDir := t.TempDir()
	key := "moduleLevelCopyFiles"
	backupID := "backup_id"

	t.Run("copy files", func(t *testing.T) {
		fpaths := moduleshelper.CreateTestFiles(t, dataDir)
		fpath := fpaths[0]
		expectedContents, err := os.ReadFile(fpath)
		require.Nil(t, err)
		require.NotNil(t, expectedContents)

		fs := mod.New()
		params := moduletools.NewMockModuleInitParams(t)
		params.EXPECT().GetLogger().Return(logrus.New())
		params.EXPECT().GetStorageProvider().Return(&fakeStorageProvider{dataPath: dataDir})
		err = fs.Init(testCtx, params)
		require.Nil(t, err)

		t.Run("verify source data path", func(t *testing.T) {
			t.Log("source data path", fs.SourceDataPath())
			assert.Equal(t, dataDir, fs.SourceDataPath())
		})

		t.Run("copy file to backend", func(t *testing.T) {
			err := fs.PutObject(testCtx, backupID, key, overrideBucket, overridePath, expectedContents)
			require.Nil(t, err)

			contents, err := fs.GetObject(testCtx, backupID, key, overrideBucket, overridePath)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})

		t.Run("fetch file from backend", func(t *testing.T) {
			destPath := dataDir + "/file_0.copy.db"

			t.Logf("download file with override: %v", override)
			err := fs.WriteToFile(testCtx, backupID, key, destPath, overrideBucket, overridePath)
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

func (f *fakeStorageProvider) Storage(name string) (moduletools.Storage, error) {
	return nil, nil
}

func (f *fakeStorageProvider) DataPath() string {
	return f.dataPath
}
