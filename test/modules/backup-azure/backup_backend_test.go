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

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/moduletools"
	mod "github.com/weaviate/weaviate/modules/backup-azure"
	"github.com/weaviate/weaviate/test/docker"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_AzureBackend_Start(t *testing.T) {
	tests := []struct {
		name           string
		overrideBucket string
		overridePath   string
	}{
		{
			name:           "default overrides",
			overrideBucket: "",
			overridePath:   "",
		},
		{
			name:           "test bucket and path overrides",
			overrideBucket: "testbucketoverride",
			overridePath:   "testBucketPathOverride",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testAzureBackendBackup(tt.overrideBucket, tt.overridePath)(t)
		})
	}
}

func testAzureBackendBackup(overrideBucket, overridePath string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		compose, err := docker.New().WithAzurite().Start(ctx)
		if err != nil {
			t.Fatalf("cannot start: %v", err)
		}
		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %v", err)
			}
		}()

		t.Setenv(envAzureEndpoint, compose.GetAzurite().URI())

		subTests := []struct {
			name string
			test func(t *testing.T)
		}{
			{
				name: "store backup meta",
				test: func(t *testing.T) {
					moduleLevelStoreBackupMeta(t, overrideBucket, overridePath)
				},
			},
			{
				name: "copy objects",
				test: func(t *testing.T) {
					moduleLevelCopyObjects(t, overrideBucket, overridePath)
				},
			},
			{
				name: "copy files",
				test: func(t *testing.T) {
					moduleLevelCopyFiles(t, overrideBucket, overridePath)
				},
			},
		}

		for _, st := range subTests {
			t.Run(st.name, st.test)
		}
	}
}

func moduleLevelStoreBackupMeta(t *testing.T, overrideBucket, overridePath string) {
	testCtx := context.Background()

	dataDir := t.TempDir()
	className := "BackupClass"
	backupID := "backup_id"
	containerName := "container-level-store-backup-meta"
	if overrideBucket != "" {
		containerName = overrideBucket
	}
	endpoint := os.Getenv(envAzureEndpoint)
	metadataFilename := "backup.json"

	t.Log("setup env")
	t.Setenv(envAzureEndpoint, endpoint)
	t.Setenv(envAzureStorageConnectionString, fmt.Sprintf(connectionString, endpoint))
	t.Setenv(envAzureContainer, containerName)
	moduleshelper.CreateAzureContainer(testCtx, t, endpoint, containerName)
	defer moduleshelper.DeleteAzureContainer(testCtx, t, endpoint, containerName)

	t.Run("store backup meta in Azure", func(t *testing.T) {
		t.Setenv(envAzureContainer, containerName)
		azure := mod.New()
		err := azure.Init(testCtx, newFakeModuleParams(dataDir))
		require.Nil(t, err)

		t.Run("access permissions", func(t *testing.T) {
			err := azure.Initialize(testCtx, backupID, overrideBucket, overridePath)
			assert.Nil(t, err)
		})

		t.Run("backup meta does not exist yet", func(t *testing.T) {
			meta, err := azure.GetObject(testCtx, backupID, metadataFilename, overrideBucket, overridePath)
			assert.Nil(t, meta)
			assert.NotNil(t, err)
			assert.IsType(t, backup.ErrNotFound{}, err)
		})

		t.Run("put backup meta on backend", func(t *testing.T) {
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

			err = azure.PutObject(testCtx, backupID, metadataFilename, overrideBucket, overridePath, b)
			require.Nil(t, err)

			dest := azure.HomeDir(backupID, overrideBucket, overridePath)

			expected := fmt.Sprintf("http://%s/devstoreaccount1/%s/%s", os.Getenv(envAzureEndpoint), containerName, backupID)
			if overridePath != "" {
				expected = fmt.Sprintf("http://%s/devstoreaccount1/%s/%s/%s", os.Getenv(envAzureEndpoint), containerName, overridePath, backupID)
			}
			assert.Equal(t, expected, dest)
		})

		t.Run("assert backup meta contents", func(t *testing.T) {
			obj, err := azure.GetObject(testCtx, backupID, metadataFilename, overrideBucket, overridePath)
			t.Logf("Error: %+v", err)
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
			assert.Equal(t, meta.Version, ubak.Version)
			assert.Nil(t, meta.Classes[0].Error)
		})
	})
}

func moduleLevelCopyObjects(t *testing.T, overrideBucket, overridePath string) {
	testCtx := context.Background()

	dataDir := t.TempDir()
	key := "moduleLevelCopyObjects"
	backupID := "backup_id"
	containerName := "container-level-copy-objects"
	if overrideBucket != "" {
		containerName = overrideBucket
	}
	endpoint := os.Getenv(envAzureEndpoint)

	t.Log("setup env")
	t.Setenv(envAzureEndpoint, endpoint)
	t.Setenv(envAzureStorageConnectionString, fmt.Sprintf(connectionString, endpoint))
	t.Setenv(envAzureContainer, containerName)
	moduleshelper.CreateAzureContainer(testCtx, t, endpoint, containerName)
	defer moduleshelper.DeleteAzureContainer(testCtx, t, endpoint, containerName)

	t.Run("copy objects", func(t *testing.T) {
		t.Setenv(envAzureContainer, containerName)
		azure := mod.New()
		err := azure.Init(testCtx, newFakeModuleParams(dataDir))
		require.Nil(t, err)

		t.Run("put object to bucket", func(t *testing.T) {
			err := azure.PutObject(testCtx, backupID, key, overrideBucket, overridePath, []byte("hello"))
			t.Logf("Error: %+v", err)
			assert.Nil(t, err)
		})

		t.Run("get object from bucket", func(t *testing.T) {
			meta, err := azure.GetObject(testCtx, backupID, key, overrideBucket, overridePath)
			assert.Nil(t, err)
			assert.Equal(t, []byte("hello"), meta)
		})
	})
}

func moduleLevelCopyFiles(t *testing.T, overrideBucket, overridePath string) {
	testCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	key := "moduleLevelCopyFiles"
	backupID := "backup_id"
	containerName := "container-level-copy-files"
	if overrideBucket != "" {
		containerName = overrideBucket
	}
	endpoint := os.Getenv(envAzureEndpoint)

	t.Log("setup env")
	t.Setenv(envAzureEndpoint, endpoint)
	t.Setenv(envAzureStorageConnectionString, fmt.Sprintf(connectionString, endpoint))
	t.Logf("Connection string: %s\n", os.Getenv(envAzureStorageConnectionString))
	t.Setenv(envAzureContainer, containerName)
	t.Logf("Creating container %s\n", containerName)
	moduleshelper.CreateAzureContainer(testCtx, t, endpoint, containerName)
	defer moduleshelper.DeleteAzureContainer(testCtx, t, endpoint, containerName)

	t.Run("copy files", func(t *testing.T) {
		fpaths := moduleshelper.CreateTestFiles(t, dataDir)
		fpath := fpaths[0]
		expectedContents, err := os.ReadFile(fpath)
		require.Nil(t, err)
		require.NotNil(t, expectedContents)

		t.Setenv(envAzureContainer, containerName)
		azure := mod.New()
		err = azure.Init(testCtx, newFakeModuleParams(dataDir))
		require.Nil(t, err)

		t.Run("verify source data path", func(t *testing.T) {
			assert.Equal(t, dataDir, azure.SourceDataPath())
		})

		t.Run("copy file to backend", func(t *testing.T) {
			err = azure.PutObject(testCtx, backupID, key, overrideBucket, overridePath, expectedContents)
			require.Nil(t, err)

			contents, err := azure.GetObject(testCtx, backupID, key, overrideBucket, overridePath)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})

		t.Run("fetch file from backend", func(t *testing.T) {
			destPath := dataDir + "/file_0.copy.db"

			err := azure.WriteToFile(testCtx, backupID, key, destPath, overrideBucket, overridePath)
			require.Nil(t, err)

			contents, err := os.ReadFile(destPath)
			require.Nil(t, err)
			assert.Equal(t, expectedContents, contents)
		})
	})
}

type fakeModuleParams struct {
	logger   logrus.FieldLogger
	provider fakeStorageProvider
	config   config.Config
}

func newFakeModuleParams(dataPath string) *fakeModuleParams {
	logger, _ := logrustest.NewNullLogger()
	return &fakeModuleParams{
		logger:   logger,
		provider: fakeStorageProvider{dataPath: dataPath},
	}
}

func (f *fakeModuleParams) GetStorageProvider() moduletools.StorageProvider {
	return &f.provider
}

func (f *fakeModuleParams) GetAppState() interface{} {
	return nil
}

func (f *fakeModuleParams) GetLogger() logrus.FieldLogger {
	return f.logger
}

func (f *fakeModuleParams) GetConfig() config.Config {
	return f.config
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
