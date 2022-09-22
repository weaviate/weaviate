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

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackup_DBLevel(t *testing.T) {
	t.Run("successful backup creation", func(t *testing.T) {
		ctx := testCtx()
		dirName := t.TempDir()
		className := "DBLevelBackupClass"
		backupID := "backup1"
		now := time.Now()

		db := setupTestDB(t, dirName, makeTestClass(className))
		defer func() {
			require.Nil(t, db.Shutdown(context.Background()))
		}()

		t.Run("insert data", func(t *testing.T) {
			require.Nil(t, db.PutObject(ctx, &models.Object{
				Class:              className,
				CreationTimeUnix:   now.UnixNano(),
				ID:                 "ff9fcae5-57b8-431c-b8e2-986fd78f5809",
				LastUpdateTimeUnix: now.UnixNano(),
				Vector:             []float32{1, 2, 3},
				VectorWeights:      nil,
			}, []float32{1, 2, 3}))
		})

		classes := db.ListBackupable()

		t.Run("create backup", func(t *testing.T) {
			err := db.Backupable(ctx, classes)
			assert.Nil(t, err)

			ch := db.BackupDescriptors(ctx, backupID, classes)

			for d := range ch {
				assert.Equal(t, className, d.Name)
				assert.Len(t, d.Shards, len(classes))
				for _, shd := range d.Shards {
					assert.NotEmpty(t, shd.Name)
					assert.NotEmpty(t, shd.Node)
					assert.NotEmpty(t, shd.Files)
					for _, f := range shd.Files {
						assert.NotEmpty(t, f)
					}
					assert.NotEmpty(t, shd.DocIDCounterPath)
					assert.NotEmpty(t, shd.DocIDCounter)
					assert.NotEmpty(t, shd.PropLengthTrackerPath)
					assert.NotEmpty(t, shd.PropLengthTracker)
					assert.NotEmpty(t, shd.ShardVersionPath)
					assert.NotEmpty(t, shd.Version)
				}
				assert.NotEmpty(t, d.ShardingState)
				assert.NotEmpty(t, d.Schema)
			}
		})

		t.Run("release backup", func(t *testing.T) {
			for _, class := range classes {
				err := db.ReleaseBackup(ctx, backupID, class)
				assert.Nil(t, err)
			}
		})
	})

	t.Run("failed backup creation from expired context", func(t *testing.T) {
		ctx := testCtx()
		dirName := t.TempDir()
		className := "DBLevelBackupClass"
		backupID := "backup1"
		now := time.Now()

		db := setupTestDB(t, dirName, makeTestClass(className))
		defer func() {
			require.Nil(t, db.Shutdown(context.Background()))
		}()

		t.Run("insert data", func(t *testing.T) {
			require.Nil(t, db.PutObject(ctx, &models.Object{
				Class:              className,
				CreationTimeUnix:   now.UnixNano(),
				ID:                 "ff9fcae5-57b8-431c-b8e2-986fd78f5809",
				LastUpdateTimeUnix: now.UnixNano(),
				Vector:             []float32{1, 2, 3},
				VectorWeights:      nil,
			}, []float32{1, 2, 3}))
		})

		t.Run("fail with expired context", func(t *testing.T) {
			classes := db.ListBackupable()

			err := db.Backupable(ctx, classes)
			assert.Nil(t, err)

			timeoutCtx, cancel := context.WithTimeout(context.Background(), 0)
			defer cancel()

			ch := db.BackupDescriptors(timeoutCtx, backupID, classes)
			for d := range ch {
				require.NotNil(t, d.Error)
				assert.Contains(t, d.Error.Error(), "context deadline exceeded")
			}
		})
	})
}

func TestBackup_BucketLevel(t *testing.T) {
	ctx := testCtx()
	className := "BucketLevelBackup"
	shard, _ := testShard(t, ctx, className)

	t.Run("insert data", func(t *testing.T) {
		err := shard.putObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    "8c29da7a-600a-43dc-85fb-83ab2b08c294",
				Class: className,
				Properties: map[string]interface{}{
					"stringField": "somevalue",
				},
			},
		},
		)
		require.Nil(t, err)
	})

	t.Run("perform backup sequence", func(t *testing.T) {
		objBucket := shard.store.Bucket("objects")
		require.NotNil(t, objBucket)

		err := objBucket.PauseCompaction(ctx)
		require.Nil(t, err)

		err = objBucket.FlushMemtable(ctx)
		require.Nil(t, err)

		files, err := objBucket.ListFiles(ctx)
		require.Nil(t, err)

		t.Run("check ListFiles, results", func(t *testing.T) {
			assert.Len(t, files, 1)

			// build regex to get very close approximation to the expected
			// contents of the ListFiles result. the only thing we can't
			// know for sure is the actual name of the segment group, hence
			// the `.*`
			re := path.Clean(fmt.Sprintf("^bucketlevelbackup_%s_lsm\\/objects\\/.*\\.(wal|db)", shard.name))

			// we expect to see only two files inside the bucket at this point:
			//   1. a *.db file
			//   2. a *.wal file
			//
			// both of these files are the result of the above FlushMemtable's
			// underlying call to FlushAndSwitch. The *.db is the flushed original
			// WAL, and the *.wal is the new one
			isMatch, err := regexp.MatchString(re, files[0])
			assert.Nil(t, err)
			assert.True(t, isMatch)

			// check that we have one of each: *.db
			assert.True(t, strings.HasSuffix(files[0], ".db"))
		})

		err = objBucket.ResumeCompaction(ctx)
		require.Nil(t, err)
	})

	t.Run("cleanup", func(t *testing.T) {
		require.Nil(t, shard.shutdown(ctx))
		require.Nil(t, os.RemoveAll(shard.index.Config.RootPath))
	})
}

func setupTestDB(t *testing.T, rootDir string, classes ...*models.Class) *DB {
	logger, _ := test.NewNullLogger()

	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	db := New(logger, Config{
		FlushIdleAfter:            60,
		RootPath:                  rootDir,
		QueryMaximumResults:       10,
		DiskUseWarningPercentage:  config.DefaultDiskUseWarningPercentage,
		DiskUseReadOnlyPercentage: config.DefaultDiskUseReadonlyPercentage,
		MaxImportGoroutinesFactor: 1,
		NodeName:                  "testNode",
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, nil, config.Config{})
	db.SetSchemaGetter(schemaGetter)
	err := db.WaitForStartup(testCtx())
	require.Nil(t, err)
	migrator := NewMigrator(db, logger)

	for _, class := range classes {
		require.Nil(t,
			migrator.AddClass(context.Background(), class, schemaGetter.shardState))
	}

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: classes,
		},
	}

	return db
}

func makeTestClass(className string) *models.Class {
	return &models.Class{
		VectorIndexConfig:   hnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               className,
		Properties: []*models.Property{
			{
				Name:     "stringProp",
				DataType: []string{string(schema.DataTypeString)},
			},
		},
	}
}
