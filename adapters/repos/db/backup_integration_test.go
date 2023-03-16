//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
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
			}, []float32{1, 2, 3}, nil))
		})

		expectedNodeName := "node1"
		expectedShardName := db.schemaGetter.
			ShardingState(className).
			AllPhysicalShards()[0]
		testShd := db.GetIndex(schema.ClassName(className)).
			Shards[expectedShardName]
		expectedCounterPath := path.Base(testShd.counter.FileName())
		expectedCounter, err := os.ReadFile(testShd.counter.FileName())
		require.Nil(t, err)
		expectedPropLengthPath := path.Base(testShd.propLengths.FileName())
		expectedShardVersionPath := path.Base(testShd.versioner.path)
		expectedShardVersion, err := os.ReadFile(testShd.versioner.path)
		require.Nil(t, err)
		expectedPropLength, err := os.ReadFile(testShd.propLengths.FileName())
		require.Nil(t, err)
		expectedShardState, err := testShd.index.getSchema.ShardingState(className).JSON()
		require.Nil(t, err)
		expectedSchema, err := testShd.index.getSchema.GetSchemaSkipAuth().
			Objects.Classes[0].MarshalBinary()
		require.Nil(t, err)

		classes := db.ListBackupable()

		t.Run("create backup", func(t *testing.T) {
			err := db.Backupable(ctx, classes)
			assert.Nil(t, err)

			ch := db.BackupDescriptors(ctx, backupID, classes)

			for d := range ch {
				assert.Equal(t, className, d.Name)
				assert.Len(t, d.Shards, len(classes))
				for _, shd := range d.Shards {
					assert.Equal(t, expectedShardName, shd.Name)
					assert.Equal(t, expectedNodeName, shd.Node)
					assert.NotEmpty(t, shd.Files)
					for _, f := range shd.Files {
						assert.NotEmpty(t, f)
					}
					assert.Equal(t, expectedCounterPath, shd.DocIDCounterPath)
					assert.Equal(t, expectedCounter, shd.DocIDCounter)
					assert.Equal(t, expectedPropLengthPath, shd.PropLengthTrackerPath)
					assert.Equal(t, expectedPropLength, shd.PropLengthTracker)
					assert.Equal(t, expectedShardVersionPath, shd.ShardVersionPath)
					assert.Equal(t, expectedShardVersion, shd.Version)
				}
				assert.Equal(t, expectedShardState, d.ShardingState)
				assert.Equal(t, expectedSchema, d.Schema)
			}
		})

		t.Run("release backup", func(t *testing.T) {
			for _, class := range classes {
				err := db.ReleaseBackup(ctx, backupID, class)
				assert.Nil(t, err)
			}
		})

		t.Run("node names from shards", func(t *testing.T) {
			res := db.Shards(ctx, className)
			assert.Len(t, res, 1)
			assert.Equal(t, "node1", res[0])
		})

		t.Run("get all classes", func(t *testing.T) {
			res := db.ListClasses(ctx)
			assert.Len(t, res, 1)
			assert.Equal(t, className, res[0])
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
			}, []float32{1, 2, 3}, nil))
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
			assert.Len(t, files, 4)

			// build regex to get very close approximation to the expected
			// contents of the ListFiles result. the only thing we can't
			// know for sure is the actual name of the segment group, hence
			// the `.*`
			re := path.Clean(fmt.Sprintf("^bucketlevelbackup_%s_lsm\\/objects\\/.*\\.(wal|db|bloom|cna)", shard.name))

			// we expect to see only four files inside the bucket at this point:
			//   1. a *.db file - the segment itself
			//   2. a *.bloom file - the segments' bloom filter (only since v1.17)
			//   3. a *.secondary.0.bloom file - the bloom filter for the secondary index at pos 0 (only since v1.17)
			//   4. a *.cna file - th segment's count net additions (only since v1.17)
			//
			// These files are created when the memtable is flushed, and the new
			// segment is initialized. Both happens as a result of calling
			// FlushMemtable().
			for i := range files {
				isMatch, err := regexp.MatchString(re, files[i])
				assert.Nil(t, err)
				assert.True(t, isMatch, files[i])
			}

			// check that we have one of each: *.db
			exts := make([]string, 4)
			for i, file := range files {
				exts[i] = filepath.Ext(file)
			}
			assert.Contains(t, exts, ".db")    // the main segment
			assert.Contains(t, exts, ".cna")   // the segment's count net additions
			assert.Contains(t, exts, ".bloom") // matches both bloom filters (primary+secondary)
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
	db, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  rootDir,
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil)
	require.Nil(t, err)
	db.SetSchemaGetter(schemaGetter)
	require.Nil(t, db.WaitForStartup(testCtx()))
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
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
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
