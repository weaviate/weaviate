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

//go:build integrationTest

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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
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
			}, []float32{1, 2, 3}, nil, nil, nil, 0))
		})

		expectedNodeName := "node1"
		shards, err := db.schemaReader.Shards(className)
		require.Nil(t, err)
		expectedShardName := shards[0]
		testShd := db.GetIndex(schema.ClassName(className)).shards.Load(expectedShardName)
		expectedCounterPath, _ := filepath.Rel(testShd.Index().Config.RootPath, testShd.Counter().FileName())
		expectedCounter, err := os.ReadFile(testShd.Counter().FileName())
		require.Nil(t, err)
		expectedPropLengthPath, _ := filepath.Rel(testShd.Index().Config.RootPath, testShd.GetPropertyLengthTracker().FileName())
		expectedShardVersionPath, _ := filepath.Rel(testShd.Index().Config.RootPath, testShd.Versioner().path)
		expectedShardVersion, err := os.ReadFile(testShd.Versioner().path)
		require.Nil(t, err)
		expectedPropLength, err := os.ReadFile(testShd.GetPropertyLengthTracker().FileName())
		require.Nil(t, err)
		var expectedShardState []byte
		err = testShd.Index().schemaReader.Read(className, true, func(class *models.Class, state *sharding.State) error {
			var jsonErr error
			expectedShardState, jsonErr = state.JSON()
			return jsonErr
		})
		require.Nil(t, err)
		expectedSchema, err := testShd.Index().getSchema.GetSchemaSkipAuth().
			Objects.Classes[0].MarshalBinary()
		require.Nil(t, err)

		classes := make([]string, 0, len(db.indices))
		for _, idx := range db.indices {
			cls := string(idx.Config.ClassName)
			classes = append(classes, cls)
		}

		t.Run("doesn't fail on casing permutation of existing class", func(t *testing.T) {
			err := db.Backupable(ctx, []string{"DBLeVELBackupClass"})
			require.NotNil(t, err)
			require.Equal(t, "class DBLeVELBackupClass doesn't exist", err.Error())
		})

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
			res, err := db.Shards(ctx, className)
			assert.NoError(t, err)
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
			}, []float32{1, 2, 3}, nil, nil, nil, 9))
		})

		t.Run("fail with expired context", func(t *testing.T) {
			classes := make([]string, 0, len(db.indices))
			for _, idx := range db.indices {
				cls := string(idx.Config.ClassName)
				classes = append(classes, cls)
			}

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
		err := shard.PutObject(ctx,
			&storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					ID:    "8c29da7a-600a-43dc-85fb-83ab2b08c294",
					Class: className,
					Properties: map[string]interface{}{
						"stringField": "somevalue",
					},
				},
			})
		require.Nil(t, err)
	})

	t.Run("perform backup sequence", func(t *testing.T) {
		objBucket := shard.Store().Bucket("objects")
		require.NotNil(t, objBucket)

		err := shard.Store().PauseCompaction(ctx)
		require.Nil(t, err)

		err = objBucket.FlushMemtable()
		require.Nil(t, err)

		files, err := objBucket.ListFiles(ctx, shard.Index().Config.RootPath)
		require.Nil(t, err)

		t.Run("check ListFiles, results", func(t *testing.T) {
			assert.Len(t, files, 4)

			// build regex to get very close approximation to the expected
			// contents of the ListFiles result. the only thing we can't
			// know for sure is the actual name of the segment group, hence
			// the `.*`
			re := path.Clean(fmt.Sprintf("%s\\/.*\\.(wal|db|bloom|cna)", shard.Index().Config.RootPath))

			// we expect to see only four files inside the bucket at this point:
			//   1. a *.db file - the segment itself
			//   2. a *.bloom file - the segments' bloom filter (only since v1.17)
			//   3. a *.secondary.0.bloom file - the bloom filter for the secondary index at pos 0 (only since v1.17)
			//   4. a *.secondary.1.bloom file - the bloom filter for the secondary index at pos 1 (only since v1.25)
			//   5. a *.cna file - th segment's count net additions (only since v1.17)
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
			exts := make([]string, 5)
			for i, file := range files {
				exts[i] = filepath.Ext(file)
			}
			assert.Contains(t, exts, ".db")    // the main segment
			assert.Contains(t, exts, ".cna")   // the segment's count net additions
			assert.Contains(t, exts, ".bloom") // matches both bloom filters (primary+secondary ones)
		})

		err = shard.Store().ResumeCompaction(ctx)
		require.Nil(t, err)
	})

	t.Run("cleanup", func(t *testing.T) {
		require.Nil(t, shard.Shutdown(ctx))
		require.Nil(t, os.RemoveAll(shard.Index().Config.RootPath))
	})
}

func setupTestDB(t *testing.T, rootDir string, classes ...*models.Class) *DB {
	logger, _ := test.NewNullLogger()

	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: classes}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, version uint64) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	db, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  rootDir,
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, &FakeNodeResolver{}, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	db.SetSchemaGetter(schemaGetter)
	require.Nil(t, db.WaitForStartup(testCtx()))
	migrator := NewMigrator(db, logger, "node1")

	for _, class := range classes {
		require.Nil(t,
			migrator.AddClass(context.Background(), class))
	}

	// update schema getter so it's in sync with class
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: classes,
		},
	}

	return db
}

func TestDB_Shards(t *testing.T) {
	ctx := testCtx()
	logger, _ := test.NewNullLogger()

	t.Run("single shard with single node", func(t *testing.T) {
		className := "SingleShardClass"

		shardState := &sharding.State{
			Physical: map[string]sharding.Physical{
				"shard1": {
					Name:           "shard1",
					BelongsToNodes: []string{"node1"},
				},
			},
		}

		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Read(className, mock.Anything, mock.Anything).RunAndReturn(
			func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
				class := &models.Class{Class: className}
				return readFunc(class, shardState)
			},
		)

		db := &DB{
			logger:       logger,
			schemaReader: mockSchemaReader,
		}

		nodes, err := db.Shards(ctx, className)
		assert.NoError(t, err)
		assert.Len(t, nodes, 1)
		assert.Equal(t, "node1", nodes[0])
	})

	t.Run("single shard with multiple nodes", func(t *testing.T) {
		className := "SingleShardMultiNodeClass"

		shardState := &sharding.State{
			Physical: map[string]sharding.Physical{
				"shard1": {
					Name:           "shard1",
					BelongsToNodes: []string{"node1", "node2", "node3"},
				},
			},
		}

		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Read(className, mock.Anything, mock.Anything).RunAndReturn(
			func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
				class := &models.Class{Class: className}
				return readFunc(class, shardState)
			},
		)

		db := &DB{
			logger:       logger,
			schemaReader: mockSchemaReader,
		}

		nodes, err := db.Shards(ctx, className)
		assert.NoError(t, err)
		assert.Len(t, nodes, 3)
		assert.Contains(t, nodes, "node1")
		assert.Contains(t, nodes, "node2")
		assert.Contains(t, nodes, "node3")
	})

	t.Run("multiple shards with overlapping nodes", func(t *testing.T) {
		className := "MultiShardClass"

		shardState := &sharding.State{
			Physical: map[string]sharding.Physical{
				"shard1": {
					Name:           "shard1",
					BelongsToNodes: []string{"node1", "node2"},
				},
				"shard2": {
					Name:           "shard2",
					BelongsToNodes: []string{"node2", "node3"},
				},
				"shard3": {
					Name:           "shard3",
					BelongsToNodes: []string{"node1", "node3"},
				},
			},
		}

		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Read(className, mock.Anything, mock.Anything).RunAndReturn(
			func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
				class := &models.Class{Class: className}
				return readFunc(class, shardState)
			},
		)

		db := &DB{
			logger:       logger,
			schemaReader: mockSchemaReader,
		}

		nodes, err := db.Shards(ctx, className)
		assert.NoError(t, err)
		assert.Len(t, nodes, 3)
		assert.Contains(t, nodes, "node1")
		assert.Contains(t, nodes, "node2")
		assert.Contains(t, nodes, "node3")
	})

	t.Run("multiple shards with distinct nodes", func(t *testing.T) {
		className := "MultiShardDistinctClass"

		shardState := &sharding.State{
			Physical: map[string]sharding.Physical{
				"shard1": {
					Name:           "shard1",
					BelongsToNodes: []string{"node1"},
				},
				"shard2": {
					Name:           "shard2",
					BelongsToNodes: []string{"node2"},
				},
				"shard3": {
					Name:           "shard3",
					BelongsToNodes: []string{"node3"},
				},
			},
		}

		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Read(className, mock.Anything, mock.Anything).RunAndReturn(
			func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
				class := &models.Class{Class: className}
				return readFunc(class, shardState)
			},
		)

		db := &DB{
			logger:       logger,
			schemaReader: mockSchemaReader,
		}

		nodes, err := db.Shards(ctx, className)
		assert.NoError(t, err)
		assert.Len(t, nodes, 3)
		assert.Contains(t, nodes, "node1")
		assert.Contains(t, nodes, "node2")
		assert.Contains(t, nodes, "node3")
	})

	t.Run("no shards for class", func(t *testing.T) {
		className := "EmptyClass"

		// Empty physical shards
		shardState := &sharding.State{
			Physical: map[string]sharding.Physical{},
		}

		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Read(className, mock.Anything, mock.Anything).RunAndReturn(
			func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
				class := &models.Class{Class: className}
				return readFunc(class, shardState)
			},
		)

		db := &DB{
			logger:       logger,
			schemaReader: mockSchemaReader,
		}

		nodes, err := db.Shards(ctx, className)
		assert.NoError(t, err)
		assert.Len(t, nodes, 0)
		assert.Equal(t, []string{}, nodes)
	})

	t.Run("invalid sharding state (nil)", func(t *testing.T) {
		className := "NilStateClass"

		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		expectedErrorMsg := "invalid sharding state: state is nil"
		mockSchemaReader.EXPECT().
			Read(className, mock.Anything, mock.Anything).
			Return(fmt.Errorf("%s", expectedErrorMsg))

		db := &DB{
			logger:       logger,
			schemaReader: mockSchemaReader,
		}

		nodes, err := db.Shards(ctx, className)
		require.Error(t, err)
		assert.Nil(t, nodes)
		assert.Contains(t, err.Error(), expectedErrorMsg)
	})

	t.Run("schema reader error", func(t *testing.T) {
		className := "ErrorClass"

		mockSchemaReader := schemaUC.NewMockSchemaReader(t)
		mockSchemaReader.EXPECT().Read(className, mock.Anything, mock.Anything).Return(
			fmt.Errorf("schema read failed"),
		)

		db := &DB{
			logger:       logger,
			schemaReader: mockSchemaReader,
		}

		nodes, err := db.Shards(ctx, className)
		assert.Error(t, err)
		assert.Nil(t, nodes)
		assert.Contains(t, err.Error(), "failed to read sharding state")
		assert.Contains(t, err.Error(), "schema read failed")
	})
}

func makeTestClass(className string) *models.Class {
	return &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Class:               className,
		Properties: []*models.Property{
			{
				Name:         "stringProp",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	}
}
