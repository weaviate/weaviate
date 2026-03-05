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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	entBackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	backupUC "github.com/weaviate/weaviate/usecases/backup"
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

			ch := db.BackupDescriptors(ctx, backupID, classes, nil)

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

			ch := db.BackupDescriptors(timeoutCtx, backupID, classes, nil)
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

// TestBackup_CompressRestoreWithSplitting is an end-to-end integration test that
// creates a real database with objects, obtains backup descriptors, compresses
// shards into chunks (with file splitting), restores from those chunks, and
// verifies every file matches the original.
func TestBackup_CompressRestoreWithSplitting(t *testing.T) {
	ctx := testCtx()
	dirName := t.TempDir()
	className := "SplitFileBackupClass"
	backupID := "backup-split"
	now := time.Now()

	db := setupTestDB(t, dirName, makeTestClass(className))
	defer func() {
		require.Nil(t, db.Shutdown(context.Background()))
	}()

	// Insert several objects to produce shard files on disk.
	for i := 0; i < 20; i++ {
		vec := []float32{float32(i), float32(i + 1), float32(i + 2)}
		require.Nil(t, db.PutObject(ctx, &models.Object{
			Class:              className,
			CreationTimeUnix:   now.UnixNano(),
			ID:                 strfmt.UUID(fmt.Sprintf("ff9fcae5-57b8-431c-b8e2-%012d", i)),
			LastUpdateTimeUnix: now.UnixNano(),
			Vector:             vec,
			VectorWeights:      nil,
		}, vec, nil, nil, nil, 0))
	}

	classes := []string{className}
	require.Nil(t, db.Backupable(ctx, classes))

	var classDescs []entBackup.ClassDescriptor
	ch := db.BackupDescriptors(ctx, backupID, classes, nil)
	for d := range ch {
		require.Nil(t, d.Error)
		classDescs = append(classDescs, d)
	}
	require.Len(t, classDescs, 1)
	require.NotEmpty(t, classDescs[0].Shards)

	sourceDataPath := db.config.RootPath

	// Use very small chunk/split sizes to force file splitting even on small test data.
	const chunkSize = 512     // 512 bytes
	const splitFileSize = 256 // 256 bytes

	result := backupWithSizes(t, ctx, sourceDataPath, classDescs, chunkSize, splitFileSize)
	t.Logf("backup produced %d chunks from %d shards", len(result.chunks), len(classDescs[0].Shards))
	require.Greater(t, len(result.chunks), 1, "expected multiple chunks due to small chunk/split sizes")

	restoreAndVerify(t, sourceDataPath, classDescs, result.chunks)

	for _, class := range classes {
		require.Nil(t, db.ReleaseBackup(ctx, backupID, class))
	}
}

// backupChunkResult holds the result of a single backup pass.
type backupChunkResult struct {
	chunks    [][]byte // raw chunk data
	fileSizes []int64  // per-file sizes (from stat) across all shards
}

// backupWithSizes performs a backup using the given chunk/split sizes and returns the chunks and file sizes.
func backupWithSizes(
	t *testing.T,
	ctx context.Context,
	sourceDataPath string,
	classDescs []entBackup.ClassDescriptor,
	chunkSize, splitFileSize int64,
) backupChunkResult {
	t.Helper()

	var result backupChunkResult

	for _, sd := range classDescs[0].Shards {
		filesInShard := &entBackup.FileList{Files: append([]string{}, sd.Files...)}

		// Collect actual file sizes for reporting.
		for _, relPath := range sd.Files {
			info, err := os.Stat(filepath.Join(sourceDataPath, relPath))
			if err == nil && info.Mode().IsRegular() {
				result.fileSizes = append(result.fileSizes, info.Size())
			}
		}

		var fileSizeExceeded *backupUC.SplitFile
		firstChunk := true

		for {
			var buf bytes.Buffer
			z, rc, err := backupUC.NewZip(sourceDataPath, int(backupUC.NoCompression), chunkSize, 0, splitFileSize)
			require.NoError(t, err)

			type writeResult struct {
				split *backupUC.SplitFile
				err   error
			}
			resultCh := make(chan writeResult, 1)

			go func() {
				preComp := atomic.Int64{}
				var sr *backupUC.SplitFile
				var we error
				if fileSizeExceeded != nil {
					sr, we = z.WriteSplitFile(ctx, sd, fileSizeExceeded, &preComp, "")
				} else {
					_, sr, we = z.WriteShard(ctx, sd, filesInShard, firstChunk, &preComp, "")
				}
				z.Close()
				resultCh <- writeResult{split: sr, err: we}
			}()

			_, err = io.Copy(&buf, rc)
			require.NoError(t, err)
			require.NoError(t, rc.Close())
			res := <-resultCh
			require.NoError(t, res.err)

			result.chunks = append(result.chunks, buf.Bytes())
			fileSizeExceeded = res.split
			firstChunk = false

			if filesInShard.Len() == 0 && fileSizeExceeded == nil {
				break
			}
		}
	}
	return result
}

// restoreAndVerify restores all chunks into a temp dir and verifies every shard file matches the original.
func restoreAndVerify(
	t *testing.T,
	sourceDataPath string,
	classDescs []entBackup.ClassDescriptor,
	chunks [][]byte,
) {
	t.Helper()

	restoreDir := t.TempDir()

	var wg sync.WaitGroup
	errs := make([]error, len(chunks))
	for i, c := range chunks {
		wg.Add(1)
		go func(idx int, chunk []byte) {
			defer wg.Done()
			uz, wc := backupUC.NewUnzip(restoreDir, entBackup.CompressionNone)
			go func() {
				_, _ = io.Copy(wc, bytes.NewReader(chunk))
				wc.Close()
			}()
			_, errs[idx] = uz.ReadChunk()
			uz.Close()
		}(i, c)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "chunk %d restore failed", i)
	}

	for _, sd := range classDescs[0].Shards {
		for _, relPath := range sd.Files {
			originalPath := filepath.Join(sourceDataPath, relPath)
			restoredPath := filepath.Join(restoreDir, relPath)

			original, err := os.ReadFile(originalPath)
			require.NoError(t, err, "read original %s", relPath)
			restored, err := os.ReadFile(restoredPath)
			require.NoError(t, err, "read restored %s", relPath)
			require.Equal(t, original, restored, "file content mismatch for %s", relPath)
		}

		restoredCounter, err := os.ReadFile(filepath.Join(restoreDir, sd.DocIDCounterPath))
		require.NoError(t, err)
		require.Equal(t, sd.DocIDCounter, restoredCounter, "DocIDCounter mismatch")

		restoredPropLength, err := os.ReadFile(filepath.Join(restoreDir, sd.PropLengthTrackerPath))
		require.NoError(t, err)
		require.Equal(t, sd.PropLengthTracker, restoredPropLength, "PropLengthTracker mismatch")

		restoredVersion, err := os.ReadFile(filepath.Join(restoreDir, sd.ShardVersionPath))
		require.NoError(t, err)
		require.Equal(t, sd.Version, restoredVersion, "Version mismatch")
	}
}

// TestBackup_SplitSizeReducesChunkSize starts with a large split file size (no splitting),
// measures chunk sizes, then sets the split size below the biggest files and verifies that
// more chunks are produced while the restore still succeeds.
func TestBackup_SplitSizeReducesChunkSize(t *testing.T) {
	ctx := testCtx()
	dirName := t.TempDir()
	className := "SplitSizeChunkClass"
	backupID := "backup-split-size"
	now := time.Now()

	db := setupTestDB(t, dirName, makeTestClass(className))
	defer func() {
		require.Nil(t, db.Shutdown(context.Background()))
	}()

	// Insert enough objects so shard files are non-trivial.
	for i := 0; i < 50; i++ {
		vec := []float32{float32(i), float32(i + 1), float32(i + 2)}
		require.Nil(t, db.PutObject(ctx, &models.Object{
			Class:              className,
			CreationTimeUnix:   now.UnixNano(),
			ID:                 strfmt.UUID(fmt.Sprintf("ff9fcae5-57b8-431c-b8e2-%012d", i)),
			LastUpdateTimeUnix: now.UnixNano(),
			Vector:             vec,
			VectorWeights:      nil,
		}, vec, nil, nil, nil, 0))
	}

	classes := []string{className}
	require.Nil(t, db.Backupable(ctx, classes))

	var classDescs []entBackup.ClassDescriptor
	ch := db.BackupDescriptors(ctx, backupID, classes, nil)
	for d := range ch {
		require.Nil(t, d.Error)
		classDescs = append(classDescs, d)
	}
	require.Len(t, classDescs, 1)
	require.NotEmpty(t, classDescs[0].Shards)

	sourceDataPath := db.config.RootPath

	// --- Pass 1: backup with a large split file size (effectively no splitting) ---
	const largeChunkSize = 512               // small chunk target to get multiple chunks
	const largeSplitSize = 100 * 1024 * 1024 // 100 MB — no file will be this big

	pass1 := backupWithSizes(t, ctx, sourceDataPath, classDescs, largeChunkSize, largeSplitSize)
	t.Logf("pass 1 (no splitting): %d chunks", len(pass1.chunks))
	require.Greater(t, len(pass1.chunks), 1, "expected multiple chunks with small chunk target")

	// Verify pass 1 restore works.
	restoreAndVerify(t, sourceDataPath, classDescs, pass1.chunks)

	// Find the sizes of the 3 biggest files and set splitFileSize below the biggest.
	sortedSizes := append([]int64{}, pass1.fileSizes...)
	sort.Slice(sortedSizes, func(i, j int) bool { return sortedSizes[i] > sortedSizes[j] })

	// We need at least 3 files bigger than our intended split size for this test to be meaningful.
	require.GreaterOrEqual(t, len(sortedSizes), 3, "need at least 3 files")
	// Set split size to half the size of the 3rd biggest file so that at least the top 3 files
	// get split, producing extra chunks.
	splitSize := sortedSizes[2] / 2
	require.Greater(t, splitSize, int64(0), "3rd biggest file must be > 0 bytes")

	t.Logf("top 3 file sizes: %d, %d, %d; using splitFileSize=%d",
		sortedSizes[0], sortedSizes[1], sortedSizes[2], splitSize)

	// --- Pass 2: backup with the reduced split file size ---
	// Release pass 1 backup and re-acquire so we can create a new descriptor.
	for _, class := range classes {
		require.Nil(t, db.ReleaseBackup(ctx, backupID, class))
	}
	require.Nil(t, db.Backupable(ctx, classes))

	var classDescs2 []entBackup.ClassDescriptor
	ch2 := db.BackupDescriptors(ctx, backupID+"-2", classes, nil)
	for d := range ch2 {
		require.Nil(t, d.Error)
		classDescs2 = append(classDescs2, d)
	}

	pass2 := backupWithSizes(t, ctx, sourceDataPath, classDescs2, largeChunkSize, splitSize)
	t.Logf("pass 2 (splitFileSize=%d): %d chunks", splitSize, len(pass2.chunks))

	// The smaller split size should produce more chunks because large files are now split.
	require.Greater(t, len(pass2.chunks), len(pass1.chunks),
		"expected more chunks when split file size is reduced (pass1=%d, pass2=%d)",
		len(pass1.chunks), len(pass2.chunks))

	// Verify pass 2 restore produces identical files.
	restoreAndVerify(t, sourceDataPath, classDescs2, pass2.chunks)

	// Release pass 2 backup hold (pass 1 was already released above).
	for _, class := range classes {
		require.Nil(t, db.ReleaseBackup(ctx, backupID+"-2", class))
	}
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
