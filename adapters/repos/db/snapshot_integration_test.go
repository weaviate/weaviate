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
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_IndexLevel(t *testing.T) {
	t.Run("successful snapshot creation", func(t *testing.T) {
		t.Run("setup env", func(t *testing.T) {
			var spec struct {
				Info struct {
					Version string `json:"version"`
				} `json:"info"`
			}

			contents, err := ioutil.ReadFile("../../../openapi-specs/schema.json")
			require.Nil(t, err)

			require.Nil(t, json.Unmarshal(contents, &spec))
			// this is normally set on server start up, so
			// it needs to be manually set for this test
			config.ServerVersion = spec.Info.Version
		})

		ctx := testCtx()
		className := "IndexLevelSnapshotClass"
		snapshotID := "index-level-snapshot-test"
		now := time.Now().UnixNano()

		shard, index := testShard(ctx, className, withVectorIndexing(true))
		// let the index age for a second so that
		// the commitlogger filenames, which are
		// based on current timestamp, can differ
		time.Sleep(time.Second)

		t.Run("insert data", func(t *testing.T) {
			require.Nil(t, index.putObject(ctx, &storobj.Object{
				MarshallerVersion: 1,
				Object: models.Object{
					Class:              className,
					CreationTimeUnix:   now,
					ID:                 "ff9fcae5-57b8-431c-b8e2-986fd78f5809",
					LastUpdateTimeUnix: now,
					Vector:             []float32{1, 2, 3},
					VectorWeights:      nil,
				},
				Vector: []float32{1, 2, 3},
			}))
		})

		t.Run("create snapshot", func(t *testing.T) {
			snap, err := index.CreateSnapshot(ctx, snapshotID)
			assert.Nil(t, err)

			t.Run("assert snapshot file contents", func(t *testing.T) {
				// should have 7 files:
				//     - 6 files from lsm store:
				//         - objects/segment-123.wal
				//         - objects/segment-123.db
				//         - hash_property__id/segment-123.wal
				//         - hash_property__id/segment-123.db
				//         - property__id/segment-123.wal
				//         - property__id/segment-123.db
				//     - 1 file from vector index commitlogger
				assert.Len(t, snap.Files, 7)
			})

			t.Run("assert shard metadata contents", func(t *testing.T) {
				assert.NotNil(t, snap.ShardMetadata)
				assert.Len(t, snap.ShardMetadata, 1)
				assert.NotEmpty(t, snap.ShardMetadata[shard.name].DocIDCounter)
				assert.NotEmpty(t, snap.ShardMetadata[shard.name].PropLengthTracker)
				assert.NotEmpty(t, snap.ShardMetadata[shard.name].ShardVersion)
			})

			t.Run("assert schema state", func(t *testing.T) {
				assert.NotEmpty(t, snap.ShardingState)
				assert.NotEmpty(t, snap.Schema)
			})

			t.Run("assert server version", func(t *testing.T) {
				assert.NotEmpty(t, snap.ServerVersion)
				assert.Equal(t, config.ServerVersion, snap.ServerVersion)
			})

			t.Run("assert snapshot disk contents", func(t *testing.T) {
				snapPath := snapshots.BuildSnapshotPath(index.Config.RootPath, snap.ClassName, snap.ID)

				contents, err := ioutil.ReadFile(snapPath)
				require.Nil(t, err)

				expected, err := json.Marshal(snap)
				require.Nil(t, err)

				assert.Equal(t, expected, contents)
			})
		})

		t.Run("release snapshot", func(t *testing.T) {
			err := index.ReleaseSnapshot(ctx, snapshotID)
			assert.Nil(t, err)

			assert.False(t, index.snapshotState.InProgress)

			t.Run("assert snapshot disk contents", func(t *testing.T) {
				snap, err := snapshots.ReadFromDisk(index.Config.RootPath, className, snapshotID)
				require.Nil(t, err)

				assert.NotEmpty(t, snap.CompletedAt)
				assert.Equal(t, snapshots.StatusReleased, snap.Status)
			})
		})

		t.Run("cleanup", func(t *testing.T) {
			err := index.Shutdown(ctx)
			require.Nil(t, err)

			err = os.RemoveAll(index.Config.RootPath)
			require.Nil(t, err)
		})
	})

	t.Run("failed snapshot creation from expired context", func(t *testing.T) {
		ctx := testCtx()

		className := "IndexLevelSnapshotClass"
		snapshotID := "index-level-snapshot-test"

		_, index := testShard(ctx, className, withVectorIndexing(true))

		timeout, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()

		snap, err := index.CreateSnapshot(timeout, snapshotID)
		assert.Nil(t, snap)

		// due to concurrently running cycle shutdowns,
		// we cannot always know which will error first
		// amongst the affected cycles (i.e. compaction,
		// tombstone cleanup, etc)
		expected1 := "create snapshot: long-running"
		expected2 := "context deadline exceeded"
		assert.Contains(t, err.Error(), expected1)
		assert.Contains(t, err.Error(), expected2)

		// snapshot state is reset on failure, so that
		// subsequent calls to create a snapshot are
		// not blocked
		assert.False(t, index.snapshotState.InProgress)
		assert.Empty(t, index.snapshotState.SnapshotID)

		snapPath := snapshots.BuildSnapshotPath(index.Config.RootPath, className, snapshotID)
		_, err = os.Stat(snapPath)
		expectedErr := &fs.PathError{Op: "stat", Path: snapPath, Err: syscall.ENOENT}
		assert.Equal(t, err, expectedErr)

		t.Run("cleanup", func(t *testing.T) {
			err := index.Shutdown(ctx)
			require.Nil(t, err)

			err = os.RemoveAll(index.Config.RootPath)
			require.Nil(t, err)
		})
	})

	t.Run("failed snapshot creation from existing unreleased snapshot", func(t *testing.T) {
		ctx := testCtx()
		className := "IndexLevelSnapshotClass"
		inProgressSnapshotID := "index-level-snapshot-test"

		_, index := testShard(ctx, className, withVectorIndexing(true))

		index.snapshotState = snapshots.State{
			SnapshotID: inProgressSnapshotID,
			InProgress: true,
		}

		snap, err := index.CreateSnapshot(ctx, "some-new-snapshot")
		assert.Nil(t, snap)

		expectedErr := fmt.Errorf("cannot create new snapshot, snapshot ‘%s’ "+
			"is not yet released, this means its contents have not yet been fully copied "+
			"to its destination, try again later", inProgressSnapshotID)
		assert.EqualError(t, expectedErr, err.Error())

		t.Run("cleanup", func(t *testing.T) {
			err := index.Shutdown(ctx)
			require.Nil(t, err)

			err = os.RemoveAll(index.Config.RootPath)
			require.Nil(t, err)
		})
	})
}

func TestSnapshot_BucketLevel(t *testing.T) {
	ctx := testCtx()
	className := "BucketLevelSnapshot"
	shard, _ := testShard(ctx, className)

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

	t.Run("perform snapshot sequence", func(t *testing.T) {
		objBucket := shard.store.Bucket("objects")
		require.NotNil(t, objBucket)

		err := objBucket.PauseCompaction(ctx)
		require.Nil(t, err)

		err = objBucket.FlushMemtable(ctx)
		require.Nil(t, err)

		files, err := objBucket.ListFiles(ctx)
		require.Nil(t, err)

		t.Run("check ListFiles, results", func(t *testing.T) {
			assert.Len(t, files, 2)

			// build regex to get very close approximation to the expected
			// contents of the ListFiles result. the only thing we can't
			// know for sure is the actual name of the segment group, hence
			// the `.*`
			re := path.Clean(fmt.Sprintf("^bucketlevelsnapshot_%s_lsm\\/objects\\/.*\\.(wal|db)", shard.name))

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
			isMatch, err = regexp.MatchString(re, files[1])
			assert.True(t, isMatch)

			// check that we have one of each: *.db, *.wal
			if strings.HasSuffix(files[0], ".db") {
				assert.True(t, strings.HasSuffix(files[0], ".db"))
				assert.True(t, strings.HasSuffix(files[1], ".wal"))
			} else {
				assert.True(t, strings.HasSuffix(files[0], ".wal"))
				assert.True(t, strings.HasSuffix(files[1], ".db"))
			}
		})

		err = objBucket.ResumeCompaction(ctx)
		require.Nil(t, err)
	})

	t.Run("cleanup", func(t *testing.T) {
		require.Nil(t, shard.shutdown(ctx))
		require.Nil(t, os.RemoveAll(shard.index.Config.RootPath))
	})
}
