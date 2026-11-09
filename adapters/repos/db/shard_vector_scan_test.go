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

package db

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// scanTestShard returns a shard with the named vectors "title" and "body".
func scanTestShard(t *testing.T) *Shard {
	t.Helper()
	shardLike, _ := testShardWithSettings(t, testCtx(), &models.Class{Class: "Scan"}, enthnsw.UserConfig{Skip: true},
		false, false, func(idx *Index) {
			idx.vectorIndexUserConfig = nil
			idx.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{
				"title": enthnsw.UserConfig{Skip: true},
				"body":  enthnsw.UserConfig{Skip: true},
			}
		})
	return underlyingShard(t, shardLike)
}

// putScanObject stores an object with the given named vectors and returns its doc ID.
func putScanObject(t *testing.T, shard *Shard, vectors map[string][]float32) uint64 {
	t.Helper()
	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object:            models.Object{ID: strfmt.UUID(uuid.NewString()), Class: "Scan"},
		Vectors:           vectors,
	}
	require.NoError(t, shard.PutObject(testCtx(), obj))
	return docIDOf(t, shard, obj.ID())
}

func docIDOf(t *testing.T, shard *Shard, id strfmt.UUID) uint64 {
	t.Helper()
	key, err := uuid.MustParse(id.String()).MarshalBinary()
	require.NoError(t, err)
	data, err := shard.store.Bucket(helpers.ObjectsBucketLSM).Get(key)
	require.NoError(t, err)
	docID, err := storobj.DocIDFromBinary(data)
	require.NoError(t, err)
	return docID
}

func flushObjects(t *testing.T, shard *Shard) {
	t.Helper()
	require.NoError(t, shard.store.Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())
}

// collectScan scans "title" and returns the vectors it saw by doc ID, failing
// the test if a doc ID is seen twice.
func collectScan(t *testing.T, shard *Shard, window vectorScanWindow, onRecord func()) (map[uint64][]float32, error) {
	t.Helper()
	seen := map[uint64][]float32{}
	err := shard.scanVectorsInWindows(testCtx(), "title", false, window, func(record common.VectorRecord) error {
		vec := record.(*common.Vector[[]float32])
		_, dup := seen[vec.ID]
		require.False(t, dup, "doc ID %d read twice", vec.ID)
		seen[vec.ID] = vec.Vector
		if onRecord != nil {
			onRecord()
		}
		return nil
	})
	return seen, err
}

// shutdownWithin fails the test if the shard cannot shut down, which happens
// when a scan leaves its cursor or its bucket pin behind.
func shutdownWithin(t *testing.T, shard *Shard, timeout time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- shard.Shutdown(context.Background()) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(timeout):
		t.Fatal("shard shutdown is still waiting for the scan")
	}
}

func TestScanVectors_ReadsEveryStoredVector(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window vectorScanWindow
	}{
		{name: "one window", window: defaultVectorScanWindow},
		{name: "reopened after every object", window: vectorScanWindow{maxDuration: 0, checkEvery: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shard := scanTestShard(t)
			want := map[uint64][]float32{}
			put := func(i int) strfmt.UUID {
				vec := []float32{float32(i), 1}
				obj := &storobj.Object{
					MarshallerVersion: 1,
					Object:            models.Object{ID: strfmt.UUID(uuid.NewString()), Class: "Scan"},
					Vectors:           map[string][]float32{"title": vec, "body": {2, 2}},
				}
				require.NoError(t, shard.PutObject(testCtx(), obj))
				want[docIDOf(t, shard, obj.ID())] = vec
				return obj.ID()
			}

			var ids []strfmt.UUID
			for i := range 20 {
				ids = append(ids, put(i))
			}
			for range 5 {
				putScanObject(t, shard, map[string][]float32{"body": {3, 3}})
			}
			flushObjects(t, shard)
			for i := 20; i < 40; i++ {
				ids = append(ids, put(i))
			}

			// a deleted object and the old version of an updated one must not be read
			deleted := docIDOf(t, shard, ids[3])
			require.NoError(t, shard.DeleteObject(testCtx(), ids[3], time.Now()))
			delete(want, deleted)
			updated := &storobj.Object{
				MarshallerVersion: 1,
				Object:            models.Object{ID: ids[25], Class: "Scan"},
				Vectors:           map[string][]float32{"title": {99, 99}},
			}
			delete(want, docIDOf(t, shard, ids[25]))
			require.NoError(t, shard.PutObject(testCtx(), updated))
			want[docIDOf(t, shard, ids[25])] = []float32{99, 99}

			seen, err := collectScan(t, shard, tc.window, nil)
			require.NoError(t, err)
			require.Equal(t, want, seen)
		})
	}
}

func TestScanVectors_DefaultWindow(t *testing.T) {
	shard := scanTestShard(t)
	docID := putScanObject(t, shard, map[string][]float32{"title": {1, 2}})

	var got []uint64
	err := shard.scanVectors(testCtx(), "title", false, func(record common.VectorRecord) error {
		got = append(got, record.(*common.Vector[[]float32]).ID)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{docID}, got)
}

func TestScanVectors_RejectsTheUnnamedVector(t *testing.T) {
	shard := scanTestShard(t)
	err := shard.scanVectors(testCtx(), "", false, func(common.VectorRecord) error { return nil })
	require.ErrorContains(t, err, "named vector is required")
}

// Objects written while the scan reopens its cursor may or may not be read,
// but every object that stays unchanged is read exactly once.
func TestScanVectors_ReopeningUnderWrites(t *testing.T) {
	shard := scanTestShard(t)
	unchanged := map[uint64]bool{}
	for i := range 60 {
		unchanged[putScanObject(t, shard, map[string][]float32{"title": {float32(i), 1}})] = true
	}
	flushObjects(t, shard)

	written := 0
	seen, err := collectScan(t, shard, vectorScanWindow{maxDuration: 0, checkEvery: 1}, func() {
		if written < 20 {
			putScanObject(t, shard, map[string][]float32{"title": {-1, -1}})
			written++
		}
		if written == 10 {
			flushObjects(t, shard)
		}
	})
	require.NoError(t, err)
	for docID := range unchanged {
		require.Contains(t, seen, docID)
	}
}

func TestScanVectors_StopsAndReleasesTheCursor(t *testing.T) {
	errCallback := errors.New("callback failed")

	for _, tc := range []struct {
		name string
		// prepare may damage the stored data; it returns the callback to scan with
		prepare   func(t *testing.T, shard *Shard, cancel context.CancelFunc) func(common.VectorRecord) error
		wantCalls int
		check     func(t *testing.T, err error)
	}{
		{
			name: "cancelled",
			prepare: func(t *testing.T, shard *Shard, cancel context.CancelFunc) func(common.VectorRecord) error {
				calls := 0
				return func(common.VectorRecord) error {
					calls++
					if calls == 3 {
						cancel()
					}
					return nil
				}
			},
			wantCalls: 3,
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, context.Canceled)
			},
		},
		{
			name: "callback error",
			prepare: func(t *testing.T, shard *Shard, cancel context.CancelFunc) func(common.VectorRecord) error {
				return func(common.VectorRecord) error { return errCallback }
			},
			wantCalls: 1,
			check: func(t *testing.T, err error) {
				require.ErrorIs(t, err, errCallback)
			},
		},
		{
			name: "callback panic",
			prepare: func(t *testing.T, shard *Shard, cancel context.CancelFunc) func(common.VectorRecord) error {
				return func(common.VectorRecord) error { panic("index insert blew up") }
			},
			wantCalls: 1,
			check: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "panic: index insert blew up")
			},
		},
		{
			name: "corrupt object",
			prepare: func(t *testing.T, shard *Shard, cancel context.CancelFunc) func(common.VectorRecord) error {
				docID := make([]byte, 8)
				binary.LittleEndian.PutUint64(docID, 1_000_000)
				key, err := uuid.New().MarshalBinary()
				require.NoError(t, err)
				// marshaller version 2 does not exist
				value := append([]byte{2}, docID...)
				require.NoError(t, shard.store.Bucket(helpers.ObjectsBucketLSM).
					Put(key, value, lsmkv.WithSecondaryKey(0, docID)))
				return func(common.VectorRecord) error { return nil }
			},
			wantCalls: -1, // depends on where the random key sorts
			check: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "unsupported marshaller version 2")
			},
		},
		{
			name: "segment read error",
			prepare: func(t *testing.T, shard *Shard, cancel context.CancelFunc) func(common.VectorRecord) error {
				corruptFirstSegmentNode(t, shard)
				return func(common.VectorRecord) error { return nil }
			},
			wantCalls: -1,
			check: func(t *testing.T, err error) {
				require.ErrorContains(t, err, "panic:")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shard := scanTestShard(t)
			for i := range 10 {
				putScanObject(t, shard, map[string][]float32{"title": {float32(i), 1}})
			}
			flushObjects(t, shard)

			ctx, cancel := context.WithCancel(testCtx())
			defer cancel()
			fn := tc.prepare(t, shard, cancel)

			calls := 0
			err := shard.scanVectorsInWindows(ctx, "title", false, defaultVectorScanWindow,
				func(record common.VectorRecord) error {
					calls++
					return fn(record)
				})
			tc.check(t, err)
			if tc.wantCalls >= 0 {
				require.Equal(t, tc.wantCalls, calls)
			}
			shutdownWithin(t, shard, 10*time.Second)
		})
	}
}

// corruptFirstSegmentNode makes the first node of the objects bucket's only
// segment claim a value longer than the file, so reading it fails.
func corruptFirstSegmentNode(t *testing.T, shard *Shard) {
	t.Helper()
	dir := shard.store.Bucket(helpers.ObjectsBucketLSM).GetDir()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var segments []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".db") {
			segments = append(segments, filepath.Join(dir, entry.Name()))
		}
	}
	require.Len(t, segments, 1)

	f, err := os.OpenFile(segments[0], os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	info, err := f.Stat()
	require.NoError(t, err)

	// a node starts with a tombstone byte, then the value length
	length := make([]byte, 8)
	binary.LittleEndian.PutUint64(length, uint64(info.Size())+1024)
	_, err = f.WriteAt(length, segmentindex.HeaderSize+1)
	require.NoError(t, err)
}
