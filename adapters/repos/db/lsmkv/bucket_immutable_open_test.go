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

package lsmkv

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type walEntry struct {
	key   string
	value string
}

// TestOpenImmutableBucketWithWAL covers what an immutable open does to a bucket directory
// that still holds write-ahead-logs. The immutable arm must leave every byte in place and
// still see the logged data; the writable arm shows the same directory being rewritten.
func TestOpenImmutableBucketWithWAL(t *testing.T) {
	bigValue := strings.Repeat("x", 5000)

	type dirCounts struct {
		wals     int
		segments int
	}

	tests := []struct {
		name string
		// seed writes the bucket directory; an empty logs entry becomes a zero-length WAL
		logs         [][]walEntry
		truncateLast int
		wantValues   map[string]string
		// wantWritable is the directory a writable open leaves behind, for contrast
		wantWritable dirCounts
	}{
		{
			name:         "single log below the reuse threshold",
			logs:         [][]walEntry{{{"k1", "v1"}}},
			wantValues:   map[string]string{"k1": "v1"},
			wantWritable: dirCounts{wals: 1, segments: 0},
		},
		{
			name:         "empty log next to a populated one",
			logs:         [][]walEntry{{}, {{"k1", "v1"}}},
			wantValues:   map[string]string{"k1": "v1"},
			wantWritable: dirCounts{wals: 1, segments: 0},
		},
		{
			name:         "two logs, the newer one overwriting a key",
			logs:         [][]walEntry{{{"k1", "v1"}, {"k2", "old"}}, {{"k2", "new"}}},
			wantValues:   map[string]string{"k1": "v1", "k2": "new"},
			wantWritable: dirCounts{wals: 1, segments: 1},
		},
		{
			name:         "log above the reuse threshold",
			logs:         [][]walEntry{{{"k1", bigValue}}},
			wantValues:   map[string]string{"k1": bigValue},
			wantWritable: dirCounts{wals: 0, segments: 1},
		},
		{
			name:         "log that ends abruptly",
			logs:         [][]walEntry{{{"k1", "v1"}, {"k2", "v2"}}},
			truncateLast: 3,
			wantValues:   map[string]string{"k1": "v1"},
			wantWritable: dirCounts{wals: 0, segments: 1},
		},
		{
			name:         "directory that does not exist",
			wantValues:   map[string]string{},
			wantWritable: dirCounts{wals: 0, segments: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"/immutable", func(t *testing.T) {
			dir := seedBucketDir(t, tt.logs, tt.truncateLast)
			before := dirSnapshot(t, dir)

			readValues(t, dir, tt.wantValues, WithImmutable(true))

			require.Equal(t, before, dirSnapshot(t, dir),
				"an immutable open must not add, change or remove a write-ahead-log")

			// the logs are still there to be recovered by the owner of the shard
			readValues(t, dir, tt.wantValues)
		})

		t.Run(tt.name+"/writable", func(t *testing.T) {
			dir := seedBucketDir(t, tt.logs, tt.truncateLast)

			readValues(t, dir, tt.wantValues)

			require.Equal(t, tt.wantWritable.wals, countFilesWithExt(t, dir, ".wal"))
			require.Equal(t, tt.wantWritable.segments, countFilesWithExt(t, dir, ".db"))
		})
	}
}

// A write-ahead-log the process may not write to is the sharpest statement of the contract:
// an immutable open must still read it, a writable open must fail on it.
func TestOpenImmutableBucketWithReadOnlyWAL(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the write permission this test relies on")
	}

	dir := seedBucketDir(t, [][]walEntry{{{"k1", "v1"}}}, 0)
	logs, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.NoError(t, os.Chmod(logs[0], 0o444))

	readValues(t, dir, map[string]string{"k1": "v1"}, WithImmutable(true))

	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()
	_, err = NewBucketCreator().NewBucket(ctx, dir, "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace))
	require.ErrorIs(t, err, os.ErrPermission)
}

// recordingCallbackGroup notes the ids registered on it and otherwise does nothing.
type recordingCallbackGroup struct {
	cyclemanager.CycleCallbackGroup
	registered []string
}

func (g *recordingCallbackGroup) Register(id string, cycleCallback cyclemanager.CycleCallback,
	options ...cyclemanager.RegisterOption,
) cyclemanager.CycleCallbackCtrl {
	g.registered = append(g.registered, id)
	return g.CycleCallbackGroup.Register(id, cycleCallback, options...)
}

// Compaction rewrites segment files, so an immutable bucket must not register for it even
// when the caller hands it a live compaction cycle.
func TestImmutableBucketRegistersNoCompaction(t *testing.T) {
	tests := []struct {
		name           string
		opts           []BucketOption
		wantRegistered int
	}{
		{name: "writable", wantRegistered: 1},
		{name: "immutable", opts: []BucketOption{WithImmutable(true)}, wantRegistered: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := seedBucketDir(t, [][]walEntry{{{"k1", "v1"}}}, 0)
			compaction := &recordingCallbackGroup{CycleCallbackGroup: cyclemanager.NewCallbackGroupNoop()}

			b, err := NewBucketCreator().NewBucket(ctx, dir, "", testLogger(), nil,
				compaction, cyclemanager.NewCallbackGroupNoop(),
				append([]BucketOption{WithStrategy(StrategyReplace)}, tt.opts...)...)
			require.NoError(t, err)
			require.NoError(t, b.Shutdown(ctx))

			require.Len(t, compaction.registered, tt.wantRegistered)
		})
	}
}

// Segment cleanup rewrites segment files and persists its progress in a bolt db next to
// them, so an immutable bucket must not start it even when the caller configures an
// interval.
func TestImmutableBucketRunsNoSegmentCleanup(t *testing.T) {
	tests := []struct {
		name          string
		opts          []BucketOption
		wantCleanupDb bool
	}{
		{name: "writable", wantCleanupDb: true},
		{name: "immutable", opts: []BucketOption{WithImmutable(true)}, wantCleanupDb: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := seedBucketDir(t, [][]walEntry{{{"k1", "v1"}}}, 0)

			opts := append(tt.opts, WithSegmentsCleanupInterval(time.Second))
			readValues(t, dir, map[string]string{"k1": "v1"}, opts...)

			_, err := os.Stat(filepath.Join(dir, cleanupDbFileName))
			if tt.wantCleanupDb {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, os.ErrNotExist)
			}
		})
	}
}

// A configured cleanup interval must not make an immutable open reach for write access on
// the bucket directory.
func TestOpenImmutableBucketWithCleanupIntervalOnReadOnlyDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the write permission this test relies on")
	}

	dir := seedBucketDir(t, [][]walEntry{{{"k1", "v1"}}}, 0)
	require.NoError(t, os.Chmod(dir, 0o500))
	t.Cleanup(func() { os.Chmod(dir, 0o700) })

	readValues(t, dir, map[string]string{"k1": "v1"},
		WithImmutable(true), WithSegmentsCleanupInterval(time.Second))
}

// readValues opens a bucket on dir, requires every wanted key to read back, and shuts it
// down again.
func readValues(t *testing.T, dir string, want map[string]string, opts ...BucketOption) {
	t.Helper()

	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	b, err := NewBucketCreator().NewBucket(ctx, dir, "", testLogger(), nil, noopCB, noopCB,
		append([]BucketOption{WithStrategy(StrategyReplace)}, opts...)...)
	require.NoError(t, err)

	for key, value := range want {
		got, err := b.Get([]byte(key))
		require.NoError(t, err)
		require.Equal(t, value, string(got), "key %q", key)
	}

	require.NoError(t, b.Shutdown(ctx))
}

// seedBucketDir builds a bucket directory holding one write-ahead-log per entry in logs, in
// the order given. An empty entry becomes a zero-length log. The last log is shortened by
// truncateLast bytes, which makes it end abruptly. No logs at all means the directory is
// never created.
func seedBucketDir(t *testing.T, logs [][]walEntry, truncateLast int) string {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "bucket")
	if len(logs) == 0 {
		return dir
	}
	require.NoError(t, os.MkdirAll(dir, 0o700))

	for i, entries := range logs {
		data := walBytes(t, entries)
		if i == len(logs)-1 && truncateLast > 0 {
			require.Greater(t, len(data), truncateLast)
			data = data[:len(data)-truncateLast]
		}
		// fixed width so the names sort chronologically, as real ones do
		name := fmt.Sprintf("segment-17000000000000000%02d.wal", i)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o600))
	}

	return dir
}

// walBytes returns the contents of a write-ahead-log holding the given puts.
func walBytes(t *testing.T, entries []walEntry) []byte {
	t.Helper()

	if len(entries) == 0 {
		return nil
	}

	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	dir := t.TempDir()
	// a threshold no log can reach keeps the log on shutdown instead of flushing a segment
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace), WithMinWalThreshold(1<<40))
	require.NoError(t, err)

	for _, e := range entries {
		require.NoError(t, b.Put([]byte(e.key), []byte(e.value)))
	}
	require.NoError(t, b.Shutdown(ctx))

	names, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	require.NoError(t, err)
	require.Len(t, names, 1)

	data, err := os.ReadFile(names[0])
	require.NoError(t, err)
	require.NotEmpty(t, data)

	return data
}

// dirSnapshot maps every file in dir to a hash of its contents. A directory that does not
// exist maps to nil.
func dirSnapshot(t *testing.T, dir string) map[string]string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)

	snapshot := map[string]string{}
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		snapshot[entry.Name()] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	return snapshot
}

func countFilesWithExt(t *testing.T, dir, ext string) int {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	count := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ext {
			count++
		}
	}
	return count
}
