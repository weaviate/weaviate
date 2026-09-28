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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBucketShutdownStoresMemtableNetCount pins that the sidecars a bucket
// leaves on shutdown add up to its object count, whether the shutdown flushes
// the memtable into a segment or keeps it as a WAL. An unloaded shard is
// counted from those sidecars alone.
func TestBucketShutdownStoresMemtableNetCount(t *testing.T) {
	tests := []struct {
		name          string
		reuseWAL      bool
		writeMetadata bool
	}{
		{name: "flushed segment", reuseWAL: false},
		{name: "flushed segment next to metadata files", reuseWAL: false, writeMetadata: true},
		{name: "reused WAL", reuseWAL: true},
		{name: "reused WAL next to metadata files", reuseWAL: true, writeMetadata: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			walThreshold := int64(0)
			if tt.reuseWAL {
				walThreshold = 1 << 30
			}
			open := func() *Bucket {
				noopCB := cyclemanager.NewCallbackGroupNoop()
				b, err := NewBucketCreator().NewBucket(ctx, dir, "", testLogger(), nil, noopCB, noopCB,
					WithStrategy(StrategyReplace),
					WithCalcCountNetAdditions(true),
					WithWriteMetadata(tt.writeMetadata),
					WithMinWalThreshold(walThreshold),
				)
				require.NoError(t, err)
				return b
			}

			b := open()
			for i := range 10 {
				require.NoError(t, b.Put(countTestKey(i), []byte("v1")))
			}
			require.NoError(t, b.FlushAndSwitch())

			// 5 updates and 5 new keys, 3 deletes of keys on disk, 1 of a key
			// that never existed
			for i := 5; i < 15; i++ {
				require.NoError(t, b.Put(countTestKey(i), []byte("v2")))
			}
			for i := range 3 {
				require.NoError(t, b.Delete(countTestKey(i)))
			}
			require.NoError(t, b.Delete(countTestKey(100)))
			const want = 12

			count, err := b.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, want, count)
			require.NoError(t, b.Shutdown(ctx))
			require.Equal(t, want, sidecarCount(t, dir))

			// the load replays a reused WAL into a memtable that takes more
			// writes, so the WAL's sidecar has to go
			b = open()
			count, err = b.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, want, count)
			if tt.reuseWAL {
				require.Equal(t, 10, sidecarCount(t, dir))
			} else {
				require.Equal(t, want, sidecarCount(t, dir))
			}

			require.NoError(t, b.Put(countTestKey(20), []byte("v1")))
			require.NoError(t, b.Shutdown(ctx))
			require.Equal(t, want+1, sidecarCount(t, dir))
		})
	}
}

func TestBucketShutdownStoresNoNetCount(t *testing.T) {
	tests := []struct {
		name      string
		calcCount bool
		writes    int
	}{
		{name: "bucket that does not count", calcCount: false, writes: 5},
		{name: "empty memtable", calcCount: true, writes: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			noopCB := cyclemanager.NewCallbackGroupNoop()
			b, err := NewBucketCreator().NewBucket(ctx, dir, "", testLogger(), nil, noopCB, noopCB,
				WithStrategy(StrategyReplace),
				WithCalcCountNetAdditions(tt.calcCount),
				WithMinWalThreshold(0),
			)
			require.NoError(t, err)

			for i := range tt.writes {
				require.NoError(t, b.Put(countTestKey(i), []byte("v1")))
			}
			require.NoError(t, b.Shutdown(ctx))

			sidecars, err := filepath.Glob(filepath.Join(dir, "*"+CountNetAdditionsFileSuffix))
			require.NoError(t, err)
			require.Empty(t, sidecars)
		})
	}
}

func countTestKey(i int) []byte {
	return fmt.Appendf(nil, "key-%03d", i)
}

// sidecarCount sums the object counts of all count sidecars in a bucket
// directory, the way an unloaded shard is counted.
func sidecarCount(t *testing.T, dir string) int {
	t.Helper()

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	total := int64(0)
	for _, e := range entries {
		path := filepath.Join(dir, e.Name())
		switch {
		case strings.HasSuffix(e.Name(), CountNetAdditionsFileSuffix):
			n, err := ReadCountNetAdditionsFile(path)
			require.NoError(t, err)
			total += n
		case strings.HasSuffix(e.Name(), MetadataFileSuffix):
			n, err := ReadObjectCountFromMetadataFile(path)
			require.NoError(t, err)
			total += n
		}
	}
	return int(total)
}
