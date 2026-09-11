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

package shardusage

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// dimRow is one <name><LE uint32 dims> key and the docIDs stored under it.
type dimRow struct {
	name   string
	dims   uint32
	docIDs []uint64
}

func dimKey(name string, dims uint32) []byte {
	key := make([]byte, len(name)+4)
	copy(key, name)
	binary.LittleEndian.PutUint32(key[len(name):], dims)
	return key
}

// seedDimRows writes rows exactly as Shard.addToDimensionBucket does, so the
// two strategies are exercised through the same layout production writes.
func seedDimRows(t *testing.T, b *lsmkv.Bucket, rows []dimRow) {
	t.Helper()
	for _, r := range rows {
		key := dimKey(r.name, r.dims)
		for _, docID := range r.docIDs {
			switch b.Strategy() {
			case lsmkv.StrategyMapCollection:
				mapKey := make([]byte, 8)
				binary.LittleEndian.PutUint64(mapKey, docID)
				require.NoError(t, b.MapSet(key, lsmkv.MapPair{Key: mapKey, Value: []byte{}}))
			default:
				require.NoError(t, b.RoaringSetAddOne(key, docID))
			}
		}
	}
}

// TestRemoveTargetVectorDimensions drives the deletion directly, on both bucket
// strategies. The shard's own store is StrategyRoaringSet on 1.34+, so a test
// that reaches this through a shard cannot cover the StrategyMapCollection arm
// at all — and that arm carries its own copy of the key-length guard.
func TestRemoveTargetVectorDimensions(t *testing.T) {
	const dims = 8

	tests := []struct {
		name   string
		seed   []dimRow
		remove string
		// want maps a vector name to the doc count still readable under it.
		want map[string]int
	}{
		{
			name: "removes the target and leaves an unrelated sibling",
			seed: []dimRow{
				{name: "keep", dims: dims, docIDs: []uint64{1, 2, 3}},
				{name: "drop", dims: dims, docIDs: []uint64{4, 5}},
			},
			remove: "drop",
			want:   map[string]int{"keep": 3, "drop": 0},
		},
		{
			name: "a longer name carrying the target as a prefix survives",
			seed: []dimRow{
				{name: "vec", dims: dims, docIDs: []uint64{1, 2}},
				{name: "vec_extra", dims: dims, docIDs: []uint64{3, 4, 5}},
			},
			remove: "vec",
			want:   map[string]int{"vec": 0, "vec_extra": 3},
		},
		{
			name: "the target itself may be the longer name",
			seed: []dimRow{
				{name: "vec", dims: dims, docIDs: []uint64{1, 2}},
				{name: "vec_extra", dims: dims, docIDs: []uint64{3, 4, 5}},
			},
			remove: "vec_extra",
			want:   map[string]int{"vec": 2, "vec_extra": 0},
		},
		{
			name: "the unnamed vector takes only the 4-byte keys",
			seed: []dimRow{
				{name: "", dims: dims, docIDs: []uint64{1, 2}},
				{name: "named", dims: dims, docIDs: []uint64{3, 4, 5}},
			},
			remove: "",
			want:   map[string]int{"": 0, "named": 3},
		},
		{
			name: "a named vector leaves the unnamed one alone",
			seed: []dimRow{
				{name: "", dims: dims, docIDs: []uint64{1, 2}},
				{name: "named", dims: dims, docIDs: []uint64{3, 4, 5}},
			},
			remove: "named",
			want:   map[string]int{"": 2, "named": 0},
		},
		{
			name: "every dimensionality the target recorded goes",
			seed: []dimRow{
				{name: "drop", dims: 4, docIDs: []uint64{1}},
				{name: "drop", dims: dims, docIDs: []uint64{2, 3}},
				{name: "keep", dims: dims, docIDs: []uint64{4}},
			},
			remove: "drop",
			want:   map[string]int{"drop": 0, "keep": 1},
		},
		{
			name:   "a name that recorded nothing is not an error",
			seed:   []dimRow{{name: "keep", dims: dims, docIDs: []uint64{1, 2}}},
			remove: "absent",
			want:   map[string]int{"keep": 2},
		},
	}

	// flushed decides whether the rows are on disk when the removal runs. It
	// matters: a memtable-only row never reaches the segment read path, which is
	// where an unloaded bucket has no bitmap buffer pool to read through.
	for _, flushed := range []bool{false, true} {
		for _, strategy := range []string{lsmkv.StrategyRoaringSet, lsmkv.StrategyMapCollection} {
			for _, tc := range tests {
				t.Run(fmt.Sprintf("flushed=%v/%s/%s", flushed, strategy, tc.name), func(t *testing.T) {
					ctx := t.Context()
					dir := filepath.Join(t.TempDir(), "dimensions")
					logger := logrus.New()
					open := func() *lsmkv.Bucket {
						b, err := lsmkv.NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
							cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
							lsmkv.WithStrategy(strategy))
						require.NoError(t, err)
						return b
					}
					assertCounts := func(t *testing.T, b *lsmkv.Bucket, when string) {
						t.Helper()
						for name, want := range tc.want {
							scan, err := ScanTargetVectorDimensions(ctx, b, name, 0)
							require.NoError(t, err)
							require.Equal(t, want, scan.Raw.Count,
								"%s: doc count under %q after removing %q", when, name, tc.remove)
						}
					}

					b := open()
					seedDimRows(t, b, tc.seed)
					if flushed {
						require.NoError(t, b.FlushMemtable())
					}
					require.NoError(t, RemoveTargetVectorDimensions(ctx, b, tc.remove))
					assertCounts(t, b, "in memory")

					// Reopened from disk: a delete that lives only in the
					// memtable would read as gone here and come back on the
					// next restart.
					require.NoError(t, b.FlushMemtable())
					require.NoError(t, b.Shutdown(ctx))

					reopened := open()
					defer reopened.Shutdown(ctx)
					assertCounts(t, reopened, "after flush and reopen")
				})
			}
		}
	}
}
