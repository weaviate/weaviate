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
	"math"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func mustNewColumnarBucket(t *testing.T, ctx context.Context, dir string,
	colType columnar.ColumnType, opts ...BucketOption,
) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	allOpts := append([]BucketOption{
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(&columnar.Schema{
			Columns: []columnar.Column{{Name: "val", Type: colType}},
		}),
	}, opts...)
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), allOpts...)
	require.Nil(t, err)
	t.Cleanup(func() {
		require.Nil(t, b.Shutdown(context.Background()))
	})
	return b
}

func TestColumnarBucket_PutLookup(t *testing.T) {
	ctx := context.Background()

	t.Run("int64 memtable only", func(t *testing.T) {
		b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)

		require.Nil(t, b.ColumnarPutInt64(7, 0, -42))
		require.Nil(t, b.ColumnarPutInt64(9, 0, 100))

		v, ok := b.ColumnarLookupInt64(7, 0)
		require.True(t, ok)
		assert.Equal(t, int64(-42), v)

		_, ok = b.ColumnarLookupInt64(8, 0)
		assert.False(t, ok)
	})

	t.Run("float64 survives flush losslessly", func(t *testing.T) {
		b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeFloat64)

		// a value that float32 cannot represent exactly
		exact := 0.1234567890123456789
		require.Nil(t, b.ColumnarPutFloat64(1, 0, exact))
		require.Nil(t, b.FlushAndSwitch())

		v, ok := b.ColumnarLookupFloat64(1, 0)
		require.True(t, ok)
		assert.Equal(t, exact, v, "float64 must be stored losslessly")
		assert.NotEqual(t, float64(float32(exact)), v,
			"sanity: the test value must actually exercise float64 precision")
	})

	t.Run("update wins over flushed value", func(t *testing.T) {
		b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)

		require.Nil(t, b.ColumnarPutInt64(1, 0, 1))
		require.Nil(t, b.FlushAndSwitch())
		require.Nil(t, b.ColumnarPutInt64(1, 0, 2))

		v, ok := b.ColumnarLookupInt64(1, 0)
		require.True(t, ok)
		assert.Equal(t, int64(2), v)
	})

	t.Run("delete hides flushed value", func(t *testing.T) {
		b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)

		require.Nil(t, b.ColumnarPutInt64(1, 0, 1))
		require.Nil(t, b.FlushAndSwitch())
		require.Nil(t, b.ColumnarDelete(1))

		_, ok := b.ColumnarLookupInt64(1, 0)
		assert.False(t, ok)

		// and across another flush (tombstone in segment)
		require.Nil(t, b.FlushAndSwitch())
		_, ok = b.ColumnarLookupInt64(1, 0)
		assert.False(t, ok)
	})
}

func TestColumnarBucket_MultiBlockSegments(t *testing.T) {
	ctx := context.Background()
	b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)

	// 3 full blocks plus a partial one
	n := uint64(columnar.BlockSize*3 + 17)
	for i := uint64(0); i < n; i++ {
		require.Nil(t, b.ColumnarPutInt64(i, 0, int64(i*10)))
	}
	require.Nil(t, b.FlushAndSwitch())

	// spot-check lookups across block boundaries
	for _, docID := range []uint64{
		0, 1, columnar.BlockSize - 1, columnar.BlockSize,
		columnar.BlockSize * 2, columnar.BlockSize*3 + 16,
	} {
		v, ok := b.ColumnarLookupInt64(docID, 0)
		require.True(t, ok, "docID %d", docID)
		assert.Equal(t, int64(docID*10), v, "docID %d", docID)
	}
	_, ok := b.ColumnarLookupInt64(n, 0)
	assert.False(t, ok)

	// full scan sees every row exactly once
	seen := make(map[uint64]int64, n)
	require.Nil(t, b.ColumnarScan(0, nil, func(docID uint64, bits uint64) bool {
		_, dup := seen[docID]
		require.False(t, dup, "docID %d visited twice", docID)
		seen[docID] = int64(bits)
		return true
	}))
	require.Len(t, seen, int(n))
	assert.Equal(t, int64(420), seen[42])
}

func TestColumnarBucket_ScanSemantics(t *testing.T) {
	ctx := context.Background()
	b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)

	// segment 1: docIDs 0..9 = 1
	for i := uint64(0); i < 10; i++ {
		require.Nil(t, b.ColumnarPutInt64(i, 0, 1))
	}
	require.Nil(t, b.FlushAndSwitch())

	// segment 2: docID 3 updated, docID 4 deleted
	require.Nil(t, b.ColumnarPutInt64(3, 0, 99))
	require.Nil(t, b.ColumnarDelete(4))
	require.Nil(t, b.FlushAndSwitch())

	// memtable: docID 5 updated, docID 6 deleted, docID 100 added
	require.Nil(t, b.ColumnarPutInt64(5, 0, 50))
	require.Nil(t, b.ColumnarDelete(6))
	require.Nil(t, b.ColumnarPutInt64(100, 0, 7))

	t.Run("newest wins, tombstones hide", func(t *testing.T) {
		got := map[uint64]int64{}
		require.Nil(t, b.ColumnarScan(0, nil, func(docID uint64, bits uint64) bool {
			got[docID] = int64(bits)
			return true
		}))
		want := map[uint64]int64{
			0: 1, 1: 1, 2: 1, 3: 99, 5: 50, 7: 1, 8: 1, 9: 1, 100: 7,
		}
		assert.Equal(t, want, got)
	})

	t.Run("allow list filters", func(t *testing.T) {
		allow := sroar.NewBitmap()
		allow.Set(3)
		allow.Set(4) // deleted — must not appear
		allow.Set(100)

		got := map[uint64]int64{}
		require.Nil(t, b.ColumnarScan(0, allow, func(docID uint64, bits uint64) bool {
			got[docID] = int64(bits)
			return true
		}))
		assert.Equal(t, map[uint64]int64{3: 99, 100: 7}, got)
	})

	t.Run("early stop", func(t *testing.T) {
		count := 0
		require.Nil(t, b.ColumnarScan(0, nil, func(uint64, uint64) bool {
			count++
			return count < 3
		}))
		assert.Equal(t, 3, count)
	})
}

func TestColumnarBucket_Compaction(t *testing.T) {
	ctx := context.Background()

	newBucketForCompaction := func(t *testing.T) *Bucket {
		return mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64,
			WithKeepTombstones(true))
	}

	t.Run("merge with updates and tombstones", func(t *testing.T) {
		b := newBucketForCompaction(t)

		n := uint64(columnar.BlockSize + 100) // force multi-block output
		for i := uint64(0); i < n; i++ {
			require.Nil(t, b.ColumnarPutInt64(i, 0, int64(i)))
		}
		require.Nil(t, b.FlushAndSwitch())

		require.Nil(t, b.ColumnarPutInt64(1, 0, -1))
		require.Nil(t, b.ColumnarDelete(2))
		require.Nil(t, b.ColumnarPutInt64(n+5, 0, 555))
		require.Nil(t, b.FlushAndSwitch())

		compacted, err := b.disk.compactOnce(ctx)
		require.Nil(t, err)
		require.True(t, compacted)

		v, ok := b.ColumnarLookupInt64(1, 0)
		require.True(t, ok)
		assert.Equal(t, int64(-1), v)

		_, ok = b.ColumnarLookupInt64(2, 0)
		assert.False(t, ok)

		v, ok = b.ColumnarLookupInt64(n+5, 0)
		require.True(t, ok)
		assert.Equal(t, int64(555), v)

		v, ok = b.ColumnarLookupInt64(n-1, 0)
		require.True(t, ok)
		assert.Equal(t, int64(n-1), v)
	})

	t.Run("repeated compaction converges", func(t *testing.T) {
		b := newBucketForCompaction(t)

		for seg := 0; seg < 4; seg++ {
			for i := uint64(0); i < 1000; i++ {
				require.Nil(t, b.ColumnarPutInt64(i, 0, int64(seg)))
			}
			require.Nil(t, b.FlushAndSwitch())
		}

		for {
			compacted, err := b.disk.compactOnce(ctx)
			require.Nil(t, err)
			if !compacted {
				break
			}
		}

		for i := uint64(0); i < 1000; i++ {
			v, ok := b.ColumnarLookupInt64(i, 0)
			require.True(t, ok, "docID %d", i)
			require.Equal(t, int64(3), v, "docID %d must hold newest value", i)
		}
	})
}

func TestColumnarBucket_WALRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	schema := &columnar.Schema{
		Columns: []columnar.Column{{Name: "val", Type: columnar.ColumnTypeFloat64}},
	}
	opts := []BucketOption{
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(schema),
	}

	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)

	exact := 1.000000059604644775390625 // 1 + 2^-24, collapses to 1.0 in float32
	require.Nil(t, b.ColumnarPutFloat64(1, 0, exact))
	require.Nil(t, b.ColumnarPutFloat64(2, 0, 2.5))
	require.Nil(t, b.ColumnarDelete(2))
	// no flush — shutdown leaves the WAL behind for recovery
	require.Nil(t, b.Shutdown(ctx))

	b2, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.Nil(t, err)
	t.Cleanup(func() {
		require.Nil(t, b2.Shutdown(context.Background()))
	})

	v, ok := b2.ColumnarLookupFloat64(1, 0)
	require.True(t, ok)
	assert.Equal(t, exact, v)

	_, ok = b2.ColumnarLookupFloat64(2, 0)
	assert.False(t, ok, "tombstone must survive WAL recovery")
}

func TestColumnarBlockWriterReader_Roundtrip(t *testing.T) {
	schema := &columnar.Schema{
		Columns: []columnar.Column{{Name: "val", Type: columnar.ColumnTypeInt64}},
	}

	var blocks [][]byte
	var entries []columnar.DirectoryEntry
	bw := columnar.NewBlockWriter(schema, 0, func(block []byte, e columnar.DirectoryEntry) error {
		cp := make([]byte, len(block))
		copy(cp, block)
		blocks = append(blocks, cp)
		entries = append(entries, e)
		return nil
	})

	rows := columnar.BlockSize + 3
	buf := make([]byte, 8)
	for i := 0; i < rows; i++ {
		columnar.EncodeInt64(buf, 0, int64(i-5)) // include negatives
		require.Nil(t, bw.Append(uint64(i*2), i%7 != 0, buf))
	}
	require.Nil(t, bw.Flush())

	require.Len(t, entries, 2)
	assert.Equal(t, uint32(columnar.BlockSize), entries[0].RowCount)
	assert.Equal(t, uint32(3), entries[1].RowCount)
	assert.Equal(t, uint64(0), entries[0].StartDocID)
	assert.Equal(t, uint64((columnar.BlockSize-1)*2), entries[0].EndDocID)

	// stats cover live rows only: row 0 (value -5) is a tombstone
	assert.Equal(t, int64(-4), int64(entries[0].Stats[0].Min))

	t.Run("out of order append fails", func(t *testing.T) {
		bw2 := columnar.NewBlockWriter(schema, 0, func([]byte, columnar.DirectoryEntry) error {
			return nil
		})
		require.Nil(t, bw2.Append(10, true, buf))
		err := bw2.Append(10, true, buf)
		require.Error(t, err)
	})

	t.Run("read back", func(t *testing.T) {
		// entries hold absolute offsets; rebuild a contiguous buffer
		var contents []byte
		for _, blk := range blocks {
			contents = append(contents, blk...)
		}
		for i := range entries {
			br, err := columnar.NewBlockReader(schema, &entries[i], contents)
			require.Nil(t, err)
			for r := 0; r < br.Rows(); r++ {
				globalRow := i*columnar.BlockSize + r
				assert.Equal(t, uint64(globalRow*2), br.DocIDAt(r))
				assert.Equal(t, globalRow%7 != 0, br.IsLive(r))
				assert.Equal(t, int64(globalRow-5), int64(br.ValueBitsAt(0, r)))
			}
		}
	})
}

func TestColumnarHeaderDirectory_Roundtrip(t *testing.T) {
	h := &columnar.Header{
		Schema: columnar.Schema{
			Columns: []columnar.Column{
				{Name: "a", Type: columnar.ColumnTypeInt64},
				{Name: "b", Type: columnar.ColumnTypeFloat64},
			},
		},
	}
	buf := h.MarshalBinary()
	h2, n, err := columnar.UnmarshalHeader(buf)
	require.Nil(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, h.Schema, h2.Schema)

	entries := []columnar.DirectoryEntry{
		{
			StartDocID: 5, EndDocID: 1000, Offset: 123, RowCount: 17, LiveCount: 16,
			Stats: []columnar.ColStats{
				{Min: 1, Max: 99},
				{Min: math.Float64bits(-1.5), Max: math.Float64bits(2.5)},
			},
		},
		{
			StartDocID: 1001, EndDocID: 2000, Offset: 456, RowCount: 3, LiveCount: 0,
			Stats: []columnar.ColStats{{}, {}},
		},
	}
	dirBuf := columnar.MarshalDirectory(entries, 2)
	entries2, err := columnar.UnmarshalDirectory(dirBuf, 2)
	require.Nil(t, err)
	assert.Equal(t, entries, entries2)

	_, err = columnar.UnmarshalDirectory(dirBuf[:len(dirBuf)-1], 2)
	require.Error(t, err)
}

func TestColumnarBucket_StrategyMismatch(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.Nil(t, err)
	t.Cleanup(func() {
		require.Nil(t, b.Shutdown(context.Background()))
	})

	require.Error(t, b.ColumnarPutInt64(1, 0, 1))
	require.Error(t, b.ColumnarDelete(1))
	require.Error(t, b.ColumnarScan(0, nil, func(uint64, uint64) bool { return true }))
}

// guards against accidentally reintroducing a partial-width type
func TestColumnarTypes_AllEightBytes(t *testing.T) {
	for _, ct := range []columnar.ColumnType{
		columnar.ColumnTypeInt64, columnar.ColumnTypeFloat64,
	} {
		assert.Equal(t, 8, ct.Width(), fmt.Sprintf("type %s", ct))
	}
}
