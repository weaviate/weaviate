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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func mustNewColumnarBucketWithSchema(t *testing.T, ctx context.Context, dir string,
	schema *columnar.Schema, opts ...BucketOption,
) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	allOpts := append([]BucketOption{
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(schema),
	}, opts...)
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), allOpts...)
	require.Nil(t, err)
	t.Cleanup(func() {
		require.Nil(t, b.Shutdown(context.Background()))
	})
	return b
}

func mustNewColumnarBucket(t *testing.T, ctx context.Context, dir string,
	colType columnar.ColumnType, opts ...BucketOption,
) *Bucket {
	t.Helper()
	return mustNewColumnarBucketWithSchema(t, ctx, dir, &columnar.Schema{
		Columns: []columnar.Column{{Name: "val", Type: colType}},
	}, opts...)
}

func vectorTestSchema(dims uint32) *columnar.Schema {
	return &columnar.Schema{Columns: []columnar.Column{{
		Name: "vec", Type: columnar.ColumnTypeVector,
		Encoding: columnar.EncodingRawFixedWidth, Dims: dims,
	}}}
}

func multiVectorTestSchema(dims uint32) *columnar.Schema {
	return &columnar.Schema{Columns: []columnar.Column{{
		Name: "mv", Type: columnar.ColumnTypeMultiVector,
		Encoding: columnar.EncodingOffsetValues, Dims: dims,
	}}}
}

// mustGetVectorPayload wraps ColumnarGetVectorPayload, failing the test on
// the (invariant-violation) error path so call sites keep the two-value
// shape.
func mustGetVectorPayload(t *testing.T, b *Bucket, docID uint64, dst []byte) ([]byte, bool) {
	t.Helper()
	out, ok, err := b.ColumnarGetVectorPayload(docID, dst)
	require.NoError(t, err)
	return out, ok
}

// testVector returns a deterministic vector so update-wins tests can tell
// generations apart.
func testVector(dims int, seed uint64) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = float32(seed) + float32(i)*0.25
	}
	return v
}

// testMultiVector returns tokens vectors of dims each; tokens may be 0.
func testMultiVector(dims, tokens int, seed uint64) [][]float32 {
	vecs := make([][]float32, tokens)
	for tok := range vecs {
		vecs[tok] = make([]float32, dims)
		for i := range vecs[tok] {
			vecs[tok][i] = float32(seed) + float32(tok) + float32(i)*0.5
		}
	}
	return vecs
}

// flattenVectors concatenates a token matrix the way the bucket stores it.
func flattenVectors(vecs [][]float32) []float32 {
	var out []float32
	for _, v := range vecs {
		out = append(out, v...)
	}
	return out
}

func TestColumnarBucket_PutLookup(t *testing.T) {
	ctx := context.Background()

	t.Run("int64 memtable only", func(t *testing.T) {
		b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)

		require.Nil(t, b.ColumnarPutInt64(7, 0, -42))
		require.Nil(t, b.ColumnarPutInt64(9, 0, 100))

		v, ok, lerr := b.ColumnarLookupInt64(7, 0)
		require.NoError(t, lerr)
		require.True(t, ok)
		assert.Equal(t, int64(-42), v)

		_, ok, lerr = b.ColumnarLookupInt64(8, 0)
		require.NoError(t, lerr)
		assert.False(t, ok)
	})

	t.Run("float64 survives flush losslessly", func(t *testing.T) {
		b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeFloat64)

		// a value that float32 cannot represent exactly
		exact := 0.1234567890123456789
		require.Nil(t, b.ColumnarPutFloat64(1, 0, exact))
		require.Nil(t, b.FlushAndSwitch())

		v, ok, lerr := b.ColumnarLookupFloat64(1, 0)
		require.NoError(t, lerr)
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

		v, ok, lerr := b.ColumnarLookupInt64(1, 0)
		require.NoError(t, lerr)
		require.True(t, ok)
		assert.Equal(t, int64(2), v)
	})

	t.Run("delete hides flushed value", func(t *testing.T) {
		b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)

		require.Nil(t, b.ColumnarPutInt64(1, 0, 1))
		require.Nil(t, b.FlushAndSwitch())
		require.Nil(t, b.ColumnarDelete(1))

		_, ok, lerr := b.ColumnarLookupInt64(1, 0)
		require.NoError(t, lerr)
		assert.False(t, ok)

		// and across another flush (tombstone in segment)
		require.Nil(t, b.FlushAndSwitch())
		_, ok, lerr = b.ColumnarLookupInt64(1, 0)
		require.NoError(t, lerr)
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
		v, ok, lerr := b.ColumnarLookupInt64(docID, 0)
		require.NoError(t, lerr)
		require.True(t, ok, "docID %d", docID)
		assert.Equal(t, int64(docID*10), v, "docID %d", docID)
	}
	_, ok, lerr := b.ColumnarLookupInt64(n, 0)
	require.NoError(t, lerr)
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

		v, ok, lerr := b.ColumnarLookupInt64(1, 0)
		require.NoError(t, lerr)
		require.True(t, ok)
		assert.Equal(t, int64(-1), v)

		_, ok, lerr = b.ColumnarLookupInt64(2, 0)
		require.NoError(t, lerr)
		assert.False(t, ok)

		v, ok, lerr = b.ColumnarLookupInt64(n+5, 0)
		require.NoError(t, lerr)
		require.True(t, ok)
		assert.Equal(t, int64(555), v)

		v, ok, lerr = b.ColumnarLookupInt64(n-1, 0)
		require.NoError(t, lerr)
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
			v, ok, lerr := b.ColumnarLookupInt64(i, 0)
			require.NoError(t, lerr)
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

	v, ok, lerr := b2.ColumnarLookupFloat64(1, 0)
	require.NoError(t, lerr)
	require.True(t, ok)
	assert.Equal(t, exact, v)

	_, ok, lerr = b2.ColumnarLookupFloat64(2, 0)
	require.NoError(t, lerr)
	assert.False(t, ok, "tombstone must survive WAL recovery")
}

func TestColumnarBucket_VectorPutGet(t *testing.T) {
	ctx := context.Background()
	const dims = 16

	getVec := func(t *testing.T, b *Bucket, docID uint64) ([]float32, bool) {
		t.Helper()
		payload, ok := mustGetVectorPayload(t, b, docID, nil)
		if !ok {
			return nil, false
		}
		return BytesToFloat32s(payload, nil), true
	}

	t.Run("memtable roundtrip", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims))

		want := testVector(dims, 7)
		require.Nil(t, b.ColumnarPutVector(7, want))

		got, ok := getVec(t, b, 7)
		require.True(t, ok)
		assert.Equal(t, want, got)

		_, ok = mustGetVectorPayload(t, b, 8, nil)
		assert.False(t, ok, "missing docID must not be found")
	})

	t.Run("dims mismatch errors", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims))

		err := b.ColumnarPutVector(1, testVector(dims-1, 1))
		require.Error(t, err)
		assert.ErrorContains(t, err, "dims")

		err = b.ColumnarPutVector(1, testVector(dims+1, 1))
		require.Error(t, err)

		// nothing must have been written
		_, ok := mustGetVectorPayload(t, b, 1, nil)
		assert.False(t, ok)
	})

	t.Run("vector put on scalar bucket errors", func(t *testing.T) {
		b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)
		err := b.ColumnarPutVector(1, testVector(dims, 1))
		require.Error(t, err)
		assert.ErrorContains(t, err, "no vector column schema")
	})

	t.Run("multi-vector put on single-vector bucket errors", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims))
		err := b.ColumnarPutMultiVector(1, testMultiVector(dims, 2, 1))
		require.Error(t, err)
		assert.ErrorContains(t, err, "not multivector")
	})

	t.Run("survives flush", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims))

		// enough rows to span multiple vector blocks (256 rows each)
		n := uint64(columnar.VectorBlockSize*2 + 50)
		for i := uint64(0); i < n; i++ {
			require.Nil(t, b.ColumnarPutVector(i, testVector(dims, i)))
		}
		require.Nil(t, b.FlushAndSwitch())

		for _, docID := range []uint64{
			0, 1, columnar.VectorBlockSize - 1, columnar.VectorBlockSize,
			columnar.VectorBlockSize * 2, n - 1,
		} {
			got, ok := getVec(t, b, docID)
			require.True(t, ok, "docID %d", docID)
			assert.Equal(t, testVector(dims, docID), got, "docID %d", docID)
		}
		_, ok := mustGetVectorPayload(t, b, n, nil)
		assert.False(t, ok)
	})

	t.Run("update wins over flushed value", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims))

		require.Nil(t, b.ColumnarPutVector(1, testVector(dims, 100)))
		require.Nil(t, b.FlushAndSwitch())
		require.Nil(t, b.ColumnarPutVector(1, testVector(dims, 200)))

		got, ok := getVec(t, b, 1)
		require.True(t, ok)
		assert.Equal(t, testVector(dims, 200), got,
			"newer memtable payload must shadow the flushed older one")

		// and across another flush (both generations in segments)
		require.Nil(t, b.FlushAndSwitch())
		got, ok = getVec(t, b, 1)
		require.True(t, ok)
		assert.Equal(t, testVector(dims, 200), got)
	})

	t.Run("tombstone hides payload", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims))

		require.Nil(t, b.ColumnarPutVector(1, testVector(dims, 1)))
		require.Nil(t, b.FlushAndSwitch())
		require.Nil(t, b.ColumnarDelete(1))

		_, ok := mustGetVectorPayload(t, b, 1, nil)
		assert.False(t, ok, "memtable tombstone must hide the flushed payload")

		require.Nil(t, b.FlushAndSwitch())
		_, ok = mustGetVectorPayload(t, b, 1, nil)
		assert.False(t, ok, "segment tombstone must hide the older segment payload")
	})

	t.Run("compaction keeps newest payloads", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims),
			WithKeepTombstones(true))

		n := uint64(columnar.VectorBlockSize + 50) // multi-block output
		for i := uint64(0); i < n; i++ {
			require.Nil(t, b.ColumnarPutVector(i, testVector(dims, i)))
		}
		require.Nil(t, b.FlushAndSwitch())

		require.Nil(t, b.ColumnarPutVector(1, testVector(dims, 9001)))
		require.Nil(t, b.ColumnarDelete(2))
		require.Nil(t, b.ColumnarPutVector(n+5, testVector(dims, n+5)))
		require.Nil(t, b.FlushAndSwitch())

		compacted, err := b.disk.compactOnce(ctx)
		require.Nil(t, err)
		require.True(t, compacted)

		got, ok := getVec(t, b, 1)
		require.True(t, ok)
		assert.Equal(t, testVector(dims, 9001), got, "updated payload must win")

		_, ok = mustGetVectorPayload(t, b, 2, nil)
		assert.False(t, ok, "deleted payload must stay hidden after compaction")

		got, ok = getVec(t, b, n+5)
		require.True(t, ok)
		assert.Equal(t, testVector(dims, n+5), got)

		got, ok = getVec(t, b, n-1)
		require.True(t, ok)
		assert.Equal(t, testVector(dims, n-1), got, "untouched payload must survive")
	})
}

func TestColumnarBucket_MultiVectorPutGet(t *testing.T) {
	ctx := context.Background()
	const dims = 4

	// variable token counts, including zero-token docs
	tokensFor := func(docID uint64) int { return int(docID % 5) }

	getTokens := func(t *testing.T, b *Bucket, docID uint64) ([]float32, bool) {
		t.Helper()
		payload, ok := mustGetVectorPayload(t, b, docID, nil)
		if !ok {
			return nil, false
		}
		return BytesToFloat32s(payload, nil), true
	}

	assertDoc := func(t *testing.T, b *Bucket, docID uint64) {
		t.Helper()
		want := flattenVectors(testMultiVector(dims, tokensFor(docID), docID))
		got, ok := getTokens(t, b, docID)
		require.True(t, ok, "docID %d", docID)
		assert.Equal(t, len(want), len(got), "docID %d: token count", docID)
		if len(want) > 0 {
			assert.Equal(t, want, got, "docID %d", docID)
		}
	}

	t.Run("memtable roundtrip with variable token counts", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), multiVectorTestSchema(dims))

		for docID := uint64(0); docID < 10; docID++ {
			require.Nil(t, b.ColumnarPutMultiVector(docID,
				testMultiVector(dims, tokensFor(docID), docID)))
		}
		for docID := uint64(0); docID < 10; docID++ {
			assertDoc(t, b, docID)
		}
		_, ok := mustGetVectorPayload(t, b, 10, nil)
		assert.False(t, ok)
	})

	t.Run("token dims mismatch errors", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), multiVectorTestSchema(dims))

		vecs := testMultiVector(dims, 3, 1)
		vecs[1] = vecs[1][:dims-1] // corrupt one token
		err := b.ColumnarPutMultiVector(1, vecs)
		require.Error(t, err)
		assert.ErrorContains(t, err, "dims")

		_, ok := mustGetVectorPayload(t, b, 1, nil)
		assert.False(t, ok, "failed put must not write")
	})

	t.Run("single-vector put on multi-vector bucket errors", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), multiVectorTestSchema(dims))
		err := b.ColumnarPutVector(1, testVector(dims, 1))
		require.Error(t, err)
		assert.ErrorContains(t, err, "not vector")
	})

	t.Run("survives flush and compaction", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), multiVectorTestSchema(dims),
			WithKeepTombstones(true))

		n := uint64(columnar.VectorBlockSize + 20) // multi-block segment
		for docID := uint64(0); docID < n; docID++ {
			require.Nil(t, b.ColumnarPutMultiVector(docID,
				testMultiVector(dims, tokensFor(docID), docID)))
		}
		require.Nil(t, b.FlushAndSwitch())

		for _, docID := range []uint64{
			0, 1, columnar.VectorBlockSize - 1, columnar.VectorBlockSize, n - 1,
		} {
			assertDoc(t, b, docID)
		}

		// second segment: update (different token count), delete, add
		updated := testMultiVector(dims, 7, 9001)
		require.Nil(t, b.ColumnarPutMultiVector(1, updated))
		require.Nil(t, b.ColumnarDelete(2))
		require.Nil(t, b.ColumnarPutMultiVector(n+3,
			testMultiVector(dims, tokensFor(n+3), n+3)))
		require.Nil(t, b.FlushAndSwitch())

		compacted, err := b.disk.compactOnce(ctx)
		require.Nil(t, err)
		require.True(t, compacted)

		got, ok := getTokens(t, b, 1)
		require.True(t, ok)
		assert.Equal(t, flattenVectors(updated), got,
			"updated token matrix (different token count) must win")

		_, ok = mustGetVectorPayload(t, b, 2, nil)
		assert.False(t, ok, "deleted doc must stay hidden after compaction")

		assertDoc(t, b, n+3)
		assertDoc(t, b, n-1)

		// zero-token doc survives the whole flush+compaction cycle
		require.Equal(t, 0, tokensFor(5), "fixture: docID 5 must be a zero-token doc")
		payload, ok := mustGetVectorPayload(t, b, 5, nil)
		require.True(t, ok, "zero-token doc must still be found")
		assert.Empty(t, payload)
	})
}

func TestColumnarBucket_VectorWALRecovery(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	const dims = 16

	t.Run("single vector", func(t *testing.T) {
		dir := t.TempDir()
		opts := []BucketOption{
			WithStrategy(StrategyColumnar),
			WithColumnarSchema(vectorTestSchema(dims)),
		}

		b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		require.Nil(t, b.ColumnarPutVector(1, testVector(dims, 1)))
		require.Nil(t, b.ColumnarPutVector(2, testVector(dims, 2)))
		require.Nil(t, b.ColumnarDelete(2))
		require.Nil(t, b.ColumnarPutVector(3, testVector(dims, 3)))
		require.Nil(t, b.ColumnarPutVector(3, testVector(dims, 33))) // update, last write wins
		// no flush — shutdown leaves the WAL behind for recovery
		require.Nil(t, b.Shutdown(ctx))

		b2, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)
		t.Cleanup(func() {
			require.Nil(t, b2.Shutdown(context.Background()))
		})

		payload, ok := mustGetVectorPayload(t, b2, 1, nil)
		require.True(t, ok)
		assert.Equal(t, testVector(dims, 1), BytesToFloat32s(payload, nil),
			"payload must survive WAL recovery intact")

		_, ok = mustGetVectorPayload(t, b2, 2, nil)
		assert.False(t, ok, "tombstone must survive WAL recovery")

		payload, ok = mustGetVectorPayload(t, b2, 3, nil)
		require.True(t, ok)
		assert.Equal(t, testVector(dims, 33), BytesToFloat32s(payload, nil),
			"latest update must win after WAL recovery")
	})

	t.Run("multi vector", func(t *testing.T) {
		dir := t.TempDir()
		opts := []BucketOption{
			WithStrategy(StrategyColumnar),
			WithColumnarSchema(multiVectorTestSchema(dims)),
		}

		b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		threeTokens := testMultiVector(dims, 3, 1)
		require.Nil(t, b.ColumnarPutMultiVector(1, threeTokens))
		require.Nil(t, b.ColumnarPutMultiVector(2, testMultiVector(dims, 0, 2))) // zero tokens
		require.Nil(t, b.Shutdown(ctx))

		b2, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)
		t.Cleanup(func() {
			require.Nil(t, b2.Shutdown(context.Background()))
		})

		payload, ok := mustGetVectorPayload(t, b2, 1, nil)
		require.True(t, ok)
		assert.Equal(t, flattenVectors(threeTokens), BytesToFloat32s(payload, nil))

		payload, ok = mustGetVectorPayload(t, b2, 2, nil)
		require.True(t, ok, "zero-token doc must survive WAL recovery")
		assert.Empty(t, payload)
	})
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
	h2, n, version, err := columnar.UnmarshalHeader(buf)
	require.Nil(t, err)
	assert.Equal(t, len(buf), n)
	assert.Equal(t, uint8(2), version, "MarshalBinary always writes format v2")
	assert.Equal(t, h.Schema, h2.Schema)

	entries := []columnar.DirectoryEntry{
		{
			StartDocID: 5, EndDocID: 1000, Offset: 123, RowCount: 17, LiveCount: 16,
			Stats: []columnar.ColStats{
				{Min: 1, Max: 99},
				{Min: math.Float64bits(-1.5), Max: math.Float64bits(2.5)},
			},
			Pages: []columnar.ColPage{
				{Offset: 17 * 9, Size: 17 * 8},
				{Offset: 17*9 + 17*8, Size: 17 * 8},
			},
		},
		{
			StartDocID: 1001, EndDocID: 2000, Offset: 456, RowCount: 3, LiveCount: 0,
			Stats: []columnar.ColStats{{}, {}},
			Pages: []columnar.ColPage{{Offset: 27, Size: 24}, {Offset: 51, Size: 24}},
		},
	}
	dirBuf := columnar.MarshalDirectory(entries, 2)
	entries2, err := columnar.UnmarshalDirectory(dirBuf, &h.Schema, version)
	require.Nil(t, err)
	assert.Equal(t, entries, entries2)

	_, err = columnar.UnmarshalDirectory(dirBuf[:len(dirBuf)-1], &h.Schema, version)
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
	require.Error(t, b.ColumnarPutVector(1, []float32{1}),
		"vector put must fail on a bucket without a vector schema")
}

// TestColumnarBucket_NilSchemaFailsLoud pins that every columnar op on a
// bucket created WITHOUT WithColumnarSchema returns a clear error instead
// of silently returning empty results (reads) or a generic memtable error
// (writes). A schema-less columnar bucket can occur when bucket loading
// runs without the per-property options (e.g. a missing property schema).
func TestColumnarBucket_NilSchemaFailsLoud(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar))
	require.Nil(t, err)
	t.Cleanup(func() {
		require.Nil(t, b.Shutdown(context.Background()))
	})

	assert.ErrorContains(t, b.ColumnarPutInt64(1, 0, 1), "no columnar schema")
	assert.ErrorContains(t, b.ColumnarPutFloat64(1, 0, 1), "no columnar schema")
	assert.ErrorContains(t, b.ColumnarDelete(1), "no columnar schema")
	_, _, err = b.ColumnarLookupBits(1, 0)
	assert.ErrorContains(t, err, "no columnar schema")
	assert.ErrorContains(t,
		b.ColumnarScan(0, nil, func(uint64, uint64) bool { return true }),
		"no columnar schema")
}

// TestColumnarBucket_ColIdxOutOfRange pins that an out-of-range column
// index surfaces as an error from every columnar op, not as an
// index-out-of-range panic deep inside the memtable or block reader.
func TestColumnarBucket_ColIdxOutOfRange(t *testing.T) {
	ctx := context.Background()
	b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)
	require.Nil(t, b.ColumnarPutInt64(1, 0, 42))

	for _, colIdx := range []int{-1, 1, 5} {
		assert.ErrorContains(t, b.ColumnarPutInt64(1, colIdx, 1), "out of range")
		assert.ErrorContains(t, b.ColumnarPutFloat64(1, colIdx, 1), "out of range")
		_, _, err := b.ColumnarLookupBits(1, colIdx)
		assert.ErrorContains(t, err, "out of range")
		assert.ErrorContains(t,
			b.ColumnarScan(colIdx, nil, func(uint64, uint64) bool { return true }),
			"out of range")
	}
}

// guards against accidentally reintroducing a partial-width scalar type
func TestColumnarTypes_AllEightBytes(t *testing.T) {
	for _, ct := range []columnar.ColumnType{
		columnar.ColumnTypeInt64, columnar.ColumnTypeFloat64,
	} {
		col := columnar.Column{Type: ct}
		assert.Equal(t, 8, col.Width(), fmt.Sprintf("type %s", ct))
	}
}

// TestColumnarBucket_ConcurrentScanAndWrite pins the memtable scan/write
// race: scans must materialize row state under the lock, because writers
// mutate row.values and row.live in place. Run with -race.
func TestColumnarBucket_ConcurrentScanAndWrite(t *testing.T) {
	ctx := context.Background()
	b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)

	for i := uint64(0); i < 512; i++ {
		require.Nil(t, b.ColumnarPutInt64(i, 0, int64(i)))
	}

	done := make(chan struct{})
	writerStopped := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(writerStopped)
		for i := uint64(0); ; i++ {
			select {
			case <-done:
				return
			default:
			}
			docID := i % 512
			if i%17 == 0 {
				assert.Nil(t, b.ColumnarDelete(docID))
			} else {
				assert.Nil(t, b.ColumnarPutInt64(docID, 0, int64(i)))
			}
		}
	}, nil)

	for n := 0; n < 200; n++ {
		err := b.ColumnarScan(0, nil, func(uint64, uint64) bool { return true })
		require.Nil(t, err)
	}

	close(done)
	<-writerStopped
}

// TestColumnarBucket_CompactionThenScan runs multiple flush+compact cycles
// with a mix of inserts, updates, and deletes, and asserts after every cycle
// that both the scan and the point-lookup paths match an in-memory model.
// Covers both tombstone modes: keepTombstones=false lets the bottom-level
// compaction clean tombstones (deleted rows must NOT resurrect), while
// keepTombstones=true preserves them across compactions.
func TestColumnarBucket_CompactionThenScan(t *testing.T) {
	ctx := context.Background()

	for _, keepTombstones := range []bool{true, false} {
		t.Run(fmt.Sprintf("keepTombstones=%v", keepTombstones), func(t *testing.T) {
			var opts []BucketOption
			if keepTombstones {
				opts = append(opts, WithKeepTombstones(true))
			}
			b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64, opts...)

			model := map[uint64]int64{}
			deleted := map[uint64]struct{}{}

			const cycles = 4
			const writesPerCycle = 1500
			const docIDSpace = 4000 // > BlockSize so segments span multiple blocks

			for cycle := 0; cycle < cycles; cycle++ {
				for i := 0; i < writesPerCycle; i++ {
					docID := uint64((cycle*977 + i*13) % docIDSpace)
					if (cycle+i)%5 == 4 {
						require.NoError(t, b.ColumnarDelete(docID))
						delete(model, docID)
						deleted[docID] = struct{}{}
					} else {
						v := int64(cycle*1_000_000 + i)
						require.NoError(t, b.ColumnarPutInt64(docID, 0, v))
						model[docID] = v
						delete(deleted, docID)
					}
				}
				require.NoError(t, b.FlushAndSwitch())

				for {
					compacted, err := b.disk.compactOnce(ctx)
					require.NoError(t, err)
					if !compacted {
						break
					}
				}

				got := map[uint64]int64{}
				require.NoError(t, b.ColumnarScan(0, nil, func(docID uint64, bits uint64) bool {
					_, dup := got[docID]
					require.Falsef(t, dup, "cycle %d: docID %d visited twice", cycle, docID)
					got[docID] = int64(bits)
					return true
				}))
				require.Equalf(t, model, got, "cycle %d: scan diverges from model", cycle)
			}

			// Point lookups agree with the model after the final compaction.
			for docID, want := range model {
				v, ok, err := b.ColumnarLookupInt64(docID, 0)
				require.NoError(t, err)
				require.Truef(t, ok, "docID %d must be found", docID)
				require.Equalf(t, want, v, "docID %d", docID)
			}
			// Deleted rows stay deleted — in the cleanup mode the tombstones
			// themselves are gone from the bottom segment, but the docIDs
			// must not resurrect.
			for docID := range deleted {
				_, ok, err := b.ColumnarLookupInt64(docID, 0)
				require.NoError(t, err)
				require.Falsef(t, ok, "deleted docID %d must stay deleted after compaction", docID)
			}
		})
	}
}

// TestColumnarMemtable_SizeAccounting pins the memtable size accounting for
// fresh puts: Size() must land within 10% of N*(columnarRowOverhead+RowWidth)
// so the size-based flush threshold trips at realistic memory use.
func TestColumnarMemtable_SizeAccounting(t *testing.T) {
	ctx := context.Background()
	b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)

	const n = 1000
	for i := uint64(0); i < n; i++ {
		require.NoError(t, b.ColumnarPutInt64(i, 0, int64(i)))
	}

	rowWidth := b.ColumnarSchema().RowWidth()
	expected := float64(n * (columnarRowOverhead + uint64(rowWidth)))
	got := float64(b.active.Size())
	assert.InDelta(t, expected, got, 0.1*expected,
		"memtable Size() after %d fresh puts must be within 10%% of N*(%d+%d), got %.0f",
		n, columnarRowOverhead, rowWidth, got)
}

// TestColumnarBucket_RejectsLazySegmentLoading pins the construction-time
// guard: the columnar read paths type-assert every segment-group element to
// a fully loaded *segment, so the lazy-loading option must be rejected at
// NewBucket instead of surfacing later as a read error.
func TestColumnarBucket_RejectsLazySegmentLoading(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	_, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(&columnar.Schema{
			Columns: []columnar.Column{{Name: "val", Type: columnar.ColumnTypeInt64}},
		}),
		WithLazySegmentLoading(true))
	require.ErrorContains(t, err, "lazy segment loading")
}

// TestColumnarBucket_CountAsyncPanics pins the strategy guard on CountAsync:
// it sums per-segment countNetAdditions, which only replace segments compute,
// so calling it on a columnar bucket must panic (cursor-constructor contract)
// rather than silently report 0.
func TestColumnarBucket_CountAsyncPanics(t *testing.T) {
	ctx := context.Background()
	b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)
	require.Panics(t, func() { _ = b.CountAsync() })
}

// TestColumnarBucket_CleanupIntervalInit pins that a columnar bucket with
// segmentsCleanupInterval > 0 initializes (newSegmentCleaner must return the
// noop cleaner for StrategyColumnar instead of "unrecognized strategy").
func TestColumnarBucket_CleanupIntervalInit(t *testing.T) {
	ctx := context.Background()
	b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64,
		WithSegmentsCleanupInterval(time.Hour))

	require.NoError(t, b.ColumnarPutInt64(1, 0, 42))
	v, ok, err := b.ColumnarLookupInt64(1, 0)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, int64(42), v)
}

// TestColumnarBucket_PrependRequiresKeepTombstones pins the prepend guard:
// a columnar group that does not keep tombstones may already have cleaned
// deletes in a bottom-level compaction, so prepending older segments below
// it would resurrect deleted docIDs and must be rejected. With tombstones
// kept (the enable-columnar migration's ingest-bucket configuration), the
// prepend is allowed.
func TestColumnarBucket_PrependRequiresKeepTombstones(t *testing.T) {
	ctx := context.Background()

	t.Run("rejected without keepTombstones", func(t *testing.T) {
		b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)
		err := b.PrependSegmentsFromBucket(ctx, t.TempDir())
		require.ErrorContains(t, err, "resurrect")
	})

	t.Run("allowed with keepTombstones", func(t *testing.T) {
		b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64,
			WithKeepTombstones(true))
		// empty source dir → no-op, but the strategy gate must pass
		require.NoError(t, b.PrependSegmentsFromBucket(ctx, t.TempDir()))
	})
}
