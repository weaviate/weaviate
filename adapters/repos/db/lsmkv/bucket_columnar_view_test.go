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
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/usecases/byteops"
)

// platformSupportsZeroCopy reports whether this build can reinterpret
// little-endian payload bytes as []float32 (true on all LE architectures,
// false on the BE fallback build).
func platformSupportsZeroCopy() bool {
	_, ok := byteops.Float32sFromBytesZeroCopy(make([]byte, 4))
	return ok
}

func TestColumnarBucket_GetVectorFloatsWithView(t *testing.T) {
	ctx := context.Background()
	const dims = 16

	t.Run("memtable rows are copied, never aliased", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims))

		want := testVector(dims, 7)
		require.NoError(t, b.ColumnarPutVector(7, want))

		view := b.GetConsistentView()
		defer view.ReleaseView()

		got, aliased, found, err := b.ColumnarGetVectorFloatsWithView(view, 7, nil)
		require.NoError(t, err)
		require.True(t, found)
		assert.False(t, aliased, "memtable-resident rows must be decoded by copy")
		assert.Equal(t, want, got)

		_, _, found, err = b.ColumnarGetVectorFloatsWithView(view, 8, nil)
		require.NoError(t, err)
		assert.False(t, found, "missing docID must not be found")
	})

	t.Run("segment rows alias the pinned mmap", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims))

		n := uint64(columnar.VectorBlockSize + 50) // multi-block segment
		for i := uint64(0); i < n; i++ {
			require.NoError(t, b.ColumnarPutVector(i, testVector(dims, i)))
		}
		require.NoError(t, b.FlushAndSwitch())

		view := b.GetConsistentView()
		defer view.ReleaseView()

		for _, docID := range []uint64{0, 1, columnar.VectorBlockSize - 1, columnar.VectorBlockSize, n - 1} {
			got, aliased, found, err := b.ColumnarGetVectorFloatsWithView(view, docID, nil)
			require.NoError(t, err)
			require.True(t, found, "docID %d", docID)
			assert.Equal(t, testVector(dims, docID), got, "docID %d", docID)

			if !platformSupportsZeroCopy() {
				assert.False(t, aliased)
				continue
			}
			require.True(t, aliased,
				"docID %d: disk-segment hit must alias the mmap on little-endian builds "+
					"(implies the 64-byte absolute payload alignment held)", docID)
			// the unsafe *float32 reinterpretation requires 4-byte alignment
			assert.Zero(t, uintptr(unsafe.Pointer(&got[0]))%4,
				"docID %d: aliased payload must be at least 4-byte aligned", docID)

			// aliasing proof: a second read with a fresh dst returns the
			// exact same backing address — both point into the same mmap
			again, aliased2, found2, err := b.ColumnarGetVectorFloatsWithView(view, docID, nil)
			require.NoError(t, err)
			require.True(t, found2)
			require.True(t, aliased2)
			assert.Equal(t, unsafe.Pointer(&got[0]), unsafe.Pointer(&again[0]),
				"docID %d: repeated zero-copy reads must return the same mmap address", docID)
		}
	})

	t.Run("newest-wins equivalence with the copy-based API", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims))

		// generation 1: docIDs 0..9 in a segment
		for i := uint64(0); i < 10; i++ {
			require.NoError(t, b.ColumnarPutVector(i, testVector(dims, i)))
		}
		require.NoError(t, b.FlushAndSwitch())
		// generation 2: update 1, delete 2 in a second segment
		require.NoError(t, b.ColumnarPutVector(1, testVector(dims, 9001)))
		require.NoError(t, b.ColumnarDelete(2))
		require.NoError(t, b.FlushAndSwitch())
		// generation 3: update 3, delete 4, create 100 in the memtable
		require.NoError(t, b.ColumnarPutVector(3, testVector(dims, 9003)))
		require.NoError(t, b.ColumnarDelete(4))
		require.NoError(t, b.ColumnarPutVector(100, testVector(dims, 100)))

		view := b.GetConsistentView()
		defer view.ReleaseView()

		for docID := uint64(0); docID <= 100; docID++ {
			wantVec, wantFound, err := b.ColumnarGetVectorFloats(docID, nil)
			require.NoError(t, err)

			gotVec, _, gotFound, err := b.ColumnarGetVectorFloatsWithView(view, docID, nil)
			require.NoError(t, err)
			require.Equal(t, wantFound, gotFound, "docID %d: found mismatch", docID)
			if wantFound {
				assert.Equal(t, wantVec, gotVec, "docID %d: value mismatch", docID)
			}
		}
	})

	t.Run("tombstones hide rows across generations", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims))

		require.NoError(t, b.ColumnarPutVector(1, testVector(dims, 1)))
		require.NoError(t, b.ColumnarPutVector(2, testVector(dims, 2)))
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.ColumnarDelete(1)) // memtable tombstone over segment row
		require.NoError(t, b.ColumnarDelete(2))
		require.NoError(t, b.FlushAndSwitch()) // segment tombstone over segment row

		view := b.GetConsistentView()
		defer view.ReleaseView()

		for _, docID := range []uint64{1, 2} {
			_, _, found, err := b.ColumnarGetVectorFloatsWithView(view, docID, nil)
			require.NoError(t, err)
			assert.False(t, found, "docID %d: tombstoned row must not be found", docID)
		}
	})

	t.Run("multi-vector flat payload aliases the mmap", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), multiVectorTestSchema(dims))

		want := testMultiVector(dims, 4, 1)
		require.NoError(t, b.ColumnarPutMultiVector(1, want))
		require.NoError(t, b.FlushAndSwitch())

		view := b.GetConsistentView()
		defer view.ReleaseView()

		flat, aliased, found, err := b.ColumnarGetVectorFloatsWithView(view, 1, nil)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, flattenVectors(want), flat)
		if platformSupportsZeroCopy() {
			assert.True(t, aliased)
			assert.Zero(t, uintptr(unsafe.Pointer(&flat[0]))%4)
		}
	})

	t.Run("alias stays valid across a concurrent compaction", func(t *testing.T) {
		b := mustNewColumnarBucketWithSchema(t, ctx, t.TempDir(), vectorTestSchema(dims),
			WithKeepTombstones(true))

		n := uint64(columnar.VectorBlockSize + 50)
		for i := uint64(0); i < n; i++ {
			require.NoError(t, b.ColumnarPutVector(i, testVector(dims, i)))
		}
		require.NoError(t, b.FlushAndSwitch())
		require.NoError(t, b.ColumnarPutVector(1, testVector(dims, 9001)))
		require.NoError(t, b.ColumnarDelete(2))
		require.NoError(t, b.FlushAndSwitch())

		// pin the pre-compaction segments
		view := b.GetConsistentView()

		const probe = uint64(5)
		got, aliased, found, err := b.ColumnarGetVectorFloatsWithView(view, probe, nil)
		require.NoError(t, err)
		require.True(t, found)
		if platformSupportsZeroCopy() {
			require.True(t, aliased)
		}
		want := testVector(dims, probe)
		require.Equal(t, want, got)

		// compaction replaces both segments; the replaced ones are parked in
		// segmentsAwaitingDrop because the view still holds references, so
		// their mmaps must stay valid
		compacted, err := b.disk.compactOnce(ctx)
		require.NoError(t, err)
		require.True(t, compacted)

		assert.Equal(t, want, got,
			"aliased slice must still read the original values while the view is held")

		// the pinned view keeps serving reads from the replaced segments
		again, _, found, err := b.ColumnarGetVectorFloatsWithView(view, probe, nil)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, want, again)

		// after release the parked segments may be munmapped at any time;
		// `got` must not be read past this point
		view.ReleaseView()

		// a fresh view serves the same data from the compacted segment
		view2 := b.GetConsistentView()
		defer view2.ReleaseView()
		fresh, _, found, err := b.ColumnarGetVectorFloatsWithView(view2, probe, nil)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, want, fresh)
	})

	t.Run("wrong strategy errors", func(t *testing.T) {
		b := mustNewColumnarBucket(t, ctx, t.TempDir(), columnar.ColumnTypeInt64)
		// scalar columnar bucket: strategy matches, lookups simply miss
		view := b.GetConsistentView()
		defer view.ReleaseView()
		_, _, found, err := b.ColumnarGetVectorFloatsWithView(view, 1, nil)
		require.NoError(t, err)
		assert.False(t, found)
	})
}
