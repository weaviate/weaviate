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

package compressionhelpers

import (
	"context"
	"encoding/binary"
	stderrors "errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// newTestBucketReplace creates a real lsmkv.Bucket backed by t.TempDir() using
// StrategyReplace. The bucket is closed via t.Cleanup so callers do not need
// to manage its lifetime explicitly.
func newTestBucketReplace(t *testing.T, opts ...lsmkv.BucketOption) *lsmkv.Bucket {
	t.Helper()
	dir := t.TempDir()
	noop := cyclemanager.NewCallbackGroupNoop()
	allOpts := append([]lsmkv.BucketOption{lsmkv.WithStrategy(lsmkv.StrategyReplace)}, opts...)
	b, err := lsmkv.NewBucketCreator().NewBucket(
		context.Background(), dir, dir, logrus.New(), nil, noop, noop, allOpts...)
	require.NoError(t, err)
	t.Cleanup(func() { b.Shutdown(context.Background()) }) //nolint:errcheck
	return b
}

// newTestSQCompressor builds a minimal quantizedVectorsCompressor[byte] backed
// by a ScalarQuantizer. The store and cache fields are left nil because
// recoverCompressedVector uses only the bucket argument and the vectorForID
// callback.
func newTestSQCompressor(t *testing.T, vectorForID func(ctx context.Context, id uint64) ([]float32, error)) *quantizedVectorsCompressor[byte] {
	t.Helper()
	// a=1.0, b=-1.0, dimensions=2 produces a valid SQ for our 2-dim test vector.
	sq, err := RestoreScalarQuantizer(1.0, -1.0, 2, distancer.NewCosineDistanceProvider())
	require.NoError(t, err)
	return &quantizedVectorsCompressor[byte]{
		quantizer:    sq,
		logger:       logrus.New(),
		targetVector: "test-vector",
		storeId:      binary.LittleEndian.PutUint64,
		vectorForID:  vectorForID,
	}
}

// rawVecRecover is the test vector used across all three cases.
var rawVecRecover = []float32{0.5, -0.5}

// recoveryIDBytes encodes id=1 as a little-endian byte slice, matching storeId above.
func recoveryIDBytes(id uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, id)
	return b
}

// TestRecoverCompressedVector_BucketReadOnly_ReturnsVectorAndNilError verifies
// that when the bucket is in READONLY state, recoverCompressedVector returns
// the encoded vector with a nil error (the write-back is skipped).
func TestRecoverCompressedVector_BucketReadOnly_ReturnsVectorAndNilError(t *testing.T) {
	bucket := newTestBucketReplace(t)
	bucket.UpdateStatus(storagestate.StatusReadOnly)

	vectorForID := func(_ context.Context, _ uint64) ([]float32, error) {
		return rawVecRecover, nil
	}
	compressor := newTestSQCompressor(t, vectorForID)

	compressed, err := compressor.recoverCompressedVector(context.Background(), 1, recoveryIDBytes(1), bucket)

	require.NoError(t, err, "read-only bucket write-back must not surface an error to the caller")
	assert.NotEmpty(t, compressed, "encoded vector must be returned even when bucket is read-only")
}

// TestRecoverCompressedVector_BucketWritable_PersistsAndReturns verifies the
// success path: the encoded vector is persisted to the bucket and returned.
// A subsequent bucket.Get must find the persisted bytes, regression-guarding
// that the writable path was not accidentally broken by the guard.
func TestRecoverCompressedVector_BucketWritable_PersistsAndReturns(t *testing.T) {
	bucket := newTestBucketReplace(t)

	vectorForID := func(_ context.Context, _ uint64) ([]float32, error) {
		return rawVecRecover, nil
	}
	compressor := newTestSQCompressor(t, vectorForID)

	id := uint64(42)
	idBytes := recoveryIDBytes(id)

	compressed, err := compressor.recoverCompressedVector(context.Background(), id, idBytes, bucket)

	require.NoError(t, err)
	assert.NotEmpty(t, compressed)

	// The bytes must have been written to the bucket.
	got, err := bucket.Get(idBytes)
	require.NoError(t, err, "bucket.Get after recoverCompressedVector should not fail")
	assert.NotEmpty(t, got, "persisted bytes should be present in the bucket after recovery")
}

// TestRecoverCompressedVector_GenuineIOError_PropagatesWrapped pins the
// invariant that only storagestate.ErrStatusReadOnly is swallowed. Any other
// bucket error (here lsmkv.ErrImmutable from a WithImmutable bucket) must be
// wrapped and propagated to the caller unchanged.
func TestRecoverCompressedVector_GenuineIOError_PropagatesWrapped(t *testing.T) {
	// WithImmutable(true) causes bucket.Put to return lsmkv.ErrImmutable, which
	// is a structurally distinct error from storagestate.ErrStatusReadOnly.
	// The guard must NOT swallow it.
	bucket := newTestBucketReplace(t, lsmkv.WithImmutable(true))

	vectorForID := func(_ context.Context, _ uint64) ([]float32, error) {
		return rawVecRecover, nil
	}
	compressor := newTestSQCompressor(t, vectorForID)

	compressed, err := compressor.recoverCompressedVector(context.Background(), 1, recoveryIDBytes(1), bucket)

	require.Error(t, err, "non-read-only bucket errors must propagate to the caller")
	assert.Nil(t, compressed, "no compressed vector should be returned on a propagated error")
	assert.False(t, stderrors.Is(err, storagestate.ErrStatusReadOnly),
		"propagated error must not be ErrStatusReadOnly (would mean wrong swallow path was taken)")
}
