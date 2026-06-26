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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func newReplaceBucketForCompaction(t *testing.T, transformer valueTransformer) *Bucket {
	t.Helper()
	ctx := context.Background()
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithForceCompaction(true))
	require.NoError(t, err)
	bucket.SetMemtableThreshold(1e9)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	// Inject the transformer via the edit-ops path: a registered op makes
	// BuildCurrentTransformer return the builder's transformer per pass.
	if transformer != nil {
		editOps := newSegmentEditOps(bucket.disk.dir,
			func(ops []ActiveOp) func([]byte) ([]byte, error) { return transformer })
		t.Cleanup(func() { require.NoError(t, editOps.Close()) })
		bucket.disk.editOps = editOps
		require.NoError(t, editOps.RegisterOp("test-op",
			OpDescriptor{Type: "remove_target_vectors", CreatedAt: 1}))
	}
	return bucket
}

func compactAll(t *testing.T, bucket *Bucket) {
	t.Helper()
	compacted, err := bucket.disk.compactOnce(context.Background())
	for ; err == nil && compacted; compacted, err = bucket.disk.compactOnce(context.Background()) {
	}
	require.NoError(t, err)
}

// TestCompactorReplaceValueTransformer verifies the compaction transformer hook:
// live values are rewritten through the transformer, while tombstones (no
// payload) bypass it.
func TestCompactorReplaceValueTransformer(t *testing.T) {
	// Prefixes every value it sees; errors if ever handed an empty payload so a
	// tombstone leaking into the transformer would fail the compaction.
	transformer := func(value []byte) ([]byte, error) {
		if len(value) == 0 {
			return nil, errors.New("transformer called with empty value")
		}
		return append([]byte("X:"), value...), nil
	}

	bucket := newReplaceBucketForCompaction(t, transformer)

	// Segment 1.
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.Put([]byte("doomed"), []byte("v-doomed")))
	require.NoError(t, bucket.FlushAndSwitch())

	// Segment 2: a fresh key plus a tombstone for the key from segment 1.
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.Delete([]byte("doomed")))
	require.NoError(t, bucket.FlushAndSwitch())

	compactAll(t, bucket)

	// Live values were transformed exactly once.
	v1, err := bucket.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("X:v1"), v1)

	v2, err := bucket.Get([]byte("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("X:v2"), v2)

	// The tombstoned key stays deleted; the transformer never saw it (otherwise
	// compaction would have failed on the empty payload).
	deleted, err := bucket.Get([]byte("doomed"))
	require.NoError(t, err)
	require.Nil(t, deleted)
}

// TestCompactorReplaceValueTransformerNil confirms compaction is unaffected when
// no transformer is set (the default).
func TestCompactorReplaceValueTransformerNil(t *testing.T) {
	bucket := newReplaceBucketForCompaction(t, nil)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())

	compactAll(t, bucket)

	v1, err := bucket.Get([]byte("k1"))
	require.NoError(t, err)
	require.True(t, bytes.Equal([]byte("v1"), v1))
}

// TestCompactorReplaceValueTransformerError confirms a failing transformer
// aborts the compaction without data loss: the original segments stay in place
// (no half-written merged segment is swapped in) and their values are readable
// and untransformed.
func TestCompactorReplaceValueTransformerError(t *testing.T) {
	transformer := func(value []byte) ([]byte, error) {
		return nil, errors.New("boom")
	}
	bucket := newReplaceBucketForCompaction(t, transformer)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.Len(t, bucket.disk.segments, 2)

	compacted, err := bucket.disk.compactOnce(context.Background())
	require.Error(t, err)
	require.False(t, compacted)

	// Both original segments survive; nothing was swapped in.
	require.Len(t, bucket.disk.segments, 2)

	v1, err := bucket.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v1)
	v2, err := bucket.Get([]byte("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), v2)
}
