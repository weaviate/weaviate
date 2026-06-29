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
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func newReplaceBucketForCleanup(t *testing.T, transformer valueTransformer) *Bucket {
	t.Helper()
	ctx := context.Background()
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSegmentsCleanupInterval(time.Second))
	require.NoError(t, err)
	bucket.SetMemtableThreshold(1e9)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	// Inject the transformer via the edit-ops path: a registered op makes
	// BuildCurrentTransformer dispatch to this type's factory per pass.
	if transformer != nil {
		editOps := newSegmentEditOps(bucket.disk.dir, map[OpType]OpTransformerFactory{
			OpTypeRemoveTargetVectors: func(ops []ActiveOp) func([]byte) ([]byte, error) { return transformer },
		})
		t.Cleanup(func() { require.NoError(t, editOps.Close()) })
		bucket.disk.editOps = editOps
		require.NoError(t, editOps.RegisterOp("test-op",
			OpDescriptor{Type: "remove_target_vectors", CreatedAt: 1}))
	}
	return bucket
}

// TestSegmentCleanerReplaceValueTransformer verifies the cleaner mirrors the
// compactor hook: a value that survives cleaning is rewritten through the
// transformer, while a surviving tombstone bypasses it.
func TestSegmentCleanerReplaceValueTransformer(t *testing.T) {
	transformer := func(value []byte) ([]byte, error) {
		if len(value) == 0 {
			return nil, errors.New("transformer called with empty value")
		}
		return append([]byte("X:"), value...), nil
	}

	bucket := newReplaceBucketForCleanup(t, transformer)

	// Segment 1 (oldest): a survivor, a tombstone, and a key shadowed by seg 2.
	require.NoError(t, bucket.Put([]byte("keep"), []byte("v1")))
	require.NoError(t, bucket.Delete([]byte("gone")))
	require.NoError(t, bucket.Put([]byte("shadow"), []byte("old")))
	require.NoError(t, bucket.FlushAndSwitch())

	// Segment 2: shadows "shadow" so the cleaner has something to drop from seg 1.
	require.NoError(t, bucket.Put([]byte("shadow"), []byte("new")))
	require.NoError(t, bucket.FlushAndSwitch())

	// Clean the oldest segment. Succeeds only if the tombstone never reached the
	// transformer (it errors on empty input).
	cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
	require.NoError(t, err)
	require.True(t, cleaned)

	// The surviving live value was transformed exactly once.
	keep, err := bucket.Get([]byte("keep"))
	require.NoError(t, err)
	require.Equal(t, []byte("X:v1"), keep)

	// The tombstone stayed a tombstone (not resurrected, not transformed).
	gone, err := bucket.Get([]byte("gone"))
	require.NoError(t, err)
	require.Nil(t, gone)

	// The shadowed key resolves to the newer, untouched segment.
	shadow, err := bucket.Get([]byte("shadow"))
	require.NoError(t, err)
	require.Equal(t, []byte("new"), shadow)
}

// TestSegmentCleanerReplaceValueTransformerNil confirms cleaning is unaffected
// when no transformer is set.
func TestSegmentCleanerReplaceValueTransformerNil(t *testing.T) {
	bucket := newReplaceBucketForCleanup(t, nil)

	require.NoError(t, bucket.Put([]byte("keep"), []byte("v1")))
	require.NoError(t, bucket.Put([]byte("shadow"), []byte("old")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("shadow"), []byte("new")))
	require.NoError(t, bucket.FlushAndSwitch())

	cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
	require.NoError(t, err)
	require.True(t, cleaned)

	keep, err := bucket.Get([]byte("keep"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), keep)
}
