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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func newInterlockTestBucket(t *testing.T, dir string) (*Bucket, error) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	return NewBucketCreator().NewBucket(context.Background(), dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSegmentsCleanupInterval(time.Hour),
		WithClassName("TestClass"))
}

// TestRegisterEditOp_RefusedOnClosingBucket pins the arm-time interlock: once
// a bucket's shutdown began, registering a NEW edit op must hard-fail — the
// flush + segment snapshot would race the dismantling and could record
// "nothing to clean" for a bucket full of data.
func TestRegisterEditOp_RefusedOnClosingBucket(t *testing.T) {
	bucket, _ := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))

	bucket.shuttingDown.Store(true)
	err := bucket.RegisterEditOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "shutting down")
}

// TestWALRecovery_FlushesLastWALUnderLiveOps pins the resurrection fix: data
// recovered from the last WAL used to become the live memtable — pre-strip
// bytes OUTSIDE the pending-segment bookkeeping, resurrecting a completed
// drop's data with nothing left to re-clean it. With ops in the sidecar,
// recovery must flush every WAL into segments so the sidecar re-snapshot
// covers them.
func TestWALRecovery_FlushesLastWALUnderLiveOps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	b1, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err)
	// Small enough for Shutdown's flushWAL path, which KEEPS the WAL on disk —
	// the exact shape an interrupted close leaves behind.
	require.NoError(t, b1.Put([]byte("k1"), []byte("v1")))

	// Register the op through a separate sidecar handle: going through the
	// bucket would FlushAndSwitch and drain the very WAL this test needs to
	// survive the close.
	ext := newSegmentEditOps(dir, "TestClass")
	require.NoError(t, ext.RegisterOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, ext.Close())
	require.NoError(t, b1.Shutdown(ctx))

	b2, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, b2.Shutdown(ctx)) }()

	require.NotNil(t, b2.disk.editOps)
	pending, err := b2.disk.editOps.Pending("op1")
	require.NoError(t, err)
	require.NotEmpty(t, pending,
		"WAL-recovered data must land in segments re-pended under the surviving op, not in the live memtable")

	v, err := b2.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v, "recovered data must still be readable after the flush")
}

// TestNewBucket_FailsWhenSidecarLocked pins the reload fence: a sidecar still
// flocked by a previous instance means that instance has not finished closing
// — loading blind here (the old soft-skip logged "cleanup stalled") is how a
// completed drop lost its healing re-pend. The load must fail retryably.
func TestNewBucket_FailsWhenSidecarLocked(t *testing.T) {
	dir := t.TempDir()

	ext := newSegmentEditOps(dir, "TestClass")
	require.NoError(t, ext.RegisterOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	defer ext.Close() // holds the bolt flock for the duration of the test

	_, err := newInterlockTestBucket(t, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "still locked by a previous instance")
}
