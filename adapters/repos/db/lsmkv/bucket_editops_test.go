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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBucket_EditOps_WiredByClassNameOnReplace proves a non-empty className on a
// replace bucket enables the edit-ops sidecar, that the bolt file is materialized
// lazily on the first registered op (not at init), and that shutdown closes the
// handle (the lsmkv-owned lifecycle).
func TestBucket_EditOps_WiredByClassNameOnReplace(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithClassName("MyClass"))
	require.NoError(t, err)
	require.NotNil(t, bucket.disk.editOps, "className on a replace bucket should wire the edit-ops sidecar")

	editOpsDir := bucket.disk.dir
	// Lazy: no bolt file until an op is registered.
	require.NoFileExists(t, filepath.Join(editOpsDir, segmentEditOpsFileName))
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.FileExists(t, filepath.Join(editOpsDir, segmentEditOpsFileName))

	require.NoError(t, bucket.Shutdown(ctx))

	// Re-opening the (now-existing) file takes an exclusive bolt lock, so this
	// succeeds only if shutdown closed the first handle.
	reopened := newSegmentEditOps(editOpsDir, "")
	_, err = reopened.LoadOps()
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

// TestBucket_EditOps_NotWiredWithoutClassName confirms a replace bucket without a
// className keeps no edit-ops sidecar (only the objects bucket sets className).
func TestBucket_EditOps_NotWiredWithoutClassName(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	require.Nil(t, bucket.disk.editOps)
}

// TestBucket_EditOps_NotWiredOnNonReplace confirms the sidecar is gated to the
// replace strategy: even with a className, a non-replace bucket gets no edit-ops
// facility (edit ops only apply to the objects store).
func TestBucket_EditOps_NotWiredOnNonReplace(t *testing.T) {
	for _, strategy := range []string{
		StrategySetCollection,
		StrategyMapCollection,
		StrategyRoaringSet,
		StrategyInverted,
	} {
		t.Run(strategy, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			logger, _ := test.NewNullLogger()

			bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithClassName("MyClass"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

			require.Nil(t, bucket.disk.editOps)
		})
	}
}
