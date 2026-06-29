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

// TestBucket_WithTransformerBuilder_OpensAndClosesEditOps proves the option wires
// the edit-ops sidecar at bucket init, that the bolt file is materialized lazily
// on the first registered op (not at init), and that shutdown closes the handle
// (the lsmkv-owned C9 lifecycle).
func TestBucket_WithTransformerBuilder_OpensAndClosesEditOps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	builder := func(ops []ActiveOp) func([]byte) ([]byte, error) {
		return func(v []byte) ([]byte, error) { return v, nil }
	}

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithTransformerBuilder(builder))
	require.NoError(t, err)
	require.NotNil(t, bucket.disk.editOps, "WithTransformerBuilder should wire the edit-ops sidecar")

	editOpsDir := bucket.disk.dir
	// Lazy: no bolt file until an op is registered.
	require.NoFileExists(t, filepath.Join(editOpsDir, segmentEditOpsFileName))
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.FileExists(t, filepath.Join(editOpsDir, segmentEditOpsFileName))

	require.NoError(t, bucket.Shutdown(ctx))

	// Re-opening the (now-existing) file takes an exclusive bolt lock, so this
	// succeeds only if shutdown closed the first handle.
	reopened := newSegmentEditOps(editOpsDir, nil)
	_, err = reopened.LoadOps()
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

// TestWithTransformerBuilder_RejectsNonReplace pins the fail-fast replace-only
// guard: wiring the option on any non-replace bucket errors at setup, across
// every other strategy.
func TestWithTransformerBuilder_RejectsNonReplace(t *testing.T) {
	builder := func(ops []ActiveOp) func([]byte) ([]byte, error) {
		return func(v []byte) ([]byte, error) { return v, nil }
	}

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

			_, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithTransformerBuilder(builder))
			require.Error(t, err)
			require.Contains(t, err.Error(), "replace")
		})
	}
}

// TestBucket_NoTransformerBuilder_NoEditOps confirms a bucket without the option
// keeps no edit-ops sidecar (no per-bucket bolt file when the feature is unused).
func TestBucket_NoTransformerBuilder_NoEditOps(t *testing.T) {
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
