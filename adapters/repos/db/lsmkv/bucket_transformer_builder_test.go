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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBucket_WithTransformerBuilder_OpensAndClosesEditOps proves the option
// opens the edit-ops sidecar at bucket init and that shutdown closes it (the
// lsmkv-owned C9 lifecycle).
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
	require.NotNil(t, bucket.disk.editOps, "WithTransformerBuilder should open the edit-ops sidecar")

	require.NoError(t, bucket.Shutdown(ctx))

	// Re-opening the same file takes an exclusive bolt lock, so it succeeds only
	// if shutdown closed the first handle.
	reopened, err := openSegmentEditOps(dir, nil)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

// TestWithTransformerBuilder_RejectsNonReplace pins the fail-fast replace-only
// guard: wiring the option on a non-replace bucket errors at setup.
func TestWithTransformerBuilder_RejectsNonReplace(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	builder := func(ops []ActiveOp) func([]byte) ([]byte, error) {
		return func(v []byte) ([]byte, error) { return v, nil }
	}

	_, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategySetCollection), WithTransformerBuilder(builder))
	require.Error(t, err)
	require.Contains(t, err.Error(), "replace")
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
