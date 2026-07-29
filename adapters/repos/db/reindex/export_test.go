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

package reindex

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// Test-only handles onto unexported internals, for the reindex_test
// files in this directory that need a real *db.Shard and therefore
// cannot live in package reindex. Compiled only into the test binary,
// so the production API stays exactly as narrow as it is.

var (
	ResolveDoubleWriteBucket       = resolveDoubleWriteBucket
	ResolveScopedDoubleWriteBucket = resolveScopedDoubleWriteBucket
	MaybeWirePerPropOverlaySet     = maybeWirePerPropOverlaySet
)

// Accessors for the task's function-valued seams. These stay unexported
// in production — main had them package-private and only a same-package
// test reached them; the package split must not turn them into API that
// an outside importer can swap the close-lock guard through.

func (t *ShardReindexTaskGeneric) SetOnPropSwapped(f func(propName string)) {
	t.onPropSwapped = f
}

func (t *ShardReindexTaskGeneric) OnPropSwapped() func(propName string) {
	return t.onPropSwapped
}

func (t *ShardReindexTaskGeneric) SetSwapPropAtomic(
	f func(context.Context, *lsmkv.Store, ReindexTracker, int, string) (*lsmkv.Bucket, error),
) {
	t.swapPropAtomic = f
}

// SwapPropAtomic invokes the wired seam, so a test can drive the
// Phase-2a flip directly.
func (t *ShardReindexTaskGeneric) SwapPropAtomic(ctx context.Context, store *lsmkv.Store,
	rt ReindexTracker, propIdx int, propName string,
) (*lsmkv.Bucket, error) {
	return t.swapPropAtomic(ctx, store, rt, propIdx, propName)
}

func (t *ShardReindexTaskGeneric) SetTrackerMkdirGuard(f func(ShardLike) func(func() error) error) {
	t.trackerMkdirGuard = f
}

func (t *ShardReindexTaskGeneric) TrackerMkdirGuard() func(ShardLike) func(func() error) error {
	return t.trackerMkdirGuard
}

func (t *ShardReindexTaskGeneric) SetRegisterDoubleWriteCallbacksFn(
	f func(shard ShardLike, props []string, bucketNamer func(string) string, forTargetStrategy bool) func(),
) {
	t.registerDoubleWriteCallbacksFn = f
}

func (t *ShardReindexTaskGeneric) RegisterDoubleWriteCallbacksFn() func(
	shard ShardLike, props []string, bucketNamer func(string) string, forTargetStrategy bool,
) func() {
	return t.registerDoubleWriteCallbacksFn
}

func (t *ShardReindexTaskGeneric) SetRebuildRangeableRepFn(
	f func(ctx context.Context, b *lsmkv.Bucket) error,
) {
	t.rebuildRangeableRepFn = f
}

func (t *ShardReindexTaskGeneric) RebuildRangeableInMemoryReps(ctx context.Context,
	logger logrus.FieldLogger, shard ShardLike, props []string,
) error {
	return t.rebuildRangeableInMemoryReps(ctx, logger, shard, props)
}
