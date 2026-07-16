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

//go:build integrationTest

package db

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

// failUnregisterCtrl wraps a real CycleCallbackCtrl so IsActive/Activate/
// Deactivate and the real callback unregistration still run (no leaked cycle,
// -race clean), but Unregister reports an error. It reproduces a pre-Shutdown
// step failure in shard.drop (the cycle-callback Unregister at shard_drop.go:99)
// that early-returns before store.Shutdown — the only step that deregisters the
// shard's buckets.
type failUnregisterCtrl struct {
	cyclemanager.CycleCallbackCtrl
	err error
}

func (f failUnregisterCtrl) Unregister(ctx context.Context) error {
	_ = f.CycleCallbackCtrl.Unregister(ctx) // still unregister the real callback
	return f.err
}

// injectPreShutdownFailure makes shard.drop fail at the cycle-callback
// unregister step (before store.Shutdown) and returns the sentinel it surfaces.
func injectPreShutdownFailure(t *testing.T, shard *Shard) error {
	t.Helper()
	boom := errors.New("injected pre-shutdown unregister failure")
	shard.cycleCallbacks.flushCallbacksCtrl = failUnregisterCtrl{
		CycleCallbackCtrl: shard.cycleCallbacks.flushCallbacksCtrl,
		err:               boom,
	}
	return boom
}

// T3: shard.drop early-returns on any pre-Shutdown step failure, and
// store.Shutdown is the only step that deregisters the shard's buckets. Without
// the deferred best-effort store.Shutdown guard, a pre-Shutdown failure ghosts
// every bucket of the shard (and leaves a zombie store). The guard must reach
// store.Shutdown on every exit, so the buckets deregister even when a
// pre-Shutdown step failed — evidenced independently of the index-level sweep.
func TestShardDrop_StoreShutdownReachedOnPreShutdownFailure(t *testing.T) {
	ctx := testCtx()
	shardLike, _ := testShard(t, ctx, "TestClass")
	shard, ok := shardLike.(*Shard)
	require.True(t, ok, "expected an eagerly loaded *Shard, got %T", shardLike)

	idBucketPath := filepath.Join(shard.pathLSM(),
		helpers.BucketFromPropNameLSM(filters.InternalPropID))

	// Precondition: while the shard is live, the id bucket is registered.
	require.ErrorIs(t, lsmkv.GlobalBucketRegistry.TryAdd(idBucketPath), lsmkv.ErrBucketAlreadyRegistered,
		"precondition: the shard's id bucket must be registered while it is live")
	// Balance the post-drop probe on every exit, including a failed-assertion Goexit.
	t.Cleanup(func() { lsmkv.GlobalBucketRegistry.Remove(idBucketPath) })

	boom := injectPreShutdownFailure(t, shard)

	err := shard.drop(false)
	require.Error(t, err, "shard.drop must surface the pre-shutdown failure")
	require.ErrorContains(t, err, boom.Error())

	require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(idBucketPath),
		"the deferred store.Shutdown guard must deregister the shard's buckets even when a pre-shutdown step failed")
	lsmkv.GlobalBucketRegistry.Remove(idBucketPath)
}
