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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
)

// The property buckets a same-name recreate re-opens.
func ghostPropBuckets() []string {
	return []string{
		helpers.BucketFromPropNameLSM(filters.InternalPropID), // property__id
		helpers.BucketFromPropNameLSM("payload"),              // property_payload
		helpers.BucketFromPropNameLSM("seq"),                  // property_seq
	}
}

// seedClassGhosts pre-registers, for each tenant×prop, a bucket entry under the
// index's class path — the residue a pre-Shutdown-failed shard.drop leaves in the
// process-global registry. Each probe is balanced via t.Cleanup. Returns the
// seeded paths.
func seedClassGhosts(t *testing.T, idx *Index, tenants []string) []string {
	t.Helper()
	var paths []string
	for _, tenant := range tenants {
		for _, prop := range ghostPropBuckets() {
			p := filepath.Join(shardPathLSM(idx.path(), tenant), prop)
			require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p))
			t.Cleanup(func() { lsmkv.GlobalBucketRegistry.Remove(p) })
			paths = append(paths, p)
		}
	}
	return paths
}

// T1: a class delete (index.drop) whose per-shard teardown left registry residue
// must sweep the whole class path root, so the process-global registry is clean
// however far teardown got. Covers the three observed failures: ghosts survive
// the class delete; a same-name recreate fails; a repeat delete is impotent —
// each fixed by the terminal sweep.
func TestIndexDrop_GhostResidueSweptOnClassDelete(t *testing.T) {
	ctx := context.Background()
	tenants := []string{"tenant-3", "tenant-17", "tenant-42"}

	t.Run("ghosts from a partially failed class delete are swept", func(t *testing.T) {
		idx := newEmptyMTIndex(t)
		ghosts := seedClassGhosts(t, idx, tenants)

		require.NoError(t, idx.drop())

		for _, p := range ghosts {
			require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p),
				"index.drop must sweep the class root of stranded registry entries (%s)", p)
			lsmkv.GlobalBucketRegistry.Remove(p)
		}
	})

	t.Run("same-name recreate re-opens the tenant id bucket", func(t *testing.T) {
		root := t.TempDir()
		idxA := newEmptyMTIndexAt(t, root)
		seedClassGhosts(t, idxA, tenants)

		require.NoError(t, idxA.drop())

		// A fresh index at the same RootPath + class computes identical tenant
		// bucket paths, so a real Store must now re-open property__id where a ghost
		// sat — the exact "create id property: bucket … already registered" failure.
		idxB := newEmptyMTIndexAt(t, root)
		require.Equal(t, idxA.path(), idxB.path(), "fresh index must share the dropped class path")

		idProp := helpers.BucketFromPropNameLSM(filters.InternalPropID)
		for _, tenant := range tenants {
			lsmPath := shardPathLSM(idxB.path(), tenant)
			store, err := lsmkv.New(lsmPath, lsmPath, logrus.New(), nil, nil,
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop())
			require.NoError(t, err)
			require.NoError(t,
				store.CreateOrLoadBucket(ctx, idProp, lsmkv.WithStrategy(lsmkv.StrategyReplace)),
				"same-name recreate must re-open %q for %s after the class delete", idProp, tenant)
			require.NoError(t, store.Shutdown(ctx))
		}
	})

	t.Run("repeat delete self-heals residual ghosts", func(t *testing.T) {
		root := t.TempDir()

		idx1 := newEmptyMTIndexAt(t, root)
		seedClassGhosts(t, idx1, tenants)
		require.NoError(t, idx1.drop())

		// A subsequent delete over the same class path (a fresh index, its tenants
		// long gone from the schema) still sweeps residual ghosts — repeat-delete
		// is self-healing, not impotent.
		idx2 := newEmptyMTIndexAt(t, root)
		ghosts := seedClassGhosts(t, idx2, tenants)
		require.NoError(t, idx2.drop())

		for _, p := range ghosts {
			require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p),
				"a repeat class delete must also sweep residual registry entries (%s)", p)
			lsmkv.GlobalBucketRegistry.Remove(p)
		}
	})
}

// T2: index.drop historically logged-and-swallowed per-shard drop
// failures (return nil). It must instead compound them into its return so a
// partial class delete is not reported as clean. A loaded shard is made to fail a
// pre-Shutdown step; index.drop must return non-nil.
func TestIndexDrop_SurfacesShardDropFailure(t *testing.T) {
	ctx := testCtx()
	shardLike, idx := testShard(t, ctx, "TestClass")
	shard, ok := shardLike.(*Shard)
	require.True(t, ok, "expected an eagerly loaded *Shard, got %T", shardLike)

	boom := injectPreShutdownFailure(t, shard)

	err := idx.drop()
	require.Error(t, err, "index.drop must surface (not swallow) a shard-drop failure")
	require.ErrorContains(t, err, boom.Error())
}
