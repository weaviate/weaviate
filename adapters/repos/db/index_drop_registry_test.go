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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
)

// A representative set of per-tenant property buckets to seed as ghosts.
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

// A class delete (index.drop) whose per-shard teardown left registry residue
// must sweep the whole class path root, so the process-global registry is clean
// however far teardown got. Without the sweep, a same-name recreate fails its
// TryAdd with ErrBucketAlreadyRegistered and a repeat delete leaves the ghosts
// in place.
func TestIndexDrop_GhostResidueSweptOnClassDelete(t *testing.T) {
	tenants := []string{"tenant-3", "tenant-17", "tenant-42"}

	idx := newEmptyMTIndex(t)
	ghosts := seedClassGhosts(t, idx, tenants)

	require.NoError(t, idx.drop())

	for _, p := range ghosts {
		require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p),
			"index.drop must sweep the class root of stranded registry entries (%s)", p)
		lsmkv.GlobalBucketRegistry.Remove(p)
	}
}

// index.drop must compound per-shard drop failures into its return rather than
// only logging them, so a partial class delete is not reported as clean. A
// loaded shard is made to fail a pre-Shutdown step; index.drop must return
// non-nil.
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
