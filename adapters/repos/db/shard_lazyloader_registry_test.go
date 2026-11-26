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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// An unloaded LazyLoadShard drop purges the shard's residual registry entries,
// which the registry tracks in memory rather than on disk.
func TestLazyLoadShardDrop_RegistryPurged(t *testing.T) {
	idx := newEmptyMTIndex(t)

	const tenant = "t1"
	l := &LazyLoadShard{shardOpts: &deferredShardOpts{
		index: idx,
		name:  tenant,
	}}

	p := tenantIDBucketPath(idx, tenant)
	require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p))
	t.Cleanup(func() { lsmkv.GlobalBucketRegistry.Remove(p) })

	require.NoError(t, l.drop())

	require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p),
		"an unloaded LazyLoadShard drop must purge the shard's registry residue")
	lsmkv.GlobalBucketRegistry.Remove(p)
}
