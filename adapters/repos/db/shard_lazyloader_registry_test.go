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

// An unloaded LazyLoadShard drop purges the shard's residual registry entries
// in BOTH keepFiles modes: the registry tracks open buckets in memory, so
// whether the on-disk files are kept (class delete with a backup in progress)
// or removed must not change the purge.
func TestLazyLoadShardDrop_RegistryPurgedRegardlessOfKeepFiles(t *testing.T) {
	tests := []struct {
		name      string
		keepFiles bool
	}{
		{name: "files removed", keepFiles: false},
		{name: "files kept for an in-progress backup", keepFiles: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newEmptyMTIndex(t)

			const tenant = "t1"
			l := &LazyLoadShard{shardOpts: &deferredShardOpts{
				index: idx,
				name:  tenant,
			}}

			p := tenantIDBucketPath(idx, tenant)
			require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p))
			t.Cleanup(func() { lsmkv.GlobalBucketRegistry.Remove(p) })

			require.NoError(t, l.drop(tt.keepFiles))

			require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p),
				"an unloaded LazyLoadShard drop must purge the shard's registry residue (keepFiles=%v)", tt.keepFiles)
			lsmkv.GlobalBucketRegistry.Remove(p)
		})
	}
}
