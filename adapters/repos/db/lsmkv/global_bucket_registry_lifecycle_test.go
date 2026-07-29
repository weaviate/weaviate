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

package lsmkv

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storagestate"
)

// A bucket registers its on-disk path with the process-global
// GlobalBucketRegistry at NewBucket time. A later Store op can mutate the
// bucket's dir (RenameBucket, FinalizeBucketSwap). Bucket.Shutdown must
// deregister the path it originally REGISTERED — not its current GetDir() —
// otherwise the original path is stranded in the registry with no live bucket
// and a same-name restore's TryAdd fails with ErrBucketAlreadyRegistered.
func TestRegistryLifecycle_ShutdownDeregistersRegisteredPath(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		// mutate opens a bucket, performs a dir-mutating Store op, and returns
		// the on-disk path the bucket was originally registered at.
		mutate func(t *testing.T, store *Store, dir string) (registeredPath string)
	}{
		{
			name: "RenameBucket",
			mutate: func(t *testing.T, store *Store, dir string) string {
				const name = "property_name"
				require.NoError(t, store.CreateOrLoadBucket(ctx, name, WithStrategy(StrategyReplace)))
				registeredPath := filepath.Join(dir, name)
				// RenameBucket requires the bucket to be read-only.
				store.Bucket(name).UpdateStatus(storagestate.StatusReadOnly)
				require.NoError(t, store.RenameBucket(ctx, name, "property_name_renamed"))
				return registeredPath
			},
		},
		{
			name: "FinalizeBucketSwap",
			mutate: func(t *testing.T, store *Store, dir string) string {
				const name = "property_searchable_ingest"
				require.NoError(t, store.CreateOrLoadBucket(ctx, name, WithStrategy(StrategyReplace)))
				registeredPath := filepath.Join(dir, name)
				canonicalDir := filepath.Join(dir, "property_searchable")
				backupDir := filepath.Join(dir, "property_searchable_bak")
				require.NoError(t, store.FinalizeBucketSwap(ctx, name, canonicalDir, registeredPath, backupDir))
				return registeredPath
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			logger, _ := test.NewNullLogger()
			store, err := New(dir, dir, logger, nil, nil,
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop(),
				cyclemanager.NewCallbackGroupNoop())
			require.NoError(t, err)

			registeredPath := tt.mutate(t, store, dir)

			// Balance the probe on every path — including the failed-assertion
			// path below, which Goexits. If the entry is stranded we still
			// remove it so a leaked probe can't contaminate sibling tests via
			// the process-global registry.
			t.Cleanup(func() { GlobalBucketRegistry.Remove(registeredPath) })

			require.NoError(t, store.Shutdown(ctx))

			require.NoError(t, GlobalBucketRegistry.TryAdd(registeredPath),
				"Bucket.Shutdown must deregister the path it registered at NewBucket, not its mutated GetDir()")
		})
	}
}
