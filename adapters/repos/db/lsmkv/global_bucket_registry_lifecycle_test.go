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

// End-to-end acceptance #2 at the lsmkv altitude: after a store is torn down
// via any teardown variant, a FRESH store at the same directory must re-open
// the same bucket names without ErrBucketAlreadyRegistered, and no stranded
// registry entry may remain for the involved paths. This is the restore-after-
// delete journey the chaos run failed.
func TestRegistryLifecycle_RestoreAfterTeardownVariants(t *testing.T) {
	ctx := context.Background()

	newStore := func(t *testing.T, dir string) *Store {
		logger, _ := test.NewNullLogger()
		store, err := New(dir, dir, logger, nil, nil,
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop())
		require.NoError(t, err)
		return store
	}

	tests := []struct {
		name string
		// teardown creates buckets on store1 and performs the variant's dir op.
		// It returns the bucket names a restore must be able to re-open and every
		// on-disk path whose registry entry must be free after store1 shuts down.
		teardown func(t *testing.T, store *Store, dir string) (reopen, mustBeFree []string)
	}{
		{
			name: "plain-shutdown",
			teardown: func(t *testing.T, store *Store, dir string) ([]string, []string) {
				require.NoError(t, store.CreateOrLoadBucket(ctx, "property__id", WithStrategy(StrategyReplace)))
				return []string{"property__id"}, []string{filepath.Join(dir, "property__id")}
			},
		},
		{
			name: "rename-then-shutdown",
			teardown: func(t *testing.T, store *Store, dir string) ([]string, []string) {
				require.NoError(t, store.CreateOrLoadBucket(ctx, "property__id", WithStrategy(StrategyReplace)))
				store.Bucket("property__id").UpdateStatus(storagestate.StatusReadOnly)
				require.NoError(t, store.RenameBucket(ctx, "property__id", "property__id_ren"))
				return []string{"property__id"},
					[]string{filepath.Join(dir, "property__id"), filepath.Join(dir, "property__id_ren")}
			},
		},
		{
			name: "swap-then-shutdown",
			teardown: func(t *testing.T, store *Store, dir string) ([]string, []string) {
				require.NoError(t, store.CreateOrLoadBucket(ctx, "main", WithStrategy(StrategyReplace)))
				require.NoError(t, store.CreateOrLoadBucket(ctx, "ingest", WithStrategy(StrategyReplace)))
				oldMain, err := store.SwapBucketPointer(ctx, "main", "ingest")
				require.NoError(t, err)
				require.NoError(t, oldMain.Shutdown(ctx))
				return []string{"main", "ingest"},
					[]string{filepath.Join(dir, "main"), filepath.Join(dir, "ingest")}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			store1 := newStore(t, dir)

			reopen, mustBeFree := tt.teardown(t, store1, dir)

			// Balance every probe even if an assertion Goexits.
			for _, p := range mustBeFree {
				p := p
				t.Cleanup(func() { GlobalBucketRegistry.Remove(p) })
			}

			require.NoError(t, store1.Shutdown(ctx))

			for _, p := range mustBeFree {
				require.NoError(t, GlobalBucketRegistry.TryAdd(p),
					"teardown left a stranded registry entry for %s", p)
				GlobalBucketRegistry.Remove(p)
			}

			store2 := newStore(t, dir)
			for _, name := range reopen {
				require.NoError(t, store2.CreateOrLoadBucket(ctx, name, WithStrategy(StrategyReplace)),
					"restore must be able to re-open bucket %q after teardown", name)
			}
			require.NoError(t, store2.Shutdown(ctx))
		})
	}
}
