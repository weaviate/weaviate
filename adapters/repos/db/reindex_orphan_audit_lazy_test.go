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

package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// A loaded lazy shard's orphan cleanup must go through its open store, not rm -rf its live bucket dirs.
func TestAuditOrphanReindexTrackers_LoadedLazyShardCleansThroughTheStore(t *testing.T) {
	ctx := testCtx()
	f := newAddPropertyLazyFixture(t, "AuditLazy", singleShardState())
	var name string
	var lazy *LazyLoadShard
	for n, l := range f.coldShards(t) {
		name, lazy = n, l
	}
	shard, _, err := lazy.loadIfCold(ctx)
	require.NoError(t, err)

	className := f.index.Config.ClassName.String()
	dir := filepath.Join(shard.pathLSM(), ".migrations", "searchable_retokenize_warm_1")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "started.mig"), nil, 0o600))
	writePayload(t, dir, "task-orphan", 9, "unit-orphan", className, ReindexTypeChangeTokenization, []string{"warm"})
	writePreAgedQuarantineSentinel(t, dir)

	knownNothing := func(string, uint64) bool { return false }
	orphans := collectOrphanTrackers(shard.pathLSM(), className, name, knownNothing, logrus.New())
	require.Len(t, orphans, 1)
	sidecars := sidecarDirsForOrphan(&orphans[0])
	require.NotEmpty(t, sidecars)
	sidecar := sidecars[0]
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, sidecar, lsmkv.WithStrategy(lsmkv.StrategyMapCollection)))
	require.NotNil(t, shard.store.Bucket(sidecar))

	outcome, err := f.index.db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New())
	require.NoError(t, err)
	require.Equal(t, 1, outcome.OrphansClean)
	require.True(t, shard.store.Bucket(sidecar) == nil, "the sidecar bucket was removed from disk while still open in the loaded store")
	require.NoDirExists(t, filepath.Join(shard.pathLSM(), sidecar))
	require.Same(t, shard, lazy.currentShard())
}

func TestAuditOrphanReindexTrackers_ColdLazyShardCleansOnDiskWithoutLoading(t *testing.T) {
	ctx := testCtx()
	f := newAddPropertyLazyFixture(t, "AuditCold", singleShardState())
	var name string
	var lazy *LazyLoadShard
	for n, l := range f.coldShards(t) {
		name, lazy = n, l
	}
	className := f.index.Config.ClassName.String()
	lsmPath := lazy.pathLSM()
	dir := filepath.Join(lsmPath, ".migrations", "searchable_retokenize_warm_1")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "started.mig"), nil, 0o600))
	writePayload(t, dir, "task-orphan", 9, "unit-orphan", className, ReindexTypeChangeTokenization, []string{"warm"})
	writePreAgedQuarantineSentinel(t, dir)
	knownNothing := func(string, uint64) bool { return false }
	orphans := collectOrphanTrackers(lsmPath, className, name, knownNothing, logrus.New())
	require.Len(t, orphans, 1)
	sidecar := filepath.Join(lsmPath, sidecarDirsForOrphan(&orphans[0])[0])
	require.NoError(t, os.MkdirAll(sidecar, 0o755))

	outcome, err := f.index.db.AuditOrphanReindexTrackers(ctx, knownNothing, logrus.New())
	require.NoError(t, err)
	require.Equal(t, 1, outcome.OrphansClean)
	require.NoDirExists(t, sidecar)
	require.NoDirExists(t, dir)
	require.False(t, lazy.isLoaded(), "the audit must not load a cold shard")
}
