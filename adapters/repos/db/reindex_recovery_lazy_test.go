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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Recovery runs when a shard loads, not at boot; a shard that never loads costs nothing.
func TestRecoveryRunsWhenAShardLoadsNotAtBoot(t *testing.T) {
	const warm, cold = "tenant-warm", "tenant-cold"
	ctx := testCtx()
	className := "LazyRecovery_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	defer shd.Shutdown(context.Background())

	logger, _ := test.NewNullLogger()
	recovery := NewMigrationRecoveryReindexer(true, logger, nil)
	idx.shardReindexer = recovery
	p := NewReindexProvider(idx.db, nil, nil, logger, idx.getSchema.NodeName(), nil, ctx)
	recovery.AttachProvider(p)

	desc := distributedtask.TaskDescriptor{ID: "T", Version: 3}
	subjects := map[string]MigrationSubject{}
	lazy := map[string]*LazyLoadShard{}
	for _, name := range []string{warm, cold} {
		subject := testMigrationSubject(desc.Version, StrategyCodeSearchableRetokenize, "title")
		subject.TaskID, subject.Collection = desc.ID, className
		subject.Key.UnitID = testMigrationUnitFor(idx, name)
		lsm := shardPathLSM(idx.path(), name)
		for _, dir := range []string{subject.Props["title"].Staged, subject.Props["title"].Sidecar} {
			require.NoError(t, os.MkdirAll(filepath.Join(lsm, dir), 0o777))
		}
		require.NoError(t, NewMigrationRecordStore(lsm, logger).Put(NewMigrationRecordIterated(subject)))
		subjects[name] = subject

		shard := NewLazyLoadShard(ctx, nil, name, idx, class, idx.centralJobQueue,
			idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
			false, idx.bitmapBufPool)
		idx.shards.Store(name, shard)
		lazy[name] = shard
		t.Cleanup(func() {
			if shard.isLoaded() {
				require.NoError(t, shard.Shutdown(context.Background()))
			}
		})
	}

	require.Empty(t, p.cachedReindexTasks(desc, subjects[warm].Key.UnitID), "boot recovered a shard that has not loaded")

	require.NoError(t, lazy[warm].Load(ctx))
	require.NotZero(t, lazy[warm].shard.migrationMirrors.ArmedMigrationMirrors(),
		"a flip the next promotion acts on needs its writes mirrored from the first write")
	require.NotEmpty(t, p.cachedReindexTasks(desc, subjects[warm].Key.UnitID),
		"the swap must reuse the task whose mirror is armed")

	require.False(t, lazy[cold].isLoaded())
	require.Empty(t, p.cachedReindexTasks(desc, subjects[cold].Key.UnitID))
	reread := NewMigrationRecordStore(shardPathLSM(idx.path(), cold), logger)
	require.NoError(t, reread.Load())
	rec, ok := reread.Get(subjects[cold].Key)
	require.True(t, ok)
	require.False(t, rec.Subject().Unmirrored, "a shard that never loaded took no writes to mirror")
}
