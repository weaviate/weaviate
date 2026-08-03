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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/objects"
)

// TestColdBackup_ReadDuringColdBackupIsServed pins the user-visible contract a
// cold backup must not break: a read for a shard the backup is uploading waits
// for the upload and is answered. Before the wait existed this returned
// HTTP 500 for the whole upload, roughly ten thousand of them per backup.
func TestColdBackup_ReadDuringColdBackupIsServed(t *testing.T) {
	const (
		className = "ColdBackupReadClass"
		tenant    = "cold-tenant"
		backupID  = "read-during-backup"
	)
	ctx := context.Background()

	repo, idx, shards := newColdLazyShardsWithData(t, className, tenant)

	var desc backup.ClassDescriptor
	require.NoError(t, idx.descriptorWithoutHardlinks(ctx, backupID, &desc, nil))
	_, protected := idx.backupProtectedShards.Load(tenant)
	require.True(t, protected, "the cold shard must be described from disk and protected")

	// Stand in for the upload finishing: the marker clears and the shard can
	// be activated again.
	released := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(released)
		time.Sleep(300 * time.Millisecond)
		require.NoError(t, idx.ReleaseBackup(ctx, backupID))
	}, idx.logger)

	_, queryErr := repo.Query(ctx, &objects.QueryInput{Class: className, Tenant: tenant, Limit: 10})
	require.Nil(t, queryErr, "a read issued during a cold backup must be answered, not refused")

	<-released
	require.True(t, shards[tenant].isLoaded(),
		"the read must have activated the shard once the backup let go")
}

// TestColdBackup_ReadThatOutlastsTheBackupIsNotAServerError pins the fallback:
// a read that gives up waiting still must not look like a server fault, since
// nothing is broken and the next request succeeds.
func TestColdBackup_ReadThatOutlastsTheBackupIsNotAServerError(t *testing.T) {
	const (
		className = "ColdBackupGiveUpClass"
		tenant    = "cold-tenant"
		backupID  = "read-outlasts-backup"
	)

	repo, idx, _ := newColdLazyShardsWithData(t, className, tenant)

	var desc backup.ClassDescriptor
	require.NoError(t, idx.descriptorWithoutHardlinks(context.Background(), backupID, &desc, nil))
	t.Cleanup(func() { require.NoError(t, idx.ReleaseBackup(context.Background(), backupID)) })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, queryErr := repo.Query(ctx, &objects.QueryInput{Class: className, Tenant: tenant, Limit: 10})
	require.NotNil(t, queryErr, "the backup never released, so the read cannot be answered")
	require.NotEqual(t, objects.StatusInternalServerError, queryErr.Code,
		"a shard held by a running backup is transient, not a server fault")
	require.Equal(t, objects.StatusUnprocessableEntity, queryErr.Code)
	require.ErrorIs(t, queryErr, enterrors.ErrShardBackupProtected)
}

// TestPrewarm_ProtectedShardDoesNotEndPrewarmingForTheIndex pins that a shard a
// backup holds cold only costs that shard its prewarm. Returning on the first
// error left every later shard of the index cold for the life of the process,
// while allShardsReady still reported true.
func TestPrewarm_ProtectedShardDoesNotEndPrewarmingForTheIndex(t *testing.T) {
	const (
		className = "PrewarmProtectedClass"
		protected = "held-tenant"
		next      = "next-tenant"
	)
	ctx := context.Background()

	_, idx, shards := newColdLazyShardsWithData(t, className, protected, next)

	// Stand in for the descriptor phase of a cold backup: the shard is claimed
	// and stays claimed until ReleaseBackup.
	idx.backupLock.Lock(protected)
	idx.backupProtectedShards.Store(protected, struct{}{})
	idx.lastBackup.Store(&BackupState{BackupID: "prewarm-backup", InProgress: true})
	t.Cleanup(func() { require.NoError(t, idx.ReleaseBackup(ctx, "prewarm-backup")) })

	idx.prewarmLazyShards([]string{protected, next})

	require.False(t, shards[protected].isLoaded(),
		"the backup still needs this shard's files where they are")
	require.True(t, shards[next].isLoaded(),
		"one held shard must not end prewarming for the rest of the index")
}

// newColdLazyShardsWithData returns an Index whose tenants are mapped as cold
// LazyLoadShards holding one object each, so prewarming does not skip them as
// empty. The background prewarm has already run and been undone, so a test
// drives prewarmLazyShards itself with a shard order it controls.
func newColdLazyShardsWithData(t *testing.T, className string, tenants ...string) (*DB, *Index, map[string]*LazyLoadShard) {
	t.Helper()
	ctx := testCtx()

	// Every tenant must own the local node: the builder spreads partitions over
	// as many nodes as it has tenants.
	builder := NewMultiTenantShardingStateBuilder().WithReplicationFactor(int64(len(tenants)))
	for _, tenant := range tenants {
		builder.AddTenant(tenant, models.TenantActivityStatusHOT)
	}

	repo, migrator, schemaGetter := newLazyLoadRepo(t, builder.Build())
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	class := newClassWithWarmProp(className)
	class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	require.NoError(t, migrator.AddClass(ctx, class))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	idx := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)

	for _, tenant := range tenants {
		require.NoError(t, repo.PutObject(ctx, &models.Object{
			Class:      className,
			Tenant:     tenant,
			ID:         strfmt.UUID(uuid.New().String()),
			Properties: map[string]interface{}{"warm": "value"},
		}, []float32{1, 2, 3}, nil, nil, nil, 0))
	}

	require.Eventually(t, idx.allShardsReady.Load, 30*time.Second, 50*time.Millisecond,
		"the index's own prewarm must finish before a test drives it directly")

	shards := map[string]*LazyLoadShard{}
	for _, tenant := range tenants {
		lazy, ok := idx.shards.Load(tenant).(*LazyLoadShard)
		require.True(t, ok, "fixture must map a LazyLoadShard for %q", tenant)
		require.NoError(t, lazy.Shutdown(ctx))
		require.False(t, lazy.isLoaded(), "fixture must hand back cold shards")
		shards[tenant] = lazy
	}

	return repo, idx, shards
}

// TestBackupProtection_WaitStopsAtTheCallersDeadline pins that a waiter is
// bounded by the caller's context rather than by the upload, and that giving up
// still reports the transient cause.
func TestBackupProtection_WaitStopsAtTheCallersDeadline(t *testing.T) {
	logger, _ := test.NewNullLogger()
	idx := &Index{logger: logger}
	idx.backupProtectedShards.Store("held", struct{}{})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := idx.waitForBackupProtection(ctx, "held")
	require.ErrorIs(t, err, enterrors.ErrShardBackupProtected)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), backupProtectionWait,
		"the wait must end with the caller, not with the upload")

	idx.backupProtectedShards.Delete("held")
	require.NoError(t, idx.waitForBackupProtection(context.Background(), "held"),
		"an unprotected shard must not wait at all")
}
