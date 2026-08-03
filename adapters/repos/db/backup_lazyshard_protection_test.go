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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// newLoadableColdLazyShard returns an Index with one mapped, unloaded
// LazyLoadShard that has real files and can genuinely load — loaded once
// then shut down, so tests can tell "left cold" apart from "can't load".
func newLoadableColdLazyShard(t *testing.T, className, shardName string) (*Index, *LazyLoadShard) {
	t.Helper()
	ctx := testCtx()

	shardState := NewMultiTenantShardingStateBuilder().
		AddTenant(shardName, models.TenantActivityStatusHOT).
		WithReplicationFactor(1).
		Build()

	repo, migrator, schemaGetter := newLazyLoadRepo(t, shardState)
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	class := newClassWithWarmProp(className)
	require.NoError(t, migrator.AddClass(ctx, class))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	idx := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)

	lazy, ok := idx.shards.Load(shardName).(*LazyLoadShard)
	require.True(t, ok, "fixture must map a LazyLoadShard for %q", shardName)

	require.NoError(t, lazy.Load(ctx))
	require.NoError(t, lazy.Shutdown(ctx))
	require.False(t, lazy.isLoaded(), "fixture must hand back a cold shard")

	return idx, lazy
}

// TestColdBackup_ProtectedLazyShardRefusesActivation pins that every shard
// activation route refuses while a non-hardlink backup holds the shard
// protected and reading it from disk.
func TestColdBackup_ProtectedLazyShardRefusesActivation(t *testing.T) {
	const (
		className = "ProtectedLazyClass"
		shardName = "cold-tenant"
	)
	ctx := context.Background()

	idx, lazy := newLoadableColdLazyShard(t, className, shardName)

	var desc backup.ClassDescriptor
	require.NoError(t, idx.descriptorWithoutHardlinks(ctx, "protection-backup", &desc, nil))
	require.Len(t, desc.Shards, 1, "the cold shard must have been described from disk")
	require.NotEmpty(t, desc.Shards[0].Files)

	_, protected := idx.backupProtectedShards.Load(shardName)
	require.True(t, protected, "the described shard must be marked protected")

	// Every route a reader or the background lazy-loader takes to activate the
	// shard must be refused while its files are listed but not yet uploaded.
	activations := []struct {
		name string
		run  func() error
	}{
		{"GetShard", func() error { _, _, err := idx.GetShard(ctx, shardName); return err }},
		{"getOrInitShard", func() error { _, _, err := idx.getOrInitShard(ctx, shardName); return err }},
		{"LoadLocalShard", func() error { return idx.LoadLocalShard(ctx, shardName, false) }},
		{"loadLocalShardIfActive", func() error { return idx.loadLocalShardIfActive(shardName) }},
		{"Load", func() error { return lazy.Load(ctx) }},
	}
	for _, a := range activations {
		t.Run(a.name, func(t *testing.T) {
			require.ErrorContains(t, a.run(), "protected for backup")
			require.False(t, lazy.isLoaded(),
				"%s must not have opened the store the backup is reading", a.name)
		})
	}

	require.NoError(t, idx.ReleaseBackup(ctx, "protection-backup"))
	require.NoError(t, lazy.Load(ctx), "release must lift the protection")
}
