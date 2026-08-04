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
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// coldTenantIndex builds an index whose local shards are all COLD
// tenants with files on disk, so a descriptor pass reaches the
// unloaded-shard check once per shard.
func coldTenantIndex(t *testing.T, className string, tenants int) (*Index, []string) {
	t.Helper()

	rootDir := t.TempDir()
	names := make([]string, tenants)
	// Replicate every tenant to every generated node so all of them are
	// local. At a lower factor the builder spreads tenants across nodes
	// and the pass walks a single shard however many were added.
	builder := NewMultiTenantShardingStateBuilder().WithReplicationFactor(int64(tenants))
	for i := range names {
		names[i] = fmt.Sprintf("cold-tenant-%d", i)
		builder = builder.AddTenant(names[i], models.TenantActivityStatusCOLD)
	}

	idx := newDescriptorTestIndex(t, rootDir, className, builder.Build())
	for _, name := range names {
		createColdShardFiles(t, rootDir, className, name)
	}

	local, _, err := idx.readSchema()
	require.NoError(t, err)
	require.ElementsMatch(t, names, local, "fixture must make every tenant a local shard")
	return idx, names
}

// TestBackupDescriptors_BuildsReindexLookupOncePerPass pins that the
// descriptor pass resolves one DTM snapshot for the whole backup, not
// one per shard.
func TestBackupDescriptors_BuildsReindexLookupOncePerPass(t *testing.T) {
	for _, tenants := range []int{1, 3, 12} {
		t.Run(fmt.Sprintf("%d cold shards", tenants), func(t *testing.T) {
			const className = "DescriptorPassCls"
			idx, _ := coldTenantIndex(t, className, tenants)

			db := &DB{indices: map[string]*Index{}, logger: logrus.New()}
			idx.db = db
			db.indices[indexID(schema.ClassName(className))] = idx

			counter := &countingActivityBuilder{snapshots: makeActivityBuilder(nil)}
			counter.install(db)

			for desc := range db.BackupDescriptors(testCtx(), "gate-backup", []string{className}, nil) {
				require.NoError(t, desc.Error)
				require.Len(t, desc.Shards, tenants)
			}

			builds, probed := counter.stats()
			assert.Equal(t, 1, builds, "DTM snapshots built by one descriptor pass")
			// Anti-vacuity: a fixture whose shards never reach the check
			// reports one build whether or not the pass shares it.
			assert.Len(t, probed, tenants, "shards the pass actually judged")
		})
	}
}

// TestBackupDescriptors_ResolvesBeforeReachingAnyShard pins that a pass
// resolves its snapshot at entry, before touching any shard.
func TestBackupDescriptors_ResolvesBeforeReachingAnyShard(t *testing.T) {
	db := &DB{indices: map[string]*Index{}, logger: logrus.New()}
	counter := &countingActivityBuilder{snapshots: makeActivityBuilder(nil)}
	counter.install(db)

	for range db.BackupDescriptors(testCtx(), "gate-backup", nil, nil) {
		t.Fatal("no classes were requested")
	}

	builds, probed := counter.stats()
	assert.Equal(t, 1, builds, "the pass resolves at entry, before any shard lock")
	assert.Empty(t, probed, "no shards to judge")
}

// TestBackupDescriptors_JudgesEachShardByItsOwnName pins that sharing
// one snapshot across the pass does not merge the shards' verdicts.
func TestBackupDescriptors_JudgesEachShardByItsOwnName(t *testing.T) {
	const className = "DescriptorPerShardCls"
	idx, names := coldTenantIndex(t, className, 6)
	blocked := names[4]

	db := &DB{indices: map[string]*Index{}, logger: logrus.New()}
	idx.db = db
	db.indices[indexID(schema.ClassName(className))] = idx

	counter := &countingActivityBuilder{snapshots: makeActivityBuilder(map[[2]string]bool{
		{className, blocked}: true,
	})}
	counter.install(db)

	var passErr error
	for desc := range db.BackupDescriptors(testCtx(), "gate-backup", []string{className}, nil) {
		passErr = desc.Error
	}

	require.Error(t, passErr)
	require.Equal(t, []string{blocked}, blockedShards(passErr, names),
		"only the shard with a live task may be refused")

	_, probed := counter.stats()
	wantProbed := make([][2]string, len(names))
	for i, name := range names {
		wantProbed[i] = [2]string{className, name}
	}
	require.ElementsMatch(t, wantProbed, probed,
		"a snapshot asked about one name repeatedly cannot tell the shards apart")
}

// TestNonHardlinkColdShards_ShareOnePassSnapshot pins that the deprecated
// fallback's unloaded-shard check shares the pass gate. Driven per shard
// directly, not through descriptorWithoutHardlinks, which hits an
// unrelated crash tracked by weaviate/weaviate#12451.
func TestNonHardlinkColdShards_ShareOnePassSnapshot(t *testing.T) {
	const className = "NonHardlinkColdCls"
	idx, names := coldTenantIndex(t, className, 5)

	counter := &countingActivityBuilder{snapshots: makeActivityBuilder(nil)}
	counter.install(idx.db)

	gate := newReindexGate(idx.db)
	for _, name := range names {
		var sd backup.ShardDescriptor
		require.NoError(t, idx.backupInactiveShardWithoutHardlinks(name, &sd, nil, gate))
	}

	builds, probed := counter.stats()
	assert.Equal(t, 1, builds, "DTM snapshots built across the pass's shards")
	assert.Len(t, probed, len(names), "shards the pass actually judged")
}

// TestBackupDescriptors_TwoPassesResolveOneSnapshotEach pins that
// admission and execution are separate passes: a backup resolves once
// for each, never once per shard, and never once for both.
func TestBackupDescriptors_TwoPassesResolveOneSnapshotEach(t *testing.T) {
	const className = "TwoPassCls"
	idx, names := coldTenantIndex(t, className, 4)

	db := &DB{indices: map[string]*Index{}, localNodeName: "node1", logger: logrus.New()}
	idx.db = db
	db.indices[indexID(schema.ClassName(className))] = idx

	counter := &countingActivityBuilder{snapshots: makeActivityBuilder(nil)}
	counter.install(db)

	require.NoError(t, db.Backupable(testCtx(), []string{className}))
	for desc := range db.BackupDescriptors(testCtx(), "gate-backup", []string{className}, nil) {
		require.NoError(t, desc.Error)
	}

	builds, probed := counter.stats()
	assert.Equal(t, 2, builds, "one snapshot per pass, two passes")
	assert.Len(t, probed, 2*len(names), "every shard judged in both passes")
}

// TestHaltForTransfer_UsesThePassGateAndFallsBackFresh pins that a halt
// answers from the pass's snapshot inside a backup pass, and resolves
// fresh outside one (replica movement, offload).
func TestHaltForTransfer_UsesThePassGateAndFallsBackFresh(t *testing.T) {
	tests := []struct {
		name       string
		inPass     bool
		halts      int
		wantBuilds int
	}{
		{name: "three halts inside one pass", inPass: true, halts: 3, wantBuilds: 1},
		{name: "twelve halts inside one pass", inPass: true, halts: 12, wantBuilds: 1},
		{name: "three halts outside any pass", inPass: false, halts: 3, wantBuilds: 3},
		{name: "twelve halts outside any pass", inPass: false, halts: 12, wantBuilds: 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			shd, idx := testShard(t, ctx, "HaltGateCls")

			counter := &countingActivityBuilder{snapshots: makeActivityBuilder(nil)}
			counter.install(idx.db)

			haltCtx := ctx
			if tt.inPass {
				haltCtx = contextWithReindexGate(ctx, newReindexGate(idx.db))
			}

			for range tt.halts {
				require.NoError(t, shd.HaltForTransfer(haltCtx, false, 100*time.Millisecond))
				require.NoError(t, shd.(*Shard).resumeMaintenanceCycles(ctx))
			}

			builds, probed := counter.stats()
			assert.Equal(t, tt.wantBuilds, builds, "DTM snapshots built across the halts")
			assert.Len(t, probed, tt.halts, "halts that actually reached the check")
		})
	}
}
