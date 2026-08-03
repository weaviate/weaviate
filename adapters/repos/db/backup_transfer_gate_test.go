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
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
)

// coldTransferPaths are the two cold-shard transfer loops. Which one runs in
// production depends on whether the filesystem supports hardlinks, so both
// carry the same gate and both are pinned here.
var coldTransferPaths = []struct {
	name string
	run  func(ctx context.Context, idx *Index, desc *backup.ClassDescriptor) error
}{
	{
		name: "hardlinks",
		run: func(ctx context.Context, idx *Index, desc *backup.ClassDescriptor) error {
			return idx.descriptorWithHardlinks(ctx, "transfer-gate-backup", desc, nil)
		},
	},
	{
		name: "without hardlinks",
		run: func(ctx context.Context, idx *Index, desc *backup.ClassDescriptor) error {
			return idx.descriptorWithoutHardlinks(ctx, "transfer-gate-backup", desc, nil)
		},
	},
}

func transferGateShardName(i int) string {
	return fmt.Sprintf("cold-tenant-%d", i)
}

// gateProbe records the shard names a fixture's activity lookup is asked
// about, to distinguish "probed shard N" from "probed shard 0, N times".
type gateProbe struct {
	mu    sync.Mutex
	names []string
}

func (p *gateProbe) record(shardName string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.names = append(p.names, shardName)
}

// probed returns the recorded names, sorted and deduplicated.
func (p *gateProbe) probed() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := map[string]struct{}{}
	out := make([]string, 0, len(p.names))
	for _, n := range p.names {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// transferGateShardNames returns the fixture's shard names in order.
func transferGateShardNames(shards int) []string {
	names := make([]string, shards)
	for s := 0; s < shards; s++ {
		names[s] = transferGateShardName(s)
	}
	sort.Strings(names)
	return names
}

// liveOnEveryShard is the fixture lookup for "a reindex is running everywhere".
func liveOnEveryShard(string) bool { return true }

// newTransferGateTestIndex builds an Index whose cold-transfer loop walks
// inactive shards, and counts how often each backup-gate lookup is built.
// withShardDirs=false never reaches the gate (stat fails first), so tests
// assert its build count explicitly rather than relying on it as a given.
func newTransferGateTestIndex(t *testing.T, shards int, withShardDirs bool, live func(shardName string) bool) (*Index, *atomic.Int64, *atomic.Int64, *gateProbe) {
	t.Helper()

	rootDir := t.TempDir()
	className := "TransferGateClass"

	// Replication factor equal to the tenant count puts every shard on every
	// node, so readSchema reports all of them as local to node1.
	builder := NewMultiTenantShardingStateBuilder().WithReplicationFactor(int64(shards))
	for s := 0; s < shards; s++ {
		builder.AddTenant(transferGateShardName(s), models.TenantActivityStatusCOLD)
	}
	idx := newDescriptorTestIndex(t, rootDir, className, builder.Build())

	if withShardDirs {
		for s := 0; s < shards; s++ {
			createColdShardFiles(t, rootDir, className, transferGateShardName(s))
		}
	}

	activityBuilds, cleanupBuilds := &atomic.Int64{}, &atomic.Int64{}
	probe := &gateProbe{}
	db := &DB{}
	db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
		activityBuilds.Add(1)
		return func(_, shardName string) bool {
			probe.record(shardName)
			return live != nil && live(shardName)
		}, nil
	})
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		cleanupBuilds.Add(1)
		return func(string, string) bool { return false }
	})
	idx.db = db

	return idx, activityBuilds, cleanupBuilds, probe
}

// TestColdTransfer_BuildsReindexLookupOncePerShardSet pins that one cold
// transfer builds each backup-gate lookup exactly once, regardless of shard
// count.
func TestColdTransfer_BuildsReindexLookupOncePerShardSet(t *testing.T) {
	tests := []struct {
		name          string
		shards        int
		withShardDirs bool
		wantBuilds    int64
	}{
		{name: "single shard", shards: 1, withShardDirs: true, wantBuilds: 1},
		{name: "three shards, still one build", shards: 3, withShardDirs: true, wantBuilds: 1},
		{name: "twelve shards, still one build", shards: 12, withShardDirs: true, wantBuilds: 1},
		{name: "fifty shards, still one build", shards: 50, withShardDirs: true, wantBuilds: 1},
		{name: "no shard dirs, gate never reached", shards: 12, withShardDirs: false, wantBuilds: 0},
	}

	for _, tc := range tests {
		for _, path := range coldTransferPaths {
			t.Run(tc.name+"/"+path.name, func(t *testing.T) {
				idx, activityBuilds, cleanupBuilds, probe := newTransferGateTestIndex(t, tc.shards, tc.withShardDirs, nil)

				var desc backup.ClassDescriptor
				require.NoError(t, path.run(context.Background(), idx, &desc))

				wantDescribed := 0
				if tc.withShardDirs {
					wantDescribed = tc.shards
				}
				require.Lenf(t, desc.Shards, wantDescribed,
					"the loop must have walked %d shards to their descriptors; a shorter run would make the build count below meaningless",
					wantDescribed)

				require.Equalf(t, tc.wantBuilds, activityBuilds.Load(),
					"expected %d ListDistributedTasks lookup build(s) for %d shards, got %d",
					tc.wantBuilds, tc.shards, activityBuilds.Load())
				require.Equalf(t, tc.wantBuilds, cleanupBuilds.Load(),
					"expected %d cleanup lookup build(s) for %d shards, got %d",
					tc.wantBuilds, tc.shards, cleanupBuilds.Load())

				if tc.withShardDirs {
					require.Equal(t, transferGateShardNames(tc.shards), probe.probed(),
						"one gate must still be asked about every shard by its own name")
				}
			})
		}
	}
}

// TestHotTransfer_BuildsReindexLookupOncePerShardSet pins that a backup of
// hot shards resolves the reindex gate once for the whole set, not once per
// shard: every shard must be handed the same gate instance.
func TestHotTransfer_BuildsReindexLookupOncePerShardSet(t *testing.T) {
	const shards = 12

	tests := []struct {
		name string
		// expectGateConsumer wires the interface method that receives the
		// gate on this path, mirroring what the real Shard does with it.
		expectGateConsumer func(m *MockShardLike, shardName string, idx *Index, record func(*reindexGate))
		run                func(ctx context.Context, idx *Index, desc *backup.ClassDescriptor) error
	}{
		{
			name: "hardlinks",
			expectGateConsumer: func(m *MockShardLike, shardName string, idx *Index, record func(*reindexGate)) {
				m.EXPECT().preventShutdown().Return(func() {}, nil)
				m.EXPECT().
					CreateBackupSnapshot(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(_ context.Context, sd *backup.ShardDescriptor, _ string, gate *reindexGate) ([]string, error) {
						record(gate)
						sd.Name = shardName
						sd.Node = "node1"
						return []string{}, nil
					})
			},
			run: func(ctx context.Context, idx *Index, desc *backup.ClassDescriptor) error {
				return idx.descriptorWithHardlinks(ctx, "hot-gate-backup", desc, nil)
			},
		},
		{
			name: "without hardlinks",
			expectGateConsumer: func(m *MockShardLike, shardName string, idx *Index, record func(*reindexGate)) {
				m.EXPECT().preventShutdown().Return(func() {}, nil)
				m.EXPECT().
					HaltForTransfer(mock.Anything, false, mock.Anything, mock.Anything).
					RunAndReturn(func(_ context.Context, _ bool, _ time.Duration, gate *reindexGate) error {
						record(gate)
						return nil
					})
				m.EXPECT().
					ListBackupFiles(mock.Anything, mock.Anything).
					RunAndReturn(func(_ context.Context, sd *backup.ShardDescriptor) ([]string, error) {
						sd.Name = shardName
						sd.Node = "node1"
						return []string{}, nil
					})
			},
			run: func(ctx context.Context, idx *Index, desc *backup.ClassDescriptor) error {
				return idx.descriptorWithoutHardlinks(ctx, "hot-gate-backup", desc, nil)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rootDir := t.TempDir()
			className := "HotTransferGateClass"

			builder := NewMultiTenantShardingStateBuilder().WithReplicationFactor(int64(shards))
			for s := 0; s < shards; s++ {
				builder.AddTenant(hotGateShardName(s), models.TenantActivityStatusHOT)
			}
			idx := newDescriptorTestIndex(t, rootDir, className, builder.Build())

			var builds atomic.Int64
			db := &DB{}
			db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
				builds.Add(1)
				return func(string, string) bool { return false }, nil
			})
			idx.db = db

			var mu sync.Mutex
			var seen []*reindexGate
			record := func(g *reindexGate) {
				mu.Lock()
				defer mu.Unlock()
				seen = append(seen, g)
			}

			for s := 0; s < shards; s++ {
				name := hotGateShardName(s)
				mockShard := NewMockShardLike(t)
				tc.expectGateConsumer(mockShard, name, idx, record)
				idx.shards.Store(name, mockShard)
			}

			var desc backup.ClassDescriptor
			require.NoError(t, tc.run(context.Background(), idx, &desc))
			require.Len(t, desc.Shards, shards,
				"every hot shard must have been walked, or the counts below mean nothing")

			require.Len(t, seen, shards, "every shard must have been handed a gate")
			distinct := map[*reindexGate]struct{}{}
			for i, g := range seen {
				require.NotNilf(t, g, "shard %d was handed a nil gate", i)
				distinct[g] = struct{}{}
			}
			require.Lenf(t, distinct, 1,
				"one backup must hand the same gate to all %d hot shards, got %d distinct gates",
				shards, len(distinct))

			// Resolved here, not inside the mock: testify reflects over
			// arguments to build mismatch messages, and that read would race
			// the concurrent resolve.
			require.Zero(t, builds.Load(), "mocked shards do not resolve the gate")
			for _, g := range seen {
				g.anyLiveReindexForShard(className, "any-shard")
			}

			require.Equalf(t, int64(1), builds.Load(),
				"the one gate handed to all %d hot shards resolves to one ListDistributedTasks query, got %d",
				shards, builds.Load())
		})
	}
}

func hotGateShardName(i int) string {
	return fmt.Sprintf("hot-tenant-%d", i)
}

// TestHotTransfer_FailClosedRefusesEveryShard pins that sharing one gate
// doesn't soften fail-closed: a failed DTM query refuses every shard, not
// just the one that happened to resolve it.
func TestHotTransfer_FailClosedRefusesEveryShard(t *testing.T) {
	const shards = 12

	var builds atomic.Int64
	db := &DB{}
	db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
		builds.Add(1)
		// Mirrors configure_api.go when ListDistributedTasks fails: no
		// snapshot, so every shard is refused until DTM is reachable.
		return nil, errors.New("list distributed tasks: leader unreachable")
	})
	idx := &Index{db: db, Config: IndexConfig{ClassName: "HotFailClosedClass"}}

	// One backup, one gate: what the transfer loops hand each shard.
	gate := idx.newReindexGate()

	refused := 0
	for s := 0; s < shards; s++ {
		shard := &Shard{index: idx, name: hotGateShardName(s)}
		if errors.Is(shard.HaltForTransfer(context.Background(), false, 0, gate),
			backup.ErrBackupBlockedByInFlightReindex) {
			refused++
		}
	}

	require.Equalf(t, shards, refused,
		"a failed DTM query must refuse all %d shards, not only the first", shards)
	require.Equal(t, int64(1), builds.Load(),
		"and must still cost exactly one query")
}

// TestColdTransfer_PopulatedShardsReachTheGate pins that the gate is actually
// consulted: a live reindex refuses a shard with data and leaves a shard with
// no local data untouched (same build count either way).
func TestColdTransfer_PopulatedShardsReachTheGate(t *testing.T) {
	for _, path := range coldTransferPaths {
		t.Run(path.name, func(t *testing.T) {
			t.Run("shards holding data are refused", func(t *testing.T) {
				idx, activityBuilds, _, _ := newTransferGateTestIndex(t, 4, true, liveOnEveryShard)

				var desc backup.ClassDescriptor
				err := path.run(context.Background(), idx, &desc)
				require.Error(t, err)
				require.True(t, errors.Is(err, backup.ErrBackupBlockedByInFlightReindex),
					"refusal must wrap the sentinel so the coordinator can classify it, got %v", err)
				require.Equal(t, int64(1), activityBuilds.Load(),
					"one build must serve the whole shard set, on the refusing path too")
			})

			t.Run("shards with no local data short-circuit before the gate", func(t *testing.T) {
				idx, activityBuilds, _, _ := newTransferGateTestIndex(t, 4, false, liveOnEveryShard)

				var desc backup.ClassDescriptor
				require.NoError(t, path.run(context.Background(), idx, &desc),
					"a missing shard dir returns errShardNoLocalData before the gate, so a live reindex cannot refuse it")
				require.Zero(t, activityBuilds.Load(),
					"a loop that reaches no shard must issue no query")
			})
		})
	}
}
