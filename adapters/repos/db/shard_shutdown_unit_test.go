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
	"bytes"
	"context"
	"errors"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/compact"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	enthfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Unit pins for the shard shutdown/restore lifecycle (the integrationTest-
// tagged journeys live in shard_shutdown_test.go; these stay untagged so the
// plain unit gate runs them).

// TestShardStillAlive pins the lifecycle-fence predicate: a shard whose
// Shutdown failed before the shut mark is fully alive and must be restored to
// the shard map (an orphaned live instance lets a reactivation double-open
// the same directory); a shard that got past the mark must not be re-served.
func TestShardStillAlive(t *testing.T) {
	live := &Shard{shutdownLock: new(sync.RWMutex)}
	require.True(t, shardStillAlive(live), "not marked shut => alive")

	shut := &Shard{shutdownLock: new(sync.RWMutex)}
	shut.shut.Store(true)
	require.False(t, shardStillAlive(shut), "marked shut => not restored")

	require.False(t, shardStillAlive(&LazyLoadShard{}),
		"an unloaded lazy shard holds nothing to orphan")

	loaded := &LazyLoadShard{loaded: true, shard: live}
	require.True(t, shardStillAlive(loaded))

	loadedShut := &LazyLoadShard{loaded: true, shard: shut}
	require.False(t, shardStillAlive(loadedShut))
}

// TestShutdown_RequestFlagAfterFailure pins the two failure contracts of the
// request flag:
//   - still-in-use: the flag STAYS armed — pending refs exist by definition,
//     and the last release completing the shutdown is the designed
//     eventual-shutdown contract (integration-pinned by
//     TestShardShutdownWhenIdleEventually); guarded ops refuse meanwhile.
//   - any other abort (e.g. a teardown that already failed): the flag is
//     cleared — there may be no pending release left to complete it, and a
//     restored shard would otherwise be a zombie behind a trusted map hit.
func TestShutdown_RequestFlagAfterFailure(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("still in use keeps the deferred shutdown armed", func(t *testing.T) {
		s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
		s.inUseCounter.Add(1) // an in-flight guarded op

		err := s.Shutdown(context.Background())
		require.ErrorIs(t, err, errShardStillInUse)
		require.True(t, s.shutdownRequested.Load(),
			"pending refs complete the shutdown on release; the flag must stay armed")
		_, err = s.preventShutdown()
		require.ErrorIs(t, err, errShutdownInProgress,
			"new guarded ops are refused while the drain is pending")
	})

	t.Run("ctx cancellation while in use keeps the flag armed", func(t *testing.T) {
		// backoff returns ctx.Err() and swallows the still-in-use attempt
		// error; the pending refs still exist, so the deferred completion
		// must stay armed.
		s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
		s.inUseCounter.Add(1)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := s.Shutdown(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.True(t, s.shutdownRequested.Load(),
			"a cancelled wait on an in-use shard is still the in-use case; disarming it strands the deferred shutdown")
	})

	t.Run("non-in-use abort clears the flag", func(t *testing.T) {
		s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
		s.shut.Store(true)
		s.teardownErr = errors.New("bucket close failed")
		s.shutdownRequested.Store(true)

		start := time.Now()
		err := s.Shutdown(context.Background())
		require.Error(t, err)
		require.ErrorIs(t, err, errTeardownFailed)
		require.NotErrorIs(t, err, errShardStillInUse)
		require.False(t, s.shutdownRequested.Load(),
			"an abort with no pending release must disarm the deferred shutdown")
		require.Less(t, time.Since(start), time.Second,
			"a sticky teardown error must fail fast, not burn the retry backoff under shardCreateLocks")
	})
}

// TestRestoreShardIfStillAlive pins the shared restore helper used by every
// failed-shutdown site (UpdateTenants cold path, ShutdownShard,
// UnloadLocalShard, IncomingReinitShard).
func TestRestoreShardIfStillAlive(t *testing.T) {
	var m shardMap

	live := &Shard{shutdownLock: new(sync.RWMutex)}
	require.True(t, restoreShardIfStillAlive(&m, "s1", live))
	got := m.Load("s1")
	require.NotNil(t, got)

	shut := &Shard{shutdownLock: new(sync.RWMutex)}
	shut.shut.Store(true)
	require.False(t, restoreShardIfStillAlive(&m, "s2", shut),
		"a cleanly-shut shard must not be re-served")
	require.Nil(t, m.Load("s2"))
}

// TestPerformShutdown_TeardownErrorSticks pins that a teardown failure after
// the shut mark stays visible: without the sticky error, the idempotent
// short-circuit converts the retry into a silent nil and callers treat the
// partially-torn shard (open buckets, uncleared registry entries) as cleanly
// closed.
func TestPerformShutdown_TeardownErrorSticks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	s := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
	s.shut.Store(true)
	s.teardownErr = errors.New("boom: bucket close failed")

	err := s.Shutdown(context.Background())
	require.Error(t, err, "a swallowed teardown failure must resurface, not read as success")
	require.Contains(t, err.Error(), "boom")

	// A cleanly shut shard keeps the idempotent nil.
	clean := &Shard{index: &Index{logger: logger}, shutdownLock: new(sync.RWMutex)}
	clean.shut.Store(true)
	require.NoError(t, clean.Shutdown(context.Background()))
}

// TestShardReleasesCounterAndVersionFiles pins that a shard holds its indexcount
// file open only while it is loaded, and its version file not at all once it has
// loaded. Each tenant a node loads and unloads would otherwise keep descriptors.
func TestShardReleasesCounterAndVersionFiles(t *testing.T) {
	ctx := context.Background()
	className := "FileDescriptors"
	shard, index := testShard(t, ctx, className)
	s, ok := shard.(*Shard)
	require.True(t, ok, "the counter and versioner live on the concrete shard")
	require.NoError(t, s.PutObject(ctx, testObject(className)))

	shardPath := s.path()
	counterPath := path.Join(shardPath, "indexcount")
	versionPath := path.Join(shardPath, "version")
	loadShard := func() (*Shard, error) {
		return NewShard(ctx, nil, s.Name(), index, &models.Class{Class: className},
			index.centralJobQueue, index.scheduler, index.indexCheckpoints,
			index.shardReindexer, false, index.bitmapBufPool, monitoring.ShardRegistrationEager)
	}

	// Round 0 shuts down the shard testShard created, and every later round shuts down one loaded from disk.
	for round := range 3 {
		require.Equal(t, 1, openDescriptorCount(t, counterPath),
			"round %d: a loaded shard keeps its counter file open to persist doc IDs", round)
		require.Zero(t, openDescriptorCount(t, versionPath),
			"round %d: a loaded shard has no use for its version file", round)

		require.NoError(t, s.Shutdown(ctx))
		require.Zero(t, openDescriptorCount(t, counterPath), "round %d: shutdown closes the counter file", round)
		require.Zero(t, openDescriptorCount(t, versionPath), "round %d: shutdown leaves the version file closed", round)

		var err error
		s, err = loadShard()
		require.NoError(t, err)
	}
	require.NoError(t, s.Shutdown(ctx))

	// Each case leaves the shard broken on disk, so the case that fails later in the load runs first.
	failedLoads := []struct {
		name      string
		breakDisk func(t *testing.T)
	}{
		{
			name: "version file too short to read",
			breakDisk: func(t *testing.T) {
				require.NoError(t, os.WriteFile(versionPath, []byte{1}, 0o644))
			},
		},
		{
			name: "store fails before the counter opens",
			breakDisk: func(t *testing.T) {
				lsmPath := path.Join(shardPath, "lsm")
				require.NoError(t, os.RemoveAll(lsmPath))
				require.NoError(t, os.WriteFile(lsmPath, nil, 0o644))
			},
		},
	}
	for _, tc := range failedLoads {
		t.Run(tc.name, func(t *testing.T) {
			tc.breakDisk(t)

			_, err := loadShard()
			require.Error(t, err)
			require.Zero(t, openDescriptorCount(t, counterPath), "a failed load closes the counter file")
			require.Zero(t, openDescriptorCount(t, versionPath), "a failed load closes the version file")
		})
	}
}

// TestShardKnownShut pins the reactivation-eviction predicate: ONLY a shard
// that completed a shutdown may be evicted from the map. An unloaded
// LazyLoadShard is the normal steady state of a not-yet-loaded shard —
// evicting it would race a concurrent Load() on the old wrapper into a second
// instance over the same directory.
func TestShardKnownShut(t *testing.T) {
	require.False(t, shardKnownShut(&Shard{shutdownLock: new(sync.RWMutex)}))
	require.False(t, shardKnownShut(&LazyLoadShard{}),
		"never-loaded lazy shard must NOT be treated as shut")

	shut := &Shard{shutdownLock: new(sync.RWMutex)}
	shut.shut.Store(true)
	require.True(t, shardKnownShut(shut))
	require.True(t, shardKnownShut(&LazyLoadShard{loaded: true, shard: shut}))
	require.False(t, shardKnownShut(&LazyLoadShard{loaded: true, shard: &Shard{shutdownLock: new(sync.RWMutex)}}))

	// A deep-teardown failure (shut=true, sticky teardownErr) is NOT
	// evictable: the map entry is the last reference to its possibly-leaked
	// handles, so the restore predicate KEEPS it and the belts serve the
	// sticky error instead of re-initializing into a bucket-registry
	// collision (that refusal is pinned in lsmkv's
	// TestBucketReinit_RefusedWhileLeakedOpenThenHealsAfterClose). It is
	// never silently double-opened and never treated as healthy.
	torn := &Shard{shutdownLock: new(sync.RWMutex), teardownErr: errors.New("bucket close failed")}
	torn.shut.Store(true)
	require.False(t, shardKnownShut(torn), "a torn shard must not be evicted for re-init")
	require.False(t, shardStillAlive(torn))
	require.ErrorContains(t, shardTeardownError(torn), "bucket close failed")

	var m shardMap
	require.True(t, restoreShardIfStillAlive(&m, "torn", torn),
		"a torn shard is retained as the last reference to its leaked handles")
	require.NotNil(t, m.Load("torn"))
}

// TestShardLoadDropsNodeIDBeyondDocIDCounter pins that the vector index and each
// geo property index of a shard are bounded by its document-ID counter, so a
// commit log naming a node far beyond it is truncated on load instead of sizing
// the node index to it (weaviate/0-weaviate-issues#649). HFresh's centroid index
// numbers its nodes by posting ID and must keep no limit; it gets none only
// because it lives in HFresh's directory, where there is no counter.
func TestShardLoadDropsNodeIDBeyondDocIDCounter(t *testing.T) {
	type commitLog struct {
		indexPath     func(s *Shard) string // relative to the shard directory
		wantTruncated bool
	}
	geoLog := commitLog{indexPath: func(*Shard) string { return geoPropID("location") }, wantTruncated: true}

	tests := []struct {
		name              string
		vectorIndexConfig schemaConfig.VectorIndexConfig
		logs              []commitLog
	}{
		{
			name:              "hnsw",
			vectorIndexConfig: enthnsw.NewDefaultUserConfig(),
			logs: []commitLog{
				{indexPath: func(s *Shard) string { return s.vectorIndexID("") }, wantTruncated: true},
				geoLog,
			},
		},
		{
			name:              "hfresh",
			vectorIndexConfig: enthfresh.NewDefaultUserConfig(),
			logs: []commitLog{
				{indexPath: func(s *Shard) string {
					id := s.vectorIndexID("")
					return path.Join(helpers.HFreshDirName(id), helpers.CentroidsID(id))
				}},
				geoLog,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			className := "NodeIDLimit"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "location", DataType: schema.DataTypeGeoCoordinates.PropString()},
				},
			}
			shard, index := testShardWithSettings(t, ctx, class, tc.vectorIndexConfig, false, false, false)
			s, ok := shard.(*Shard)
			require.True(t, ok)

			const objects = 3
			for i := range objects {
				obj := testObject(className)
				obj.Vector = []float32{float32(i), 1, 2, 3}
				obj.Object.Properties = map[string]interface{}{"location": geoCoordinates(float32(i), 1)}
				require.NoError(t, s.PutObject(ctx, obj))
			}
			shardPath := s.path()
			logPaths := make([]string, len(tc.logs))
			for i, l := range tc.logs {
				logPaths[i] = path.Join(shardPath, l.indexPath(s)+".hnsw.commitlog.d", "9999999999")
			}
			require.NoError(t, s.Shutdown(ctx))

			record := walBytesForShardTest(t, objects+1<<24+1000)
			for _, p := range logPaths {
				require.NoError(t, os.WriteFile(p, record, 0o644))
			}

			s, err := NewShard(ctx, nil, s.Name(), index, class,
				index.centralJobQueue, index.scheduler, index.indexCheckpoints,
				index.shardReindexer, false, index.bitmapBufPool, monitoring.ShardRegistrationEager)
			require.NoError(t, err)
			defer s.Shutdown(ctx)

			for i, l := range tc.logs {
				p := logPaths[i]
				st, err := os.Stat(p)
				if l.wantTruncated {
					// The truncated log is empty, and the commit logger prunes empty raw logs.
					if os.IsNotExist(err) {
						continue
					}
					require.NoError(t, err)
					require.Zero(t, st.Size(), "%s must be truncated before the out-of-limit record", p)
					continue
				}
				require.NoError(t, err)
				require.Equal(t, int64(len(record)), st.Size(), "%s must keep its record", p)
			}
		})
	}
}

// walBytesForShardTest encodes an AddNode record for id.
func walBytesForShardTest(t *testing.T, id uint64) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, compact.NewWALWriter(&buf).WriteAddNode(id, 0))
	return buf.Bytes()
}
