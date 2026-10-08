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
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/changelog"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/objects"
)

func changelogTestClass() *models.Class {
	return &models.Class{
		Class: "ChangelogTestClass",
		InvertedIndexConfig: &models.InvertedIndexConfig{
			UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
		},
		Properties: []*models.Property{
			{
				Name:         "label",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
		},
	}
}

func logrusTestLogger() (*logrus.Logger, *logrus.Logger) {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l, l
}

func setupChangelogTestShard(t *testing.T, ctx context.Context) *Shard {
	t.Helper()
	class := changelogTestClass()
	vic := hnsw.UserConfig{Distance: common.DefaultDistanceMetric}
	shardLike, _ := testShardWithSettings(t, ctx, class, vic, false, false)
	switch s := shardLike.(type) {
	case *Shard:
		return s
	case *LazyLoadShard:
		require.NoError(t, s.Load(ctx), "force-load lazy shard")
		return s.shard
	default:
		t.Fatalf("setupChangelogTestShard: unexpected shard type %T", shardLike)
		return nil
	}
}

func changelogTestObject(idStr string, label string, updateTimeMillis int64) *storobj.Object {
	return &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 strfmt.UUID(idStr),
			Class:              "ChangelogTestClass",
			Properties:         map[string]interface{}{"label": label},
			LastUpdateTimeUnix: updateTimeMillis,
		},
	}
}

func drainAllEntries(t *testing.T, log *changelog.ChangeLog) []*changelog.Entry {
	t.Helper()
	tailer, err := log.NewTailer(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tailer.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var out []*changelog.Entry
	for {
		entry, err := tailer.Next(ctx)
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		out = append(out, entry)
	}
}

// Exercises all 5 tee sites. Without this, a refactor silently dropping a
// tee call is invisible to the suite.
func TestShard_ChangeLog_AllWritePaths_Roundtrip(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	log, err := shard.ActivateChangeLog(ctx, "op-roundtrip")
	require.NoError(t, err)

	// Site 1: PUT.
	id1 := uuid.NewString()
	require.NoError(t, shard.PutObject(ctx, changelogTestObject(id1, "initial", 1_000)))

	// Site 2: MERGE non-mutable.
	require.NoError(t, shard.MergeObject(ctx, objects.MergeDocument{
		ID:              strfmt.UUID(id1),
		Class:           "ChangelogTestClass",
		PrimitiveSchema: map[string]interface{}{"label": "merged"},
		UpdateTime:      2_000,
	}))

	// Site 3: MERGE mutable. Called directly — production only reaches this
	// path via batch-reference writes, which need cross-class fixtures.
	idBytes, err := uuid.MustParse(id1).MarshalBinary()
	require.NoError(t, err)
	_, err = shard.mutableMergeObjectLSM(ctx, objects.MergeDocument{
		ID:              strfmt.UUID(id1),
		Class:           "ChangelogTestClass",
		PrimitiveSchema: map[string]interface{}{"label": "merged-mutable"},
		UpdateTime:      3_000,
	}, idBytes)
	require.NoError(t, err)

	// Site 4: DELETE single.
	require.NoError(t, shard.DeleteObject(ctx, strfmt.UUID(id1), time.UnixMilli(4_000)))

	// Site 5: DELETE batch. Needs a fresh object to delete.
	id2 := uuid.NewString()
	require.NoError(t, shard.PutObject(ctx, changelogTestObject(id2, "batch-target", 5_000)))
	results := shard.DeleteObjectBatch(ctx, []strfmt.UUID{strfmt.UUID(id2)}, time.UnixMilli(6_000), false)
	for _, r := range results {
		require.NoError(t, r.Err)
	}

	finalLSN, err := shard.FinalizeChangeLog(ctx, "op-roundtrip")
	require.NoError(t, err)
	require.Equal(t, uint64(6), finalLSN, "expected 6 entries across the 5 tee sites")

	entries := drainAllEntries(t, log)
	require.Len(t, entries, 6)

	// LSNs 1-4 are id1 (put, merge, mutable merge, delete); LSNs 5-6 are id2 (put, batch delete).
	type expectation struct {
		isDelete bool
		uuid     strfmt.UUID
	}
	want := []expectation{
		{false, strfmt.UUID(id1)},
		{false, strfmt.UUID(id1)},
		{false, strfmt.UUID(id1)},
		{true, strfmt.UUID(id1)},
		{false, strfmt.UUID(id2)},
		{true, strfmt.UUID(id2)},
	}
	for i, entry := range entries {
		require.Equal(t, uint64(i+1), entry.LSN, "LSNs must be monotonic and gap-free")
		require.Equalf(t, want[i].isDelete, entry.IsDelete,
			"entry %d: IsDelete mismatch", i)
		if entry.IsDelete {
			require.Emptyf(t, entry.Payload, "entry %d DELETE must not carry a payload", i)
			continue
		}
		require.NotEmptyf(t, entry.Payload, "entry %d PUT must carry a payload", i)
		decoded, err := storobj.FromBinaryNetwork(entry.Payload)
		require.NoErrorf(t, err, "entry %d storobj payload must decode cleanly", i)
		require.Equalf(t, want[i].uuid, decoded.ID(), "entry %d wrong UUID in storobj", i)
	}

	require.NoError(t, shard.StopChangeCapture(ctx, "op-roundtrip"))
}

// Pins the three skip-gate branches the plan calls out: skipUpsert=true skips,
// docIDPreserved=true with skipUpsert=false MUST fire, and existing==nil DELETE
// skips. Without case 2, "simplifying" the skip condition drops real writes.
func TestShard_ChangeLog_SkipPaths_NoEntry(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	log, err := shard.ActivateChangeLog(ctx, "op-skips")
	require.NoError(t, err)

	id := uuid.NewString()

	// Case 1: identical PUT → skipUpsert=true → no tee.
	require.NoError(t, shard.PutObject(ctx, changelogTestObject(id, "same", 100)))
	require.NoError(t, shard.PutObject(ctx, changelogTestObject(id, "same", 100)))

	// Case 2: same id+vector, different property → docIDPreserved=true, skipUpsert=false → MUST tee.
	require.NoError(t, shard.PutObject(ctx, changelogTestObject(id, "different", 150)))

	// Case 3: DELETE on nonexistent UUID → existing==nil early-return → no tee.
	require.NoError(t, shard.DeleteObject(ctx, strfmt.UUID(uuid.NewString()), time.UnixMilli(200)))

	finalLSN, err := shard.FinalizeChangeLog(ctx, "op-skips")
	require.NoError(t, err)
	require.Equal(t, uint64(2), finalLSN)

	entries := drainAllEntries(t, log)
	require.Len(t, entries, 2)
	require.False(t, entries[0].IsDelete)
	require.False(t, entries[1].IsDelete, "docIDPreserved PUT must appear as entry 2")

	require.NoError(t, shard.StopChangeCapture(ctx, "op-skips"))
}

// Writers racing FinalizeChangeLog must neither deadlock nor hang the seal.
// The only assertion is the 10s timeout.
func TestShard_ChangeLog_LockOrder_NoDeadlock(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	_, err := shard.ActivateChangeLog(ctx, "op-lockorder")
	require.NoError(t, err)

	const writers = 8
	const writesPerWorker = 50

	logger, _ := logrusTestLogger()

	var (
		wg          sync.WaitGroup
		startSignal = make(chan struct{})
	)
	for w := range writers {
		wg.Add(1)
		workerIdx := w
		enterrors.GoWrapper(func() {
			defer wg.Done()
			<-startSignal
			for i := range writesPerWorker {
				id := uuid.NewString()
				// Errors irrelevant: any outcome still exercises the lock stack.
				_ = shard.PutObject(ctx, changelogTestObject(id, "lo", int64(workerIdx*10000+i)))
			}
		}, logger)
	}
	close(startSignal)

	// Let writers enter the lock stack so Finalize actually contends.
	time.Sleep(1 * time.Millisecond)
	_, err = shard.FinalizeChangeLog(ctx, "op-lockorder")
	require.NoError(t, err)

	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		wg.Wait()
		close(done)
	}, logger)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: writers did not complete within 10s — lock order regression likely")
	}

	require.NoError(t, shard.StopChangeCapture(ctx, "op-lockorder"))
}

// Pins the "tee is free in production steady state" invariant that the whole
// design rests on. Any allocation in the nil-set path fails this.
func TestShard_ChangeLog_NilFastPath_NoAllocations(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	obj := changelogTestObject(uuid.NewString(), "x", 1)
	idBytes, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
	require.NoError(t, err)
	objBinary, err := obj.MarshalBinary()
	require.NoError(t, err)

	require.Nil(t, shard.changeLogs.Load())

	puts := testing.AllocsPerRun(200, func() {
		shard.AppendChangeLogPut(idBytes, obj.LastUpdateTimeUnix(), objBinary)
	})
	require.Zero(t, puts, "AppendChangeLogPut with nil changeLogs must not allocate")

	deletes := testing.AllocsPerRun(200, func() {
		shard.AppendChangeLogDelete(idBytes, 1)
	})
	require.Zero(t, deletes, "AppendChangeLogDelete with nil changeLogs must not allocate")

	_ = ctx
}

// Bounds a HOT shard's changelog directory: orphans get cleaned on the next
// activation, registered ops survive, non-.log files untouched.
func TestShard_ChangeLog_ActivateSweepsOrphans(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	dir := filepath.Join(shard.path(), "changelog")
	require.NoError(t, os.MkdirAll(dir, 0o700))

	_, err := shard.ActivateChangeLog(ctx, "op-live")
	require.NoError(t, err)
	livePath := filepath.Join(dir, "op-live.log")

	orphan1 := filepath.Join(dir, "op-orphan-1.log")
	orphan2 := filepath.Join(dir, "op-orphan-2.log")
	keepNonLog := filepath.Join(dir, "keep.meta")
	require.NoError(t, os.WriteFile(orphan1, []byte("o1"), 0o600))
	require.NoError(t, os.WriteFile(orphan2, []byte("o2"), 0o600))
	require.NoError(t, os.WriteFile(keepNonLog, []byte("k"), 0o600))

	_, err = shard.ActivateChangeLog(ctx, "op-new")
	require.NoError(t, err)

	_, err = os.Stat(orphan1)
	require.True(t, os.IsNotExist(err), "orphan-1 must be swept on activate")
	_, err = os.Stat(orphan2)
	require.True(t, os.IsNotExist(err), "orphan-2 must be swept on activate")
	_, err = os.Stat(livePath)
	require.NoError(t, err, "live op's file must not be swept")
	_, err = os.Stat(keepNonLog)
	require.NoError(t, err, "non-.log files must not be swept")

	require.NoError(t, shard.StopChangeCapture(ctx, "op-live"))
	require.NoError(t, shard.StopChangeCapture(ctx, "op-new"))
}

// Two concurrent activates must each leave their own .log file on disk.
// Without changeLogsLifecycleMu, the second caller's keep-snapshot can miss
// the first caller's pending op and sweep its freshly-opened file.
func TestShard_ChangeLog_ConcurrentActivate_NoCrossSweep(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	dir := filepath.Join(shard.path(), "changelog")
	require.NoError(t, os.MkdirAll(dir, 0o700))

	const N = 8
	var wg sync.WaitGroup
	errs := make(chan error, N)
	wg.Add(N)
	start := make(chan struct{})
	for i := range N {
		opID := "op-concurrent-" + strconv.Itoa(i)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			<-start
			if _, err := shard.ActivateChangeLog(ctx, opID); err != nil {
				errs <- err
			}
		}, shard.index.logger)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	for i := range N {
		opID := "op-concurrent-" + strconv.Itoa(i)
		_, err := os.Stat(filepath.Join(dir, opID+".log"))
		require.NoError(t, err, "op %q file must survive concurrent activates", opID)
	}

	for i := range N {
		require.NoError(t, shard.StopChangeCapture(ctx, "op-concurrent-"+strconv.Itoa(i)))
	}
}

// SnapshotChangeLogLSN must reflect every committed append AND keep the log
// writable past the snapshot — the single-log movement design relies on
// "drain up to N" without sealing.
func TestShard_SnapshotChangeLogLSN_ReflectsAppendsAndStaysWritable(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	_, err := shard.ActivateChangeLog(ctx, "op-snapshot")
	require.NoError(t, err)

	const before = 4
	for i := range before {
		require.NoError(t, shard.PutObject(ctx,
			changelogTestObject(uuid.NewString(), "x", int64(1_000+i))))
	}

	snap, err := shard.SnapshotChangeLogLSN(ctx, "op-snapshot")
	require.NoError(t, err)
	require.Equal(t, uint64(before), snap, "snapshot must reflect all committed appends")

	// Log is still writable: subsequent appends get LSNs > snap.
	require.NoError(t, shard.PutObject(ctx,
		changelogTestObject(uuid.NewString(), "after-snap", 9_999)))
	finalLSN, err := shard.FinalizeChangeLog(ctx, "op-snapshot")
	require.NoError(t, err)
	require.Equal(t, uint64(before+1), finalLSN, "writes after snapshot must keep advancing the LSN")

	require.NoError(t, shard.StopChangeCapture(ctx, "op-snapshot"))
}

// Unknown op-id must error rather than return zero — otherwise a typo from
// the gRPC layer would surface as a successful "snapshot" of LSN 0.
func TestShard_SnapshotChangeLogLSN_NoSuchLog(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	_, err := shard.SnapshotChangeLogLSN(ctx, "op-never-activated")
	require.Error(t, err)
	require.ErrorIs(t, err, errNoSuchChangeLog)
}

// Snapshot post-Finalize must be a pure read of finalLSN — retries on the
// consumer side may legitimately observe a finalized log.
func TestShard_SnapshotChangeLogLSN_AfterFinalize(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	_, err := shard.ActivateChangeLog(ctx, "op-after-final")
	require.NoError(t, err)

	const k = 3
	for i := range k {
		require.NoError(t, shard.PutObject(ctx,
			changelogTestObject(uuid.NewString(), "x", int64(i+1))))
	}
	finalLSN, err := shard.FinalizeChangeLog(ctx, "op-after-final")
	require.NoError(t, err)
	require.Equal(t, uint64(k), finalLSN)

	snap, err := shard.SnapshotChangeLogLSN(ctx, "op-after-final")
	require.NoError(t, err)
	require.Equal(t, finalLSN, snap)

	// And again, to confirm it is purely a read.
	snap2, err := shard.SnapshotChangeLogLSN(ctx, "op-after-final")
	require.NoError(t, err)
	require.Equal(t, finalLSN, snap2)

	require.NoError(t, shard.StopChangeCapture(ctx, "op-after-final"))
}

// TestShard_ChangeLog_Finalize_WaitsForPreSealPendingOnly: FinalizeChangeLog
// blocks on the in-flight set snapshotted at entry but not on later
// registrations, so the seal completes under sustained write load.
func TestShard_ChangeLog_Finalize_WaitsForPreSealPendingOnly(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)
	logger, _ := logrusTestLogger()

	_, err := shard.ActivateChangeLog(ctx, "op-fence")
	require.NoError(t, err)

	// Pre-snapshot PREPARE: in flight before FinalizeChangeLog runs, left
	// uncommitted. The seal must block on this.
	preObj := changelogTestObject(uuid.NewString(), "pre-snapshot", 1_000)
	require.Empty(t, shard.preparePutObject(ctx, "req-pre", preObj).Errors)

	finalizeDone := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, ferr := shard.FinalizeChangeLog(ctx, "op-fence")
		finalizeDone <- ferr
	}, logger)

	// Let FinalizeChangeLog snapshot the pending set ({req-pre}).
	time.Sleep(20 * time.Millisecond)

	// Post-snapshot PREPAREs, left uncommitted. They must NOT block the seal
	// — the snapshot was taken before they registered.
	for i := range 20 {
		obj := changelogTestObject(uuid.NewString(), "post-snapshot", int64(2_000+i))
		require.Empty(t, shard.preparePutObject(ctx, "req-post-"+strconv.Itoa(i), obj).Errors)
	}

	// req-pre is still in flight, so the seal must still block.
	select {
	case <-finalizeDone:
		t.Fatal("FinalizeChangeLog sealed while a pre-snapshot PREPARE was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	// Drain the pre-snapshot set. The 20 post-snapshot PREPAREs are still
	// uncommitted, but they must not hold the seal.
	shard.commitReplication(ctx, "req-pre")

	select {
	case ferr := <-finalizeDone:
		require.NoError(t, ferr, "seal must complete once the pre-snapshot set drains, ignoring post-snapshot PREPAREs")
	case <-time.After(5 * time.Second):
		t.Fatal("FinalizeChangeLog did not seal after the pre-snapshot set drained — post-snapshot PREPAREs wrongly blocked it")
	}

	require.NoError(t, shard.StopChangeCapture(ctx, "op-fence"))
}

// A resumed movement reactivates its op-id while the donor still holds the log.
func TestShard_ChangeLog_ReactivateSameOpID(t *testing.T) {
	activators := []struct {
		name     string
		activate func(ctx context.Context, shard *Shard, opID string) error
	}{
		{
			name: "shard",
			activate: func(ctx context.Context, shard *Shard, opID string) error {
				_, err := shard.ActivateChangeLog(ctx, opID)
				return err
			},
		},
		{
			name: "index",
			activate: func(ctx context.Context, shard *Shard, opID string) error {
				return shard.index.IncomingStartChangeCapture(ctx, shard.name, opID)
			},
		},
	}
	tests := []struct {
		name       string
		prevWrites int
		prep       func(t *testing.T, old *changelog.ChangeLog) *changelog.Tailer
	}{
		{name: "plain"},
		{name: "entries appended before", prevWrites: 3},
		{
			name:       "tailer open on old log",
			prevWrites: 2,
			prep: func(t *testing.T, old *changelog.ChangeLog) *changelog.Tailer {
				tailer, err := old.NewTailer(0)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, tailer.Close()) })
				return tailer
			},
		},
		{
			name:       "old log finalized",
			prevWrites: 1,
			prep: func(t *testing.T, old *changelog.ChangeLog) *changelog.Tailer {
				_, err := old.Finalize()
				require.NoError(t, err)
				return nil
			},
		},
	}
	for _, act := range activators {
		for _, tc := range tests {
			t.Run(act.name+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				shard := setupChangelogTestShard(t, ctx)
				const opID = "op-resumed"

				require.NoError(t, act.activate(ctx, shard, opID))
				old, ok := shard.GetChangeLog(ctx, opID)
				require.True(t, ok)
				for i := range tc.prevWrites {
					require.NoError(t, shard.PutObject(ctx, changelogTestObject(uuid.NewString(), "before", int64(i+1))))
				}
				var tailer *changelog.Tailer
				if tc.prep != nil {
					tailer = tc.prep(t, old)
				}

				require.NoError(t, act.activate(ctx, shard, opID))

				fresh, ok := shard.GetChangeLog(ctx, opID)
				require.True(t, ok)
				require.NotSame(t, old, fresh)
				require.Equal(t, uint64(0), fresh.LSN())
				matches, err := filepath.Glob(filepath.Join(shard.changelogDir(), opID+changelogFileExtension))
				require.NoError(t, err)
				require.Len(t, matches, 1)
				_, err = old.AppendDelete([16]byte{}, 1)
				require.ErrorIs(t, err, changelog.ErrLogDeactivated)
				if tailer != nil {
					nextCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					_, err := tailer.Next(nextCtx)
					require.ErrorIs(t, err, changelog.ErrLogDeactivated)
				}

				id := uuid.NewString()
				require.NoError(t, shard.PutObject(ctx, changelogTestObject(id, "after", 100)))
				finalLSN, err := shard.FinalizeChangeLog(ctx, opID)
				require.NoError(t, err)
				require.Equal(t, uint64(1), finalLSN)
				entries := drainAllEntries(t, fresh)
				require.Len(t, entries, 1)
				require.Equal(t, uuid.MustParse(id), uuid.UUID(entries[0].UUID))

				require.NoError(t, shard.StopChangeCapture(ctx, opID))
			})
		}
	}
}

func TestShard_ChangeLog_AbandonedReactivateKeepsLiveLog(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)
	const opID = "op-live"

	_, err := shard.ActivateChangeLog(ctx, opID)
	require.NoError(t, err)
	live, ok := shard.GetChangeLog(ctx, opID)
	require.True(t, ok)
	require.NoError(t, shard.PutObject(ctx, changelogTestObject(uuid.NewString(), "live", 1)))

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = shard.ActivateChangeLog(cancelled, opID)
	require.ErrorIs(t, err, context.Canceled)

	still, ok := shard.GetChangeLog(ctx, opID)
	require.True(t, ok)
	require.Same(t, live, still)
	require.Equal(t, uint64(1), still.LSN())
	require.NoError(t, shard.StopChangeCapture(ctx, opID))
}

func changelogDirEntries(t *testing.T, shard *Shard) []string {
	t.Helper()
	entries, err := os.ReadDir(shard.changelogDir())
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestShard_ChangeLog_StaleLogFailureSparesSuccessor(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)
	const opID = "op-replaced"

	stale, err := shard.ActivateChangeLog(ctx, opID)
	require.NoError(t, err)
	fresh, err := shard.ActivateChangeLog(ctx, opID)
	require.NoError(t, err)
	require.NotSame(t, stale, fresh)

	shard.handleChangeLogFailure(opID, stale, errors.New("disk full"))

	registered, ok := shard.GetChangeLog(ctx, opID)
	require.True(t, ok)
	require.Same(t, fresh, registered)
	require.Equal(t, []string{opID + changelogFileExtension}, changelogDirEntries(t, shard))
	id := uuid.NewString()
	require.NoError(t, shard.PutObject(ctx, changelogTestObject(id, "after", 1)))
	finalLSN, err := shard.FinalizeChangeLog(ctx, opID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), finalLSN)
	entries := drainAllEntries(t, fresh)
	require.Len(t, entries, 1)
	require.NoError(t, shard.StopChangeCapture(ctx, opID))
}

func TestShard_ChangeLog_LifecycleSerializedWithResumedActivation(t *testing.T) {
	const opID = "op-raced"
	type result struct {
		log *changelog.ChangeLog
		err error
	}
	tests := []struct {
		name   string
		hooked func(ctx context.Context, shard *Shard, log *changelog.ChangeLog) error
	}{
		{
			name: "failure vs resumed activate",
			hooked: func(_ context.Context, shard *Shard, log *changelog.ChangeLog) error {
				shard.handleChangeLogFailure(opID, log, errors.New("disk full"))
				return nil
			},
		},
		{
			name: "stop vs resumed activate",
			hooked: func(ctx context.Context, shard *Shard, _ *changelog.ChangeLog) error {
				return shard.StopChangeCapture(ctx, opID)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			shard := setupChangelogTestShard(t, ctx)
			log, err := shard.ActivateChangeLog(ctx, opID)
			require.NoError(t, err)

			done := make(chan result, 1)
			shard.changeLogLifecycleCheckedHook = func() {
				shard.changeLogLifecycleCheckedHook = nil
				enterrors.GoWrapper(func() {
					fresh, err := shard.ActivateChangeLog(ctx, opID)
					done <- result{log: fresh, err: err}
				}, shard.index.logger)
				select {
				case res := <-done:
					done <- res
				case <-time.After(250 * time.Millisecond):
				}
			}

			require.NoError(t, tc.hooked(ctx, shard, log))

			var res result
			select {
			case res = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("resumed activation did not finish")
			}
			require.NoError(t, res.err)
			_, err = log.AppendDelete([16]byte{}, 1)
			require.ErrorIs(t, err, changelog.ErrLogDeactivated)

			registered, ok := shard.GetChangeLog(ctx, opID)
			require.True(t, ok)
			require.Same(t, res.log, registered)
			require.Equal(t, []string{opID + changelogFileExtension}, changelogDirEntries(t, shard))
			require.NoError(t, shard.PutObject(ctx, changelogTestObject(uuid.NewString(), "after", 1)))
			finalLSN, err := shard.FinalizeChangeLog(ctx, opID)
			require.NoError(t, err)
			require.Equal(t, uint64(1), finalLSN)
			require.NoError(t, shard.StopChangeCapture(ctx, opID))
			require.Empty(t, changelogDirEntries(t, shard))
		})
	}
}

// A stop its caller gave up on while it queued must not tear down the log a retried activation registered since.
func TestShard_ChangeLog_AbandonedStopKeepsRetriedLog(t *testing.T) {
	const opID = "op-retried"
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)
	idx := shard.index
	require.NoError(t, idx.IncomingStartChangeCapture(ctx, shard.name, opID))
	require.NoError(t, shard.PutObject(ctx, changelogTestObject(uuid.NewString(), "captured", 1)))

	shard.changeLogsLifecycleMu.Lock()
	abandoned, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	enterrors.GoWrapper(func() { done <- idx.IncomingStopChangeCapture(abandoned, shard.name, opID) }, idx.logger)
	cancel()
	shard.changeLogsLifecycleMu.Unlock()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("abandoned stop did not return")
	}

	require.Equal(t, []string{opID + changelogFileExtension}, changelogDirEntries(t, shard))
	lsn, err := idx.IncomingSnapshotChangeLogLSN(ctx, shard.name, opID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), lsn)
}

func TestIndex_ChangeLog_RefusesNamesLeavingTheirDir(t *testing.T) {
	const opID = "op-1"
	type row struct {
		name     string
		shard    string
		op       string
		badOp    bool
		unloaded bool
	}
	var tests []row
	for _, bad := range []string{"..", "../x", "a/b", ".", "", "/x", "x/", "x" + api.RecoveryFolderSuffix} {
		tests = append(tests, row{name: "shard " + strconv.Quote(bad), shard: bad, op: opID})
		for _, unloaded := range []bool{false, true} {
			tests = append(tests, row{name: "op " + strconv.Quote(bad) + ", unloaded " + strconv.FormatBool(unloaded), op: bad, badOp: true, unloaded: unloaded})
		}
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			shard := setupChangelogTestShard(t, ctx)
			idx := shard.index
			shardName := tc.shard
			wantErr := "invalid shard name"
			if tc.badOp {
				shardName = shard.name
				wantErr = "invalid op id"
			}
			if tc.unloaded {
				loaded, ok := idx.shards.LoadAndDelete(shard.name)
				require.True(t, ok)
				require.NoError(t, loaded.Shutdown(ctx))
				idx.shards.Store(shard.name, NewLazyLoadShard(ctx, nil, shard.name, idx, idx.getClass(), idx.centralJobQueue,
					idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer, false, idx.bitmapBufPool))
			}
			escapedLog := filepath.Join(shardPath(idx.path(), shardName), changelogDirName, tc.op+changelogFileExtension)

			require.ErrorContains(t, idx.IncomingStartChangeCapture(ctx, shardName, tc.op), wantErr)
			for _, ep := range incomingChangeLogEndpoints {
				require.ErrorContains(t, ep.call(ctx, idx, shardName, tc.op), wantErr, ep.name)
			}

			require.NoFileExists(t, escapedLog)
			if !tc.badOp {
				require.NoDirExists(t, shardPathLSM(idx.path(), shardName))
				require.Nil(t, idx.shards.Load(shardName))
			}
			if tc.unloaded {
				require.Nil(t, idx.shards.Loaded(shard.name))
			}
		})
	}
}
