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
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/sirupsen/logrus"

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
	shardLike, _ := testShardWithSettings(t, ctx, class, vic, false, true, false)
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

	log, err := shard.ActivateChangeLog("op-roundtrip")
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

	finalLSN, err := shard.FinalizeChangeLog("op-roundtrip")
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
		decoded, err := storobj.FromBinary(entry.Payload)
		require.NoErrorf(t, err, "entry %d storobj payload must decode cleanly", i)
		require.Equalf(t, want[i].uuid, decoded.ID(), "entry %d wrong UUID in storobj", i)
	}

	require.NoError(t, shard.StopChangeCapture("op-roundtrip"))
}

// Pins the three skip-gate branches the plan calls out: skipUpsert=true skips,
// docIDPreserved=true with skipUpsert=false MUST fire, and existing==nil DELETE
// skips. Without case 2, "simplifying" the skip condition drops real writes.
func TestShard_ChangeLog_SkipPaths_NoEntry(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	log, err := shard.ActivateChangeLog("op-skips")
	require.NoError(t, err)

	id := uuid.NewString()

	// Case 1: identical PUT → skipUpsert=true → no tee.
	require.NoError(t, shard.PutObject(ctx, changelogTestObject(id, "same", 100)))
	require.NoError(t, shard.PutObject(ctx, changelogTestObject(id, "same", 100)))

	// Case 2: same id+vector, different property → docIDPreserved=true, skipUpsert=false → MUST tee.
	require.NoError(t, shard.PutObject(ctx, changelogTestObject(id, "different", 150)))

	// Case 3: DELETE on nonexistent UUID → existing==nil early-return → no tee.
	require.NoError(t, shard.DeleteObject(ctx, strfmt.UUID(uuid.NewString()), time.UnixMilli(200)))

	finalLSN, err := shard.FinalizeChangeLog("op-skips")
	require.NoError(t, err)
	require.Equal(t, uint64(2), finalLSN)

	entries := drainAllEntries(t, log)
	require.Len(t, entries, 2)
	require.False(t, entries[0].IsDelete)
	require.False(t, entries[1].IsDelete, "docIDPreserved PUT must appear as entry 2")

	require.NoError(t, shard.StopChangeCapture("op-skips"))
}

// Hangs on lock-order regression. The only assertion is the 10s timeout —
// if quiesceMux ends up nested inside docIdLock, this deadlocks.
func TestShard_ChangeLog_LockOrder_NoDeadlock(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	_, err := shard.ActivateChangeLog("op-lockorder")
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
	_, err = shard.FinalizeChangeLog("op-lockorder")
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

	require.NoError(t, shard.StopChangeCapture("op-lockorder"))
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

	_, err := shard.ActivateChangeLog("op-live")
	require.NoError(t, err)
	livePath := filepath.Join(dir, "op-live.log")

	orphan1 := filepath.Join(dir, "op-orphan-1.log")
	orphan2 := filepath.Join(dir, "op-orphan-2.log")
	keepNonLog := filepath.Join(dir, "keep.meta")
	require.NoError(t, os.WriteFile(orphan1, []byte("o1"), 0o600))
	require.NoError(t, os.WriteFile(orphan2, []byte("o2"), 0o600))
	require.NoError(t, os.WriteFile(keepNonLog, []byte("k"), 0o600))

	_, err = shard.ActivateChangeLog("op-new")
	require.NoError(t, err)

	_, err = os.Stat(orphan1)
	require.True(t, os.IsNotExist(err), "orphan-1 must be swept on activate")
	_, err = os.Stat(orphan2)
	require.True(t, os.IsNotExist(err), "orphan-2 must be swept on activate")
	_, err = os.Stat(livePath)
	require.NoError(t, err, "live op's file must not be swept")
	_, err = os.Stat(keepNonLog)
	require.NoError(t, err, "non-.log files must not be swept")

	require.NoError(t, shard.StopChangeCapture("op-live"))
	require.NoError(t, shard.StopChangeCapture("op-new"))
}

// SnapshotChangeLogLSN must reflect every committed append AND keep the log
// writable past the snapshot — the single-log movement design relies on
// "drain up to N" without sealing.
func TestShard_SnapshotChangeLogLSN_ReflectsAppendsAndStaysWritable(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	_, err := shard.ActivateChangeLog("op-snapshot")
	require.NoError(t, err)

	const before = 4
	for i := range before {
		require.NoError(t, shard.PutObject(ctx,
			changelogTestObject(uuid.NewString(), "x", int64(1_000+i))))
	}

	snap, err := shard.SnapshotChangeLogLSN("op-snapshot")
	require.NoError(t, err)
	require.Equal(t, uint64(before), snap, "snapshot must reflect all committed appends")

	// Log is still writable: subsequent appends get LSNs > snap.
	require.NoError(t, shard.PutObject(ctx,
		changelogTestObject(uuid.NewString(), "after-snap", 9_999)))
	finalLSN, err := shard.FinalizeChangeLog("op-snapshot")
	require.NoError(t, err)
	require.Equal(t, uint64(before+1), finalLSN, "writes after snapshot must keep advancing the LSN")

	require.NoError(t, shard.StopChangeCapture("op-snapshot"))
}

// Unknown op-id must error rather than return zero — otherwise a typo from
// the gRPC layer would surface as a successful "snapshot" of LSN 0.
func TestShard_SnapshotChangeLogLSN_NoSuchLog(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	_, err := shard.SnapshotChangeLogLSN("op-never-activated")
	require.Error(t, err)
	require.ErrorIs(t, err, errNoSuchChangeLog)
}

// Snapshot post-Finalize must be a pure read of finalLSN — retries on the
// consumer side may legitimately observe a finalized log.
func TestShard_SnapshotChangeLogLSN_AfterFinalize(t *testing.T) {
	ctx := context.Background()
	shard := setupChangelogTestShard(t, ctx)

	_, err := shard.ActivateChangeLog("op-after-final")
	require.NoError(t, err)

	const k = 3
	for i := range k {
		require.NoError(t, shard.PutObject(ctx,
			changelogTestObject(uuid.NewString(), "x", int64(i+1))))
	}
	finalLSN, err := shard.FinalizeChangeLog("op-after-final")
	require.NoError(t, err)
	require.Equal(t, uint64(k), finalLSN)

	snap, err := shard.SnapshotChangeLogLSN("op-after-final")
	require.NoError(t, err)
	require.Equal(t, finalLSN, snap)

	// And again, to confirm it is purely a read.
	snap2, err := shard.SnapshotChangeLogLSN("op-after-final")
	require.NoError(t, err)
	require.Equal(t, finalLSN, snap2)

	require.NoError(t, shard.StopChangeCapture("op-after-final"))
}
