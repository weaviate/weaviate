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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// newSharedSlotShard is newQueuedInitShard on a caller-supplied scheduler, so several shards queue on the same slot.
func newSharedSlotShard(t *testing.T, ctx context.Context, class string, sched *AsyncReplicationScheduler, hooks ...logrus.Hook) (ShardLike, *Shard, *Index, *test.Hook) {
	t.Helper()
	logger, hook := test.NewNullLogger()
	for _, h := range hooks {
		logger.AddHook(h)
	}
	sl, idx := testShard(t, ctx, class, func(idx *Index) {
		idx.asyncReplicationScheduler = sched
		idx.logger = logger
	})
	s := concreteShard(t, sl)
	t.Cleanup(func() {
		if err := sl.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return sl, s, idx, hook
}

// parkScanInMemtablePass enables async replication on a shard whose slot is free and blocks until its scan is parked inside the memtable pass with the gate armed.
func parkScanInMemtablePass(t *testing.T, ctx context.Context, s *Shard, hook *parkingLogHook) {
	t.Helper()
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	select {
	case <-hook.parked:
	case <-time.After(initGateWriteTimeout):
		t.Fatal("init scan did not reach its memtable pass")
	}
	ch := gateChannel(s)
	require.NotNil(t, ch, "gate must be armed during the memtable pass")
	select {
	case <-ch:
		t.Fatal("gate closed while the memtable pass was still running")
	default:
	}
}

func referenceRootAtHeight(t *testing.T, entries map[strfmt.UUID]int64, height int) hashtree.Digest {
	t.Helper()
	ht, err := hashtree.NewHashTree(height)
	require.NoError(t, err)
	for id, updateTime := range entries {
		idBytes, err := bytesFromUUID(id)
		require.NoError(t, err)
		require.NoError(t, aggregateHashTreeLeaf(ht, height, idBytes, updateTime))
	}
	return ht.Root()
}

func serialRebuildRootWith(t *testing.T, ctx context.Context, s *Shard, cfg AsyncReplicationConfig) hashtree.Digest {
	t.Helper()
	require.NoError(t, s.disableAsyncReplication(ctx))
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)
	return liveRoot(t, s)
}

func liveHeight(t *testing.T, s *Shard) int {
	t.Helper()
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()
	require.NotNil(t, s.hashtree)
	return s.hashtree.Height()
}

func replicaResponseErr(resp interface{}) error {
	switch r := resp.(type) {
	case replica.SimpleResponse:
		for i := range r.Errors {
			if !r.Errors[i].Empty() {
				return fmt.Errorf("replica error: %s", r.Errors[i].Msg)
			}
		}
		return nil
	case replica.DeleteBatchResponse:
		for i := range r.Batch {
			if !r.Batch[i].Error.Empty() {
				return fmt.Errorf("replica batch error: %s", r.Batch[i].Error.Msg)
			}
		}
		return nil
	case nil:
		return fmt.Errorf("commit found no prepared task")
	}
	return fmt.Errorf("unexpected replica response %T", resp)
}

type queuedShard struct {
	sl       ShardLike
	s        *Shard
	idx      *Index
	hook     *test.Hook
	class    string
	expected map[strfmt.UUID]int64
}

func TestWritesToQueuedShardsFlowWhileAnotherShardScans(t *testing.T) {
	ctx := context.Background()
	const t0, t1 = tsFarPast, tsFarPast + 1
	cfg := minAsyncReplicationConfig()

	sched := newSingleSlotScheduler(t)
	parkHook := newParkingLogHook("hashtree initialization in progress")
	slA, sA, _, _ := newSharedSlotShard(t, ctx, "MultiShardScanner", sched, parkHook)
	t.Cleanup(parkHook.release)
	scannerExpected := map[strfmt.UUID]int64{}
	for i := range 3 {
		id := leafUUID(i%2 == 1, 900+i)
		require.NoError(t, slA.PutObject(ctx, testObjWithTime("MultiShardScanner", id, t0)))
		scannerExpected[id] = t0
	}

	queued := make([]*queuedShard, 3)
	for i := range queued {
		class := fmt.Sprintf("MultiShardQueued%d", i)
		sl, s, idx, hook := newSharedSlotShard(t, ctx, class, sched)
		queued[i] = &queuedShard{sl: sl, s: s, idx: idx, hook: hook, class: class, expected: map[strfmt.UUID]int64{}}
	}

	type journey struct {
		name      string
		seed      map[int]int64
		write     func(q *queuedShard, u []strfmt.UUID) error
		survivors map[int]int64
	}
	commit := func(q *queuedShard, name string, prepare func(reqID string) replica.SimpleResponse) error {
		reqID := "req-" + name
		if resp := prepare(reqID); len(resp.Errors) > 0 {
			return fmt.Errorf("prepare: %s", resp.Errors[0].Msg)
		}
		return replicaResponseErr(q.s.commitReplication(ctx, reqID))
	}
	overwrite := func(q *queuedShard, updates []*objects.VObject) error {
		res, err := q.idx.OverwriteObjects(ctx, q.s.name, updates)
		if err != nil {
			return err
		}
		if len(res) > 0 {
			return fmt.Errorf("overwrite %s: %s", res[0].ID, res[0].Err)
		}
		return nil
	}
	rows := []journey{
		{
			name: "putObject",
			write: func(q *queuedShard, u []strfmt.UUID) error {
				return q.sl.PutObject(ctx, testObjWithTime(q.class, u[0], t1))
			},
			survivors: map[int]int64{0: t1},
		},
		{
			name: "putObjectBatch",
			seed: map[int]int64{0: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error {
				for _, err := range q.sl.PutObjectBatch(ctx, []*storobj.Object{testObjWithTime(q.class, u[1], t1), testObjWithTime(q.class, u[0], t1)}) {
					if err != nil {
						return err
					}
				}
				return nil
			},
			survivors: map[int]int64{0: t1, 1: t1},
		},
		{
			name: "mergeObject",
			seed: map[int]int64{0: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error {
				return q.s.MergeObject(ctx, objects.MergeDocument{Class: q.class, ID: u[0], UpdateTime: t1})
			},
			survivors: map[int]int64{0: t1},
		},
		{
			name: "mutableMergeObjectLSM",
			seed: map[int]int64{0: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error {
				idBytes, err := bytesFromUUID(u[0])
				if err != nil {
					return err
				}
				_, err = q.s.mutableMergeObjectLSM(ctx, objects.MergeDocument{Class: q.class, ID: u[0], UpdateTime: t1}, idBytes)
				return err
			},
			survivors: map[int]int64{0: t1},
		},
		{
			name:  "deleteObjectZeroTime",
			seed:  map[int]int64{0: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error { return q.sl.DeleteObject(ctx, u[0], time.Time{}) },
		},
		{
			name:  "deleteObjectTimestamped",
			seed:  map[int]int64{0: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error { return q.sl.DeleteObject(ctx, u[0], time.Now()) },
		},
		{
			name: "deleteObjectBatch",
			seed: map[int]int64{0: t0, 1: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error {
				for _, res := range q.sl.DeleteObjectBatch(ctx, []strfmt.UUID{u[0], u[1]}, time.Now(), false) {
					if res.Err != nil {
						return res.Err
					}
				}
				return nil
			},
		},
		{
			name: "replicationPutObject",
			write: func(q *queuedShard, u []strfmt.UUID) error {
				return commit(q, "put", func(reqID string) replica.SimpleResponse {
					return q.s.preparePutObject(ctx, reqID, testObjWithTime(q.class, u[0], t1))
				})
			},
			survivors: map[int]int64{0: t1},
		},
		{
			name: "replicationPutObjects",
			seed: map[int]int64{0: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error {
				return commit(q, "puts", func(reqID string) replica.SimpleResponse {
					return q.s.preparePutObjects(ctx, reqID, []*storobj.Object{testObjWithTime(q.class, u[0], t1), testObjWithTime(q.class, u[1], t1)})
				})
			},
			survivors: map[int]int64{0: t1, 1: t1},
		},
		{
			name: "replicationMergeObject",
			seed: map[int]int64{0: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error {
				return commit(q, "merge", func(reqID string) replica.SimpleResponse {
					return q.s.prepareMergeObject(ctx, reqID, &objects.MergeDocument{Class: q.class, ID: u[0], UpdateTime: t1})
				})
			},
			survivors: map[int]int64{0: t1},
		},
		{
			name: "replicationDeleteObject",
			seed: map[int]int64{0: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error {
				return commit(q, "delete", func(reqID string) replica.SimpleResponse {
					return q.s.prepareDeleteObject(ctx, reqID, u[0], time.Now())
				})
			},
		},
		{
			name: "replicationDeleteObjects",
			seed: map[int]int64{0: t0, 1: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error {
				return commit(q, "deletes", func(reqID string) replica.SimpleResponse {
					return q.s.prepareDeleteObjects(ctx, reqID, []strfmt.UUID{u[0], u[1]}, time.Now(), false)
				})
			},
		},
		{
			name: "overwriteObjectsPut",
			seed: map[int]int64{0: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error {
				return overwrite(q, []*objects.VObject{
					{ID: u[0], LastUpdateTimeUnixMilli: t1, StaleUpdateTime: t0, LatestObject: &models.Object{ID: u[0], Class: q.class, LastUpdateTimeUnix: t1}},
					{ID: u[1], LastUpdateTimeUnixMilli: t1, LatestObject: &models.Object{ID: u[1], Class: q.class, LastUpdateTimeUnix: t1}},
				})
			},
			survivors: map[int]int64{0: t1, 1: t1},
		},
		{
			name: "overwriteObjectsDelete",
			seed: map[int]int64{0: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error {
				return overwrite(q, []*objects.VObject{{ID: u[0], Deleted: true, LastUpdateTimeUnixMilli: t1, StaleUpdateTime: t0}})
			},
		},
		{
			name: "objectByID",
			seed: map[int]int64{0: t0},
			write: func(q *queuedShard, u []strfmt.UUID) error {
				obj, err := q.s.ObjectByID(ctx, u[0], nil, additional.Properties{})
				if err != nil {
					return err
				}
				if obj == nil {
					return fmt.Errorf("object %s not found", u[0])
				}
				return nil
			},
			survivors: map[int]int64{0: t0},
		},
	}

	rowIDs := make([][]strfmt.UUID, len(rows))
	for i, row := range rows {
		q := queued[i%len(queued)]
		rowIDs[i] = []strfmt.UUID{leafUUID(i%2 == 1, i*10+1), leafUUID(i%2 == 0, i*10+2)}
		for k, ts := range row.seed {
			require.NoError(t, q.sl.PutObject(ctx, testObjWithTime(q.class, rowIDs[i][k], ts)))
		}
	}
	for i, q := range queued {
		flushShard(t, ctx, q.sl)
		unflushed := leafUUID(true, 800+i)
		require.NoError(t, q.sl.PutObject(ctx, testObjWithTime(q.class, unflushed, t0)))
		q.expected[unflushed] = t0
	}

	parkScanInMemtablePass(t, ctx, sA, parkHook)
	for _, q := range queued {
		require.NoError(t, q.s.enableAsyncReplication(ctx, cfg))
		requireQueuedInitState(t, q.s)
	}

	for i, row := range rows {
		q := queued[i%len(queued)]
		u := rowIDs[i]
		assertCompletesWithin(t, initGateWriteTimeout, row.name, func() error { return row.write(q, u) })
		for k, ts := range row.survivors {
			q.expected[u[k]] = ts
		}
	}

	for _, q := range queued {
		requireQueuedInitState(t, q.s)
		require.False(t, hookHasMessage(q.hook, "hashtree successfully initialized"))
	}

	parkHook.release()
	awaitHashtreeInitialized(t, sA)
	require.Equal(t, referenceRoot(t, scannerExpected), liveRoot(t, sA))
	for _, q := range queued {
		awaitHashtreeInitialized(t, q.s)
		got := liveRoot(t, q.s)
		require.Equal(t, referenceRoot(t, q.expected), got, "%s: every write applied while queued must be folded exactly once", q.class)
		require.Equal(t, serialRebuildRoot(t, ctx, q.s), got, q.class)
	}
}

func TestHeightChangeWhileHashtreeInitQueued(t *testing.T) {
	ctx := context.Background()
	const t0, t1, t2 = tsFarPast, tsFarPast + 1, tsFarPast + 2
	cfg1 := minAsyncReplicationConfig()
	cfg2 := cfg1
	cfg2.hashtreeHeight = 2
	extra := leafUUID(true, 42)

	seed := func(t *testing.T, sl ShardLike, class string) map[strfmt.UUID]int64 {
		t.Helper()
		expected := map[strfmt.UUID]int64{}
		for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
			require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, t0)))
			expected[id] = t0
		}
		flushShard(t, ctx, sl)
		return expected
	}
	writeWhileQueued := func(t *testing.T, sl ShardLike, s *Shard, class string, expected map[strfmt.UUID]int64) {
		t.Helper()
		assertCompletesWithin(t, initGateWriteTimeout, "insert", func() error { return sl.PutObject(ctx, testObjWithTime(class, extra, t1)) })
		assertCompletesWithin(t, initGateWriteTimeout, "merge", func() error {
			return s.MergeObject(ctx, objects.MergeDocument{Class: class, ID: uuidLow, UpdateTime: t2})
		})
		assertCompletesWithin(t, initGateWriteTimeout, "delete", func() error { return sl.DeleteObject(ctx, uuidHigh, time.Now()) })
		expected[extra] = t1
		expected[uuidLow] = t2
		delete(expected, uuidHigh)
		requireQueuedInitState(t, s)
	}
	assertConverged := func(t *testing.T, s *Shard, expected map[strfmt.UUID]int64) {
		t.Helper()
		awaitHashtreeInitialized(t, s)
		require.Equal(t, cfg2.hashtreeHeight, liveHeight(t, s), "the scan must build the tree at the effective height at scan start")
		got := liveRoot(t, s)
		require.Equal(t, referenceRootAtHeight(t, expected, cfg2.hashtreeHeight), got)
		require.Equal(t, serialRebuildRootWith(t, ctx, s, cfg2), got)
	}

	t.Run("configUpdatedWhileQueued", func(t *testing.T) {
		const class = "HeightChangeQueuedConfig"
		sl, s, sched, hook := newQueuedInitShard(t, ctx, class)
		expected := seed(t, sl, class)

		release := holdInitSlot(t, sched)
		require.NoError(t, s.enableAsyncReplication(ctx, cfg1))
		requireQueuedInitState(t, s)
		require.NoError(t, s.enableAsyncReplication(ctx, cfg2))
		writeWhileQueued(t, sl, s, class, expected)

		release()
		awaitHashtreeInitialized(t, s)
		require.Equal(t, 1, countMessages(hook, "hashtree successfully initialized"), "one scan must suffice after a height change while queued")
		assertConverged(t, s, expected)
	})

	t.Run("rebuildFromScratchWhileQueued", func(t *testing.T) {
		const class = "HeightChangeQueuedRebuild"
		sl, s, sched, _ := newQueuedInitShard(t, ctx, class)
		expected := seed(t, sl, class)
		require.NoError(t, s.enableAsyncReplication(ctx, cfg1))
		awaitHashtreeInitialized(t, s)

		release := holdInitSlot(t, sched)
		require.NoError(t, s.rebuildAsyncReplicationFromScratch(ctx, true, cfg2))
		writeWhileQueued(t, sl, s, class, expected)

		release()
		assertConverged(t, s, expected)
	})

	t.Run("schedulerRebuildWhileQueued", func(t *testing.T) {
		const class = "HeightChangeQueuedScheduler"
		sched := newSingleSlotScheduler(t)
		sl, s, idx, _ := newSharedSlotShard(t, ctx, class, sched)
		expected := seed(t, sl, class)
		require.NoError(t, s.enableAsyncReplication(ctx, cfg1))
		awaitHashtreeInitialized(t, s)

		prevDrain := asyncReplicationWorkerDrainTimeout.Load()
		asyncReplicationWorkerDrainTimeout.Store(int64(2 * time.Second))
		t.Cleanup(func() { asyncReplicationWorkerDrainTimeout.Store(prevDrain) })
		idx.replicationConfigLock.Lock()
		idx.Config.ReplicationFactor = 3
		idx.Config.AsyncReplicationConfig = cfg2
		idx.replicationConfigLock.Unlock()

		release := holdInitSlot(t, sched)
		retry, _, rebuilt, yielded := sched.tryRebuildHashtree(s)
		require.False(t, retry)
		require.False(t, yielded)
		require.True(t, rebuilt)
		writeWhileQueued(t, sl, s, class, expected)

		release()
		assertConverged(t, s, expected)
	})
}

func TestCancelledHashtreeInitAttemptIsNotCountedAsFailure(t *testing.T) {
	ctx := context.Background()
	const class = "InitCancelledMetrics"

	hook := newParkingLogHook("hashtree initialization in progress")
	sl, s, sched, _ := newQueuedInitShard(t, ctx, class, hook)
	t.Cleanup(hook.release)
	m, err := NewMetrics(s.index.logger, monitoring.GetMetrics(), class, s.name)
	require.NoError(t, err)
	s.metrics = m
	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
	flushShard(t, ctx, sl)
	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidMid, tsFarPast)))

	initsBefore := testutil.ToFloat64(m.asyncReplicationHashTreeInitCount)
	failuresBefore := testutil.ToFloat64(m.asyncReplicationHashTreeInitFailureCount)
	runningBefore := testutil.ToFloat64(m.asyncReplicationHashTreeInitRunning)
	queuedBefore := testutil.ToFloat64(m.asyncReplicationHashTreeInitQueued)

	release := holdInitSlot(t, sched)
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	requireQueuedInitState(t, s)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(m.asyncReplicationHashTreeInitQueued) == queuedBefore+1
	}, initGateWriteTimeout, 10*time.Millisecond, "the queued init must be counted once it waits for the slot")
	require.Equal(t, initsBefore, testutil.ToFloat64(m.asyncReplicationHashTreeInitCount))
	release()

	select {
	case <-hook.parked:
	case <-time.After(initGateWriteTimeout):
		t.Fatal("init scan did not reach its memtable pass")
	}
	require.Equal(t, queuedBefore, testutil.ToFloat64(m.asyncReplicationHashTreeInitQueued))
	require.Equal(t, initsBefore+1, testutil.ToFloat64(m.asyncReplicationHashTreeInitCount))
	require.Equal(t, runningBefore+1, testutil.ToFloat64(m.asyncReplicationHashTreeInitRunning))

	require.NoError(t, s.disableAsyncReplication(ctx))
	hook.release()
	drained := make(chan struct{})
	go func() { s.asyncRepWg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(initGateWriteTimeout):
		t.Fatal("cancelled init attempt did not exit")
	}
	require.Equal(t, failuresBefore, testutil.ToFloat64(m.asyncReplicationHashTreeInitFailureCount), "a cancelled scan is a stop, not a failure")
	require.Equal(t, runningBefore, testutil.ToFloat64(m.asyncReplicationHashTreeInitRunning))

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)
	require.ErrorIs(t, s.initHashtree(cancelled, minAsyncReplicationConfig(), bucket), context.Canceled)
	require.Equal(t, initsBefore+1, testutil.ToFloat64(m.asyncReplicationHashTreeInitCount), "an attempt cancelled before it starts is not an init")
	require.Equal(t, failuresBefore, testutil.ToFloat64(m.asyncReplicationHashTreeInitFailureCount))
	require.Equal(t, runningBefore, testutil.ToFloat64(m.asyncReplicationHashTreeInitRunning))
}

func TestFlapDuringHashtreeMemtablePassReleasesParkedWrites(t *testing.T) {
	ctx := context.Background()
	const class = "InitMemtablePassFlap"
	const t0, t1 = tsFarPast, tsFarPast + 1
	extra := leafUUID(false, 77)

	hook := newParkingLogHook("hashtree initialization in progress")
	sl, s, sched, _ := newQueuedInitShard(t, ctx, class, hook)
	t.Cleanup(hook.release)

	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, t0)))
	flushShard(t, ctx, sl)
	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidMid, t0)))

	parkScanInMemtablePass(t, ctx, s, hook)

	parkedWrite := make(chan error, 1)
	go func() { parkedWrite <- sl.PutObject(ctx, testObjWithTime(class, uuidHigh, t1)) }()
	select {
	case <-parkedWrite:
		t.Fatal("write completed while the memtable pass was still running")
	case <-time.After(initGateParkProbe):
	}

	require.NoError(t, s.disableAsyncReplication(ctx))
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	requireQueuedInitState(t, s)
	if sched.hashtreeInitSem.TryAcquire(1) {
		sched.hashtreeInitSem.Release(1)
		t.Fatal("the parked scan must still hold the only slot")
	}

	lateWrite := make(chan error, 1)
	go func() { lateWrite <- sl.PutObject(ctx, testObjWithTime(class, extra, t1)) }()
	select {
	case <-lateWrite:
		t.Fatal("a write issued after the flap completed while the cancelled scan still held the memtable")
	case <-parkedWrite:
		t.Fatal("the parked write completed while the cancelled scan still held the memtable")
	case <-time.After(initGateParkProbe):
	}

	hook.release()
	for name, done := range map[string]chan error{"parked": parkedWrite, "late": lateWrite} {
		select {
		case err := <-done:
			require.NoError(t, err, name)
		case <-time.After(initGateWriteTimeout):
			t.Fatalf("%s write did not complete after the cancelled scan released the memtable", name)
		}
	}

	awaitHashtreeInitialized(t, s)
	expected := map[strfmt.UUID]int64{uuidLow: t0, uuidMid: t0, uuidHigh: t1, extra: t1}
	got := liveRoot(t, s)
	require.Equal(t, referenceRoot(t, expected), got, "writes released by a cancelled scan must fold exactly once into the next attempt")
	require.Equal(t, serialRebuildRoot(t, ctx, s), got)
}
