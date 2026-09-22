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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	entreplication "github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/storobj"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const (
	initGateWriteTimeout = 5 * time.Second
	initGateParkProbe    = 200 * time.Millisecond
	initGateWatchdog     = 30 * time.Second
)

func newSingleSlotScheduler(t *testing.T) *AsyncReplicationScheduler {
	t.Helper()
	logger, _ := test.NewNullLogger()
	sched, err := NewAsyncReplicationScheduler(context.Background(), entreplication.GlobalConfig{
		AsyncReplicationSchedulerWorkers:        configRuntime.NewDynamicValue(1),
		AsyncReplicationHashtreeInitConcurrency: configRuntime.NewDynamicValue(1),
		AsyncReplicationDisabled:                configRuntime.NewDynamicValue(false),
	}, nil, logger)
	require.NoError(t, err)
	t.Cleanup(sched.Close)
	return sched
}

func holdInitSlot(t *testing.T, sched *AsyncReplicationScheduler) (release func()) {
	t.Helper()
	require.NoError(t, sched.hashtreeInitSem.Acquire(context.Background(), 1))
	var once sync.Once
	release = func() { once.Do(func() { sched.hashtreeInitSem.Release(1) }) }
	t.Cleanup(release)
	return release
}

// newQueuedInitShard builds a shard on a one-slot scheduler with a log hook, so a test can hold the slot and observe init messages.
func newQueuedInitShard(t *testing.T, ctx context.Context, class string, hooks ...logrus.Hook) (ShardLike, *Shard, *AsyncReplicationScheduler, *test.Hook) {
	t.Helper()
	sched := newSingleSlotScheduler(t)
	sl, s, _, hook := newSharedSlotShard(t, ctx, class, sched, hooks...)
	return sl, s, sched, hook
}

func assertCompletesWithin(t *testing.T, d time.Duration, name string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		require.NoError(t, err, name)
	case <-time.After(d):
		t.Fatalf("%s did not complete within %s: write blocked on hashtree init", name, d)
	}
}

func referenceRoot(t *testing.T, entries map[strfmt.UUID]int64) hashtree.Digest {
	t.Helper()
	return referenceRootAtHeight(t, entries, minAsyncReplicationConfig().hashtreeHeight)
}

func liveRoot(t *testing.T, s *Shard) hashtree.Digest {
	t.Helper()
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()
	require.NotNil(t, s.hashtree)
	return s.hashtree.Root()
}

func serialRebuildRoot(t *testing.T, ctx context.Context, s *Shard) hashtree.Digest {
	t.Helper()
	return serialRebuildRootWith(t, ctx, s, minAsyncReplicationConfig())
}

func gateChannel(s *Shard) chan struct{} {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()
	return s.minimalHashtreeInitializationCh
}

func requireQueuedInitState(t *testing.T, s *Shard) {
	t.Helper()
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()
	require.NotNil(t, s.hashtree)
	require.False(t, s.hashtreeFullyInitialized)
	require.Nil(t, s.minimalHashtreeInitializationCh, "gate must stay disarmed while init is queued for a slot")
}

func countMessages(hook *test.Hook, substr string) int {
	n := 0
	for _, e := range hook.AllEntries() {
		if strings.Contains(e.Message, substr) {
			n++
		}
	}
	return n
}

func hookHasMessage(hook *test.Hook, substr string) bool {
	return countMessages(hook, substr) > 0
}

func leafUUID(high bool, n int) strfmt.UUID {
	prefix := "0"
	if high {
		prefix = "f"
	}
	return strfmt.UUID(fmt.Sprintf("%s0000000-0000-0000-0000-%012d", prefix, n))
}

func testObjWithTimeAndProps(class string, id strfmt.UUID, updateTime int64) *storobj.Object {
	obj := testObjWithTime(class, id, updateTime)
	obj.Object.Properties = map[string]interface{}{}
	return obj
}

// parkingLogHook parks the goroutine that logs the first matching message until release is called; one-shot.
type parkingLogHook struct {
	match       string
	parked      chan struct{}
	resume      chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func newParkingLogHook(match string) *parkingLogHook {
	return &parkingLogHook{match: match, parked: make(chan struct{}), resume: make(chan struct{})}
}

func (h *parkingLogHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h *parkingLogHook) Fire(e *logrus.Entry) error {
	if strings.Contains(e.Message, h.match) {
		h.once.Do(func() {
			close(h.parked)
			<-h.resume
		})
	}
	return nil
}

func (h *parkingLogHook) release() { h.releaseOnce.Do(func() { close(h.resume) }) }

func TestWritesDoNotBlockWhileHashtreeInitQueued(t *testing.T) {
	ctx := context.Background()
	const class = "InitQueuedWrites"
	const t0, t1, t2, t3 = tsFarPast, tsFarPast + 1, tsFarPast + 2, tsFarPast + 3

	sl, s, sched, hook := newQueuedInitShard(t, ctx, class)

	type journey struct {
		name      string
		seed      map[int]int64
		seedProps bool
		write     func(ids []strfmt.UUID) error
		survivors map[int]int64
	}
	ids := func(row, n int) []strfmt.UUID {
		out := make([]strfmt.UUID, n)
		for k := range out {
			out[k] = leafUUID((row+k)%2 == 1, row*10+k+1)
		}
		return out
	}
	firstErr := func(errs []error) error {
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		return nil
	}
	rows := []journey{
		{
			name:      "insertNew",
			write:     func(u []strfmt.UUID) error { return sl.PutObject(ctx, testObjWithTime(class, u[0], t1)) },
			survivors: map[int]int64{0: t1},
		},
		{
			name:      "updateDiskResidentNilProps",
			seed:      map[int]int64{0: t0},
			write:     func(u []strfmt.UUID) error { return sl.PutObject(ctx, testObjWithTime(class, u[0], t1)) },
			survivors: map[int]int64{0: t1},
		},
		{
			name:      "updateDiskResidentSameProps",
			seed:      map[int]int64{0: t0},
			seedProps: true,
			write:     func(u []strfmt.UUID) error { return sl.PutObject(ctx, testObjWithTimeAndProps(class, u[0], t1)) },
			survivors: map[int]int64{0: t1},
		},
		{
			name: "putBatchMixed",
			seed: map[int]int64{0: t0},
			write: func(u []strfmt.UUID) error {
				return firstErr(sl.PutObjectBatch(ctx, []*storobj.Object{
					testObjWithTime(class, u[1], t1),
					testObjWithTime(class, u[0], t1),
				}))
			},
			survivors: map[int]int64{0: t1, 1: t1},
		},
		{
			name: "mergeObject",
			seed: map[int]int64{0: t0},
			write: func(u []strfmt.UUID) error {
				return s.MergeObject(ctx, objects.MergeDocument{Class: class, ID: u[0], UpdateTime: t1})
			},
			survivors: map[int]int64{0: t1},
		},
		{
			name: "mutableMerge",
			seed: map[int]int64{0: t0},
			write: func(u []strfmt.UUID) error {
				idBytes, err := bytesFromUUID(u[0])
				if err != nil {
					return err
				}
				_, err = s.mutableMergeObjectLSM(ctx, objects.MergeDocument{Class: class, ID: u[0], UpdateTime: t1}, idBytes)
				return err
			},
			survivors: map[int]int64{0: t1},
		},
		{
			name:  "deleteDiskResident",
			seed:  map[int]int64{0: t0},
			write: func(u []strfmt.UUID) error { return sl.DeleteObject(ctx, u[0], time.Now()) },
		},
		{
			name: "deleteBatch",
			seed: map[int]int64{0: t0, 1: t0},
			write: func(u []strfmt.UUID) error {
				for _, res := range sl.DeleteObjectBatch(ctx, []strfmt.UUID{u[0], u[1]}, time.Now(), false) {
					if res.Err != nil {
						return res.Err
					}
				}
				return nil
			},
		},
		{
			name: "putThenDelete",
			write: func(u []strfmt.UUID) error {
				if err := sl.PutObject(ctx, testObjWithTime(class, u[0], t1)); err != nil {
					return err
				}
				return sl.DeleteObject(ctx, u[0], time.Now())
			},
		},
		{
			name: "deleteThenReinsert",
			seed: map[int]int64{0: t0},
			write: func(u []strfmt.UUID) error {
				if err := sl.DeleteObject(ctx, u[0], time.Now()); err != nil {
					return err
				}
				return sl.PutObject(ctx, testObjWithTime(class, u[0], t2))
			},
			survivors: map[int]int64{0: t2},
		},
		{
			name: "updateChain",
			seed: map[int]int64{0: t0},
			write: func(u []strfmt.UUID) error {
				for _, ts := range []int64{t1, t2, t3} {
					if err := sl.PutObject(ctx, testObjWithTime(class, u[0], ts)); err != nil {
						return err
					}
				}
				return nil
			},
			survivors: map[int]int64{0: t3},
		},
	}

	rowIDs := make([][]strfmt.UUID, len(rows))
	for i, row := range rows {
		rowIDs[i] = ids(i, 2)
		for k, ts := range row.seed {
			obj := testObjWithTime(class, rowIDs[i][k], ts)
			if row.seedProps {
				obj = testObjWithTimeAndProps(class, rowIDs[i][k], ts)
			}
			require.NoError(t, sl.PutObject(ctx, obj))
		}
	}
	flushShard(t, ctx, sl)

	release := holdInitSlot(t, sched)
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	requireQueuedInitState(t, s)

	expected := map[strfmt.UUID]int64{}
	for i, row := range rows {
		u := rowIDs[i]
		assertCompletesWithin(t, initGateWriteTimeout, row.name, func() error { return row.write(u) })
		for k, ts := range row.survivors {
			expected[u[k]] = ts
		}
	}

	requireQueuedInitState(t, s)
	require.False(t, hookHasMessage(hook, "hashtree successfully initialized"))

	release()
	awaitHashtreeInitialized(t, s)

	got := liveRoot(t, s)
	require.Equal(t, referenceRoot(t, expected), got, "every write applied while queued must be folded exactly once")
	require.Equal(t, serialRebuildRoot(t, ctx, s), got)
}

func TestConcurrentWritesWhileHashtreeInitQueuedThenReleased(t *testing.T) {
	const n = 240
	const writers = 4
	const t0, t1 = tsFarPast, tsFarPast + 5

	tests := []struct {
		name         string
		flushSeedAt  int
		releaseAfter int64
	}{
		{name: "seedHalfFlushed", flushSeedAt: n / 2, releaseAfter: n / 2},
		{name: "seedUnflushed", flushSeedAt: -1, releaseAfter: n / 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			class := "InitQueuedConcurrent" + tc.name

			uuids := make([]strfmt.UUID, n)
			for i := range uuids {
				uuids[i] = leafUUID(i%2 == 1, i+1)
			}

			sl, s, sched, _ := newQueuedInitShard(t, ctx, class)
			for i := range n {
				require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuids[i], t0)))
				if i == tc.flushSeedAt {
					require.NoError(t, s.store.FlushMemtables(ctx))
				}
			}

			release := holdInitSlot(t, sched)
			require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
			requireQueuedInitState(t, s)

			var (
				progress atomic.Int64
				errMu    sync.Mutex
				errs     []error
				wg       sync.WaitGroup
			)
			record := func(err error) {
				if err != nil {
					errMu.Lock()
					errs = append(errs, err)
					errMu.Unlock()
				}
			}
			for w := range writers {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := w; i < n; i += writers {
						switch i % 4 {
						case 0:
							record(sl.PutObject(ctx, testObjWithTime(class, uuids[i], t1)))
						case 1:
							record(sl.DeleteObject(ctx, uuids[i], time.Now()))
						case 2:
							record(sl.PutObject(ctx, testObjWithTime(class, uuids[i], t1)))
							record(s.store.FlushMemtables(ctx))
						case 3:
							record(s.MergeObject(ctx, objects.MergeDocument{Class: class, ID: uuids[i], UpdateTime: t1}))
						}
						if progress.Add(1) == tc.releaseAfter {
							release()
						}
					}
				}(w)
			}

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(initGateWatchdog):
				t.Fatalf("writers did not finish within %s: writes blocked while hashtree init was queued", initGateWatchdog)
			}
			require.Empty(t, errs)

			awaitHashtreeInitialized(t, s)

			expected := map[strfmt.UUID]int64{}
			for i := range uuids {
				if i%4 != 1 {
					expected[uuids[i]] = t1
				}
			}
			got := liveRoot(t, s)
			require.Equal(t, referenceRoot(t, expected), got)
			require.Equal(t, serialRebuildRoot(t, ctx, s), got)
		})
	}
}

func TestConcurrentWritesUnderFlappingWhileHashtreeInitQueued(t *testing.T) {
	const (
		writers    = 8
		iterations = 150
	)

	ids := []strfmt.UUID{uuidLow, uuidMid, uuidHigh}

	for _, tc := range asyncWriteJourneys() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			sl, s, sched, _ := newQueuedInitShard(t, ctx, "InitQueuedFlapping"+tc.name)

			for _, id := range ids {
				require.NoError(t, sl.PutObject(ctx, testObjWithTime(s.class.Class, id, tsFarPast)))
			}
			require.NoError(t, s.store.FlushMemtables(ctx))

			release := holdInitSlot(t, sched)
			cfg := minAsyncReplicationConfig()
			require.NoError(t, s.enableAsyncReplication(ctx, cfg))

			var (
				clock      atomic.Int64
				writeErrs  atomic.Int64
				toggleErrs atomic.Int64
				wg         sync.WaitGroup
			)
			clock.Store(tsFarPast)

			for w := range writers {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for range iterations {
						if err := tc.write(s, ids[w%len(ids)], clock.Add(1)); err != nil {
							writeErrs.Add(1)
						}
					}
				}(w)
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range iterations {
					if err := s.disableAsyncReplication(ctx); err != nil {
						toggleErrs.Add(1)
					}
					if err := s.enableAsyncReplication(ctx, cfg); err != nil {
						toggleErrs.Add(1)
					}
					if i == iterations/2 {
						release()
					}
				}
			}()

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(initGateWatchdog):
				t.Fatalf("%s writes did not finish within %s under flapping with a queued init", tc.name, initGateWatchdog)
			}

			if n := writeErrs.Load(); n > 0 {
				t.Logf("%d/%d writes returned a non-fatal error during flapping", n, writers*iterations)
			}
			if n := toggleErrs.Load(); n > 0 {
				t.Logf("%d enable/disable toggles returned a non-fatal error during flapping", n)
			}

			awaitHashtreeInitialized(t, s)
			got := liveRoot(t, s)
			require.Equal(t, serialRebuildRoot(t, ctx, s), got)
		})
	}
}

func TestWritesCompleteWhileHashtreeInitRetriesAndConvergeAfterRepair(t *testing.T) {
	ctx := context.Background()
	const class = "InitRetryWrites"
	const t0, t1, t2, t3 = tsFarPast, tsFarPast + 1, tsFarPast + 2, tsFarPast + 3
	extra := leafUUID(false, 42)

	sl, s, sched, hook := newQueuedInitShard(t, ctx, class)
	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, t0)))
	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidHigh, t0)))
	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidMid, 0)))
	flushShard(t, ctx, sl)

	release := holdInitSlot(t, sched)
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	requireQueuedInitState(t, s)
	release()

	const failureMsg = "hashtree initialization attempt"
	require.Eventually(t, func() bool { return hookHasMessage(hook, failureMsg+" 0 failure") },
		initGateWriteTimeout, 10*time.Millisecond)
	reholdRelease := holdInitSlot(t, sched)
	failuresWhenHeld := countMessages(hook, failureMsg)
	time.Sleep(3 * initRetryBackoff(failuresWhenHeld-1))

	writes := []struct {
		name  string
		write func() error
	}{
		{name: "insertNew", write: func() error { return sl.PutObject(ctx, testObjWithTime(class, extra, t1)) }},
		{name: "updateExisting", write: func() error { return sl.PutObject(ctx, testObjWithTime(class, uuidLow, t2)) }},
		{name: "deleteExisting", write: func() error { return sl.DeleteObject(ctx, uuidHigh, time.Now()) }},
		{name: "repairCorruptObject", write: func() error { return sl.PutObject(ctx, testObjWithTime(class, uuidMid, t3)) }},
	}
	for _, w := range writes {
		assertCompletesWithin(t, initGateWriteTimeout, w.name, w.write)
	}
	require.Equal(t, failuresWhenHeld, countMessages(hook, failureMsg), "no attempt may run while the slot is held")
	s.asyncReplicationRWMux.RLock()
	require.False(t, s.hashtreeFullyInitialized)
	s.asyncReplicationRWMux.RUnlock()

	reholdRelease()
	awaitHashtreeInitialized(t, s)
	require.Eventually(t, func() bool { return hookHasMessage(hook, "hashtree successfully initialized") },
		initGateWriteTimeout, 10*time.Millisecond)

	expected := map[strfmt.UUID]int64{extra: t1, uuidLow: t2, uuidMid: t3}
	got := liveRoot(t, s)
	require.Equal(t, referenceRoot(t, expected), got, "writes applied between failed attempts must be folded exactly once")
	require.Equal(t, serialRebuildRoot(t, ctx, s), got)
}

func TestWritesWaitDuringHashtreeMemtablePass(t *testing.T) {
	ctx := context.Background()
	const class = "InitMemtablePassWrites"
	const t0, t1 = tsFarPast, tsFarPast + 1

	hook := newParkingLogHook("hashtree initialization in progress")
	sl, s, sched, _ := newQueuedInitShard(t, ctx, class, hook)
	t.Cleanup(hook.release)

	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, t0)))
	flushShard(t, ctx, sl)
	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidMid, t0)))

	release := holdInitSlot(t, sched)
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	requireQueuedInitState(t, s)
	release()

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

	writeDone := make(chan error, 1)
	go func() { writeDone <- sl.PutObject(ctx, testObjWithTime(class, uuidHigh, t1)) }()
	gateDone := make(chan error, 1)
	go func() { gateDone <- s.waitForMinimalHashTreeInitialization(ctx) }()
	select {
	case <-writeDone:
		t.Fatal("write completed while the memtable pass was still running")
	case <-gateDone:
		t.Fatal("gate opened while the memtable pass was still running")
	case <-time.After(initGateParkProbe):
	}

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, s.waitForMinimalHashTreeInitialization(cancelledCtx), context.Canceled)

	hook.release()
	for name, done := range map[string]chan error{"write": writeDone, "gate": gateDone} {
		select {
		case err := <-done:
			require.NoError(t, err, name)
		case <-time.After(initGateWriteTimeout):
			t.Fatalf("%s did not complete after the memtable pass ended", name)
		}
	}

	awaitHashtreeInitialized(t, s)
	expected := map[strfmt.UUID]int64{uuidLow: t0, uuidMid: t0, uuidHigh: t1}
	got := liveRoot(t, s)
	require.Equal(t, referenceRoot(t, expected), got, "a write landing after the snapshot must be folded exactly once")
	require.Equal(t, serialRebuildRoot(t, ctx, s), got)
}

func TestMergeDoesNotParkWhileHashtreeInitQueued(t *testing.T) {
	ctx := context.Background()
	const class = "MergeInitQueued"
	const t0, t1, t2 = tsFarPast, tsFarPast + 1, tsFarPast + 2

	sl, s, sched, _ := newQueuedInitShard(t, ctx, class)
	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, t0)))
	}
	flushShard(t, ctx, sl)

	release := holdInitSlot(t, sched)
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	requireQueuedInitState(t, s)

	merge := func(updateTime int64) func() error {
		return func() error {
			return s.MergeObject(ctx, objects.MergeDocument{Class: class, ID: uuidLow, UpdateTime: updateTime})
		}
	}
	assertCompletesWithin(t, initGateWriteTimeout, "merge while init is queued", merge(t1))

	require.NoError(t, s.disableAsyncReplication(ctx))
	drained := make(chan struct{})
	go func() { s.asyncRepWg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(initGateWriteTimeout):
		t.Fatal("queued init goroutine did not exit after disable")
	}
	s.asyncReplicationRWMux.RLock()
	require.Nil(t, s.hashtree)
	s.asyncReplicationRWMux.RUnlock()
	assertCompletesWithin(t, initGateWriteTimeout, "merge while disabled", merge(t2))

	release()
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	awaitHashtreeInitialized(t, s)

	expected := map[strfmt.UUID]int64{uuidLow: t2, uuidMid: t0, uuidHigh: t0}
	got := liveRoot(t, s)
	require.Equal(t, referenceRoot(t, expected), got)
	require.Equal(t, serialRebuildRoot(t, ctx, s), got)
}
