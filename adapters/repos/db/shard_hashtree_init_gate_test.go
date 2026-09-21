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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	entreplication "github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/storobj"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
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

// registeredInScheduler reports whether the scheduler currently holds an entry for s.
func registeredInScheduler(sched *AsyncReplicationScheduler, s *Shard) bool {
	sched.mu.Lock()
	defer sched.mu.Unlock()
	_, ok := sched.entries[s]
	return ok
}

// requireUnreadyHashtree asserts a tree is installed but not yet served to hashbeat.
func requireUnreadyHashtree(t *testing.T, s *Shard) {
	t.Helper()
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()
	require.NotNil(t, s.hashtree)
	require.False(t, s.hashtreeFullyInitialized)
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

// injectedScanPanic must not contain the matched message: the recovery log line repeats it and would re-arm the hook.
const injectedScanPanic = "injected scan failure"

// panickingLogHook panics from Fire on every matching message while armed, injecting a failure into whatever goroutine logs it.
type panickingLogHook struct {
	match string
	armed atomic.Bool
}

func (h *panickingLogHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h *panickingLogHook) Fire(e *logrus.Entry) error {
	if h.armed.Load() && strings.Contains(e.Message, h.match) {
		panic(injectedScanPanic)
	}
	return nil
}

// enableRecoveryOnPanic opts a test out of the integration suite's DISABLE_RECOVERY_ON_PANIC, under which initHashtree re-panics and an injected scan panic kills the test binary.
// Call it before the shard is built so the restore runs after the shutdown cleanup.
func enableRecoveryOnPanic(t *testing.T) {
	t.Helper()
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
}

func histogramSampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	var mtr dto.Metric
	require.NoError(t, h.Write(&mtr))
	return mtr.GetHistogram().GetSampleCount()
}

// corruptNewestObjectsSegment overwrites the first node's value length in the newest objects segment so the lsmkv replace cursor panics; the returned func restores it.
func corruptNewestObjectsSegment(t *testing.T, s *Shard) (repair func()) {
	t.Helper()
	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)
	segments, err := filepath.Glob(filepath.Join(bucket.GetDir(), "segment-*.db"))
	require.NoError(t, err)
	require.NotEmpty(t, segments, "a flushed objects bucket must have at least one segment")
	slices.Sort(segments)
	path := segments[len(segments)-1]

	// node layout right after the segment header: [tombstone:1][valueLength:8][value]...
	const valueLengthOffset = int64(segmentindex.HeaderSize) + 1
	const bogusValueLength = uint64(1) << 40

	writeAt := func(b []byte) {
		t.Helper()
		// in place: the segment is mmapped MAP_SHARED, so truncating it would SIGBUS the open cursor
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		_, err = f.WriteAt(b, valueLengthOffset)
		require.NoError(t, err)
	}

	original := make([]byte, 8)
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	require.NoError(t, err)
	_, err = f.ReadAt(original, valueLengthOffset)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	corrupt := make([]byte, 8)
	binary.LittleEndian.PutUint64(corrupt, bogusValueLength)
	writeAt(corrupt)

	return func() { writeAt(original) }
}

// TestHashtreeInitPanicIsRetriedAsFailedAttempt pins that a panicking init scan is a failed attempt, not an abandoned init leaving the shard unready and unregistered.
func TestHashtreeInitPanicIsRetriedAsFailedAttempt(t *testing.T) {
	enableRecoveryOnPanic(t)

	ctx := context.Background()
	const t0, t1 = tsFarPast, tsFarPast + 1
	const failureMsg = "hashtree initialization attempt"

	armHook := func(t *testing.T, s *Shard, hook *panickingLogHook) func() {
		hook.armed.Store(true)
		return func() { hook.armed.Store(false) }
	}

	tests := []struct {
		name      string
		seed      func(t *testing.T, sl ShardLike, class string) map[strfmt.UUID]int64
		breakScan func(t *testing.T, s *Shard, hook *panickingLogHook) (repair func())
	}{
		{
			name: "memtablePass",
			seed: func(t *testing.T, sl ShardLike, class string) map[strfmt.UUID]int64 {
				require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, t0)))
				return map[strfmt.UUID]int64{uuidLow: t0}
			},
			breakScan: armHook,
		},
		{
			name: "diskPass",
			seed: func(t *testing.T, sl ShardLike, class string) map[strfmt.UUID]int64 {
				require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, t0)))
				flushShard(t, ctx, sl)
				return map[strfmt.UUID]int64{uuidLow: t0}
			},
			breakScan: armHook,
		},
		{
			name: "corruptSegment",
			seed: func(t *testing.T, sl ShardLike, class string) map[strfmt.UUID]int64 {
				require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, t0)))
				require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidMid, t0)))
				flushShard(t, ctx, sl)
				return map[strfmt.UUID]int64{uuidLow: t0, uuidMid: t0}
			},
			breakScan: func(t *testing.T, s *Shard, _ *panickingLogHook) func() {
				return corruptNewestObjectsSegment(t, s)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			class := "InitPanicRetry" + tc.name
			hook := &panickingLogHook{match: "hashtree initialization in progress"}
			sl, s, sched, logs := newQueuedInitShard(t, ctx, class, hook)
			t.Cleanup(func() { hook.armed.Store(false) })

			m, err := NewMetrics(s.index.logger, monitoring.GetMetrics(), class, s.name)
			require.NoError(t, err)
			s.metrics = m

			expected := tc.seed(t, sl, class)

			initsBefore := testutil.ToFloat64(m.asyncReplicationHashTreeInitCount)
			failuresBefore := testutil.ToFloat64(m.asyncReplicationHashTreeInitFailureCount)
			runningBefore := testutil.ToFloat64(m.asyncReplicationHashTreeInitRunning)
			durationsBefore := histogramSampleCount(t, m.asyncReplicationHashTreeInitDuration)

			repair := tc.breakScan(t, s, hook)

			release := holdInitSlot(t, sched)
			require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
			requireQueuedInitState(t, s)
			release()

			require.Eventually(t, func() bool { return hookHasMessage(logs, failureMsg+" 0 failure") },
				initGateWriteTimeout, 10*time.Millisecond, "a panicking scan must be logged as a failed attempt")

			holdCtx, cancelHold := context.WithTimeout(ctx, initGateWriteTimeout)
			require.NoError(t, sched.hashtreeInitSem.Acquire(holdCtx, 1), "a panicking attempt must return its init slot")
			cancelHold()
			var releaseOnce sync.Once
			releaseHold := func() { releaseOnce.Do(func() { sched.hashtreeInitSem.Release(1) }) }
			t.Cleanup(releaseHold)

			require.Eventually(t, func() bool {
				n := float64(countMessages(logs, failureMsg))
				return n >= 1 &&
					testutil.ToFloat64(m.asyncReplicationHashTreeInitCount) == initsBefore+n &&
					testutil.ToFloat64(m.asyncReplicationHashTreeInitFailureCount) == failuresBefore+n
			}, initGateWriteTimeout, 10*time.Millisecond, "every panicking attempt must be counted once as an init and once as a failure")

			attempts := countMessages(logs, failureMsg)
			for _, e := range logs.AllEntries() {
				if strings.Contains(e.Message, failureMsg) {
					require.Contains(t, e.Message, "panicked", "the attempt error must carry the recovered panic")
				}
			}
			require.Equal(t, runningBefore, testutil.ToFloat64(m.asyncReplicationHashTreeInitRunning))
			require.Equal(t, durationsBefore, histogramSampleCount(t, m.asyncReplicationHashTreeInitDuration),
				"a panicking attempt must not observe a success duration")

			assertCompletesWithin(t, initGateWriteTimeout, "write after a panicking attempt", func() error {
				return sl.PutObject(ctx, testObjWithTime(class, uuidHigh, t1))
			})
			expected[uuidHigh] = t1

			requireUnreadyHashtree(t, s)
			require.False(t, registeredInScheduler(sched, s), "a shard whose init panicked must not be registered as ready")

			repair()
			releaseHold()
			awaitHashtreeInitialized(t, s)

			got := liveRoot(t, s)
			require.Equal(t, referenceRoot(t, expected), got, "the retried attempt must fold every object exactly once")
			require.Equal(t, serialRebuildRoot(t, ctx, s), got)
			require.Equal(t, failuresBefore+float64(attempts), testutil.ToFloat64(m.asyncReplicationHashTreeInitFailureCount),
				"the succeeding attempt must not be counted as a failure")
		})
	}
}

// TestHashtreeInitPanicAfterSuccessResetsReadiness pins that an attempt panicking past the readiness flag leaves no shard serving the retry's fresh, empty tree as ready.
func TestHashtreeInitPanicAfterSuccessResetsReadiness(t *testing.T) {
	enableRecoveryOnPanic(t)

	ctx := context.Background()
	const class = "InitPanicAfterSuccess"
	const t0 = tsFarPast

	panicking := &panickingLogHook{match: "hashtree successfully initialized"}
	scanning := newParkingLogHook("hashtree initialization in progress")
	sl, s, sched, logs := newQueuedInitShard(t, ctx, class, panicking, scanning)
	t.Cleanup(scanning.release)
	t.Cleanup(func() { panicking.armed.Store(false) })
	panicking.armed.Store(true)

	release := holdInitSlot(t, sched)
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	requireQueuedInitState(t, s)
	release()

	require.Eventually(t, func() bool { return hookHasMessage(logs, "hashtree initialization attempt 0 failure") },
		initGateWriteTimeout, 10*time.Millisecond, "a panic past the success log must be a failed attempt")

	holdCtx, cancelHold := context.WithTimeout(ctx, initGateWriteTimeout)
	require.NoError(t, sched.hashtreeInitSem.Acquire(holdCtx, 1), "a panicking attempt must return its init slot")
	cancelHold()
	var releaseOnce sync.Once
	releaseHold := func() { releaseOnce.Do(func() { sched.hashtreeInitSem.Release(1) }) }
	t.Cleanup(releaseHold)

	panicking.armed.Store(false)
	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, t0)))
	releaseHold()

	select {
	case <-scanning.parked:
	case <-time.After(initGateWriteTimeout):
		t.Fatal("the retried attempt did not reach its memtable pass")
	}

	requireUnreadyHashtree(t, s)
	_, served := s.HashTreeRoot()
	require.False(t, served, "a half-scanned retry tree must not be served to peers")
	require.False(t, registeredInScheduler(sched, s))

	scanning.release()
	awaitHashtreeInitialized(t, s)
	require.Eventually(t, func() bool { return registeredInScheduler(sched, s) },
		initGateWriteTimeout, 10*time.Millisecond, "the completed retry must register the shard")

	got := liveRoot(t, s)
	require.Equal(t, referenceRoot(t, map[strfmt.UUID]int64{uuidLow: t0}), got, "the retried attempt must fold every object exactly once")
	require.Equal(t, serialRebuildRoot(t, ctx, s), got)
}

// TestHashtreeInitPanicIsFatalWhenRecoveryIsDisabled pins that DISABLE_RECOVERY_ON_PANIC keeps a scan panic fatal (the integration suite sets it) instead of silently retrying it, with the write gate still released on the way out.
func TestHashtreeInitPanicIsFatalWhenRecoveryIsDisabled(t *testing.T) {
	ctx := context.Background()
	const class = "InitPanicRecoveryDisabled"

	panicking := &panickingLogHook{match: "hashtree initialization in progress"}
	sl, s, _, _ := newQueuedInitShard(t, ctx, class, panicking)
	t.Cleanup(func() { panicking.armed.Store(false) })

	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	awaitHashtreeInitialized(t, s)

	bucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, bucket)

	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "true")
	panicking.armed.Store(true)
	require.PanicsWithValue(t, injectedScanPanic, func() {
		require.NoError(t, s.initHashtree(ctx, minAsyncReplicationConfig(), bucket))
	})
	panicking.armed.Store(false)

	requireUnreadyHashtree(t, s)
	gate := gateChannel(s)
	require.NotNil(t, gate)
	select {
	case <-gate:
	default:
		t.Fatal("the write gate must be released before the panic leaves initHashtree")
	}
}

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

// TestStaleInitializerDoesNotKeepUnreadyTreeRegistered pins that a disable+enable flap landing while an initializer registers never leaves an unready hashtree serving hashbeat.
func TestStaleInitializerDoesNotKeepUnreadyTreeRegistered(t *testing.T) {
	ctx := context.Background()

	t.Run("rebuildWhileRegistering", func(t *testing.T) {
		const class = "StaleInitRegistering"
		const t0, t1 = tsFarPast, tsFarPast + 1

		registering := newParkingLogHook("hashtree successfully initialized")
		scanning := newParkingLogHook("hashtree initialization in progress")
		sl, s, sched, _ := newQueuedInitShard(t, ctx, class, registering, scanning)
		t.Cleanup(registering.release)
		t.Cleanup(scanning.release)

		require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
		select {
		case <-registering.parked:
		case <-time.After(initGateWriteTimeout):
			t.Fatal("the empty-shard scan did not reach its success log")
		}

		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, t0)))
		flushShard(t, ctx, sl)
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidMid, t1)))

		require.NoError(t, s.rebuildAsyncReplicationFromScratch(ctx, true, minAsyncReplicationConfig()))
		requireQueuedInitState(t, s)
		registering.release()

		select {
		case <-scanning.parked:
		case <-time.After(initGateWriteTimeout):
			t.Fatal("the replacement scan did not start")
		}
		requireUnreadyHashtree(t, s)
		require.False(t, registeredInScheduler(sched, s),
			"a stale initializer must not keep an unready hashtree registered")

		scanning.release()
		awaitHashtreeInitialized(t, s)
		require.Eventually(t, func() bool { return registeredInScheduler(sched, s) },
			initGateWriteTimeout, 10*time.Millisecond, "the completed scan must re-register the shard")

		got := liveRoot(t, s)
		require.Equal(t, referenceRoot(t, map[strfmt.UUID]int64{uuidLow: t0, uuidMid: t1}), got)
		require.Equal(t, serialRebuildRoot(t, ctx, s), got)
	})

	t.Run("registeredWhileQueued", func(t *testing.T) {
		const class = "StaleInitQueued"
		const t0, t1 = tsFarPast, tsFarPast + 1

		zeroCoalesceWindow(t)
		sl, s, sched, _ := newQueuedInitShard(t, ctx, class)
		m, err := NewMetrics(s.index.logger, monitoring.GetMetrics(), class, s.name)
		require.NoError(t, err)
		s.metrics = m

		idx := s.index
		idx.replicationConfigLock.Lock()
		idx.Config.ReplicationFactor = 3
		idx.replicationConfigLock.Unlock()

		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, t0)))
		flushShard(t, ctx, sl)
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidMid, t1)))

		cfg := minAsyncReplicationConfig()
		cfg.frequency = 50 * time.Millisecond
		cfg.frequencyWhilePropagating = 50 * time.Millisecond

		release := holdInitSlot(t, sched)
		require.NoError(t, s.enableAsyncReplication(ctx, cfg))
		requireQueuedInitState(t, s)

		var dispatches atomic.Int64
		asyncRepDispatchSeam = func(entry *asyncSchedulerEntry) {
			if entry.shard == s {
				dispatches.Add(1)
			}
		}
		t.Cleanup(func() {
			if err := sched.Deregister(s); err != nil {
				t.Errorf("deregister before restoring the dispatch seam: %v", err)
			}
			asyncRepDispatchSeam = nil
		})

		iterationsBefore := testutil.ToFloat64(m.asyncReplicationIterationCount)
		require.NoError(t, sched.Register(s))
		require.Eventually(t, func() bool { return dispatches.Load() > 0 },
			initGateWriteTimeout, 10*time.Millisecond, "the registered entry must reach dispatch")
		time.Sleep(initGateParkProbe)
		require.Equal(t, iterationsBefore, testutil.ToFloat64(m.asyncReplicationIterationCount),
			"a queued, unready hashtree must never be served to hashbeat")

		release()
		awaitHashtreeInitialized(t, s)
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(m.asyncReplicationIterationCount) > iterationsBefore
		}, initGateWriteTimeout, 10*time.Millisecond, "hashbeat must resume once the scan serves its tree")
	})
}
