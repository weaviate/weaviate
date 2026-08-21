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

package lsmkv

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storagestate"
)

var errInjectedCloseFailure = errors.New("injected commit log close failure")

// failNCloseCommitLogger wraps a real memtableCommitLogger and makes its
// next N close() calls fail without touching the underlying logger, so a
// test can simulate a WAL close failure independently of the flush that
// follows it.
type failNCloseCommitLogger struct {
	memtableCommitLogger
	failN int
}

func (cl *failNCloseCommitLogger) close() error {
	if cl.failN > 0 {
		cl.failN--
		return errInjectedCloseFailure
	}
	return cl.memtableCommitLogger.close()
}

// newRealMemtableWithCommitLogger builds a real *Memtable backed by cl, for
// tests that need to control commit-log behavior independently of
// Memtable.flush() itself.
func newRealMemtableWithCommitLogger(t *testing.T, path, strategy string, cl memtableCommitLogger) *Memtable {
	t.Helper()
	m, err := newMemtable(cl, nil, nullLogger(), nil, memtableConfig{
		path:     path,
		strategy: strategy,
	})
	require.NoError(t, err)
	return m
}

// TestCommitLoggerCloseTerminalOnError pins that a close() failure still
// leaves the logger in its terminal closed state: the underlying fd is
// unusable either way (bufio.Writer errors are sticky, and a failed
// os.File.Close still consumes the fd), so a second close() must not
// attempt the sequence again.
func TestCommitLoggerCloseTerminalOnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment")
	cl, err := newCommitLogger(path, StrategyReplace, 0)
	require.NoError(t, err)

	require.NoError(t, cl.put(segmentReplaceNode{primaryKey: []byte("k"), value: []byte("v")}))

	// Close the fd out from under the logger: writer.Flush's pending bytes
	// now have nowhere to go.
	require.NoError(t, cl.file.Close())

	require.Error(t, cl.close())
	require.NoError(t, cl.close())
}

// TestFlushProceedsAfterCommitLogCloseFailure pins that Memtable.flush()
// treats a commit-log close failure as non-fatal: the memtable is the
// source of truth for the segment written right after, so the flush must
// still land the row on disk instead of aborting.
func TestFlushProceedsAfterCommitLogCloseFailure(t *testing.T) {
	ctx := context.Background()
	b := createTestBucket(t, ctx, t.TempDir(), StrategyReplace)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	realCL, err := newCommitLogger(filepath.Join(b.GetDir(), "segment-custom"), StrategyReplace, 0)
	require.NoError(t, err)
	m := newRealMemtableWithCommitLogger(t, filepath.Join(b.GetDir(), "segment-custom"), StrategyReplace,
		&failNCloseCommitLogger{memtableCommitLogger: realCL, failN: 1})
	b.active = m

	require.NoError(t, b.Put([]byte("key"), []byte("value")))

	require.NoError(t, b.FlushAndSwitch())

	require.Nil(t, b.flushing)
	require.Zero(t, b.active.Size())

	v, err := b.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), v)
}

// TestFlushAndSwitchReadOnlyCheckInsideMutex pins that the shard-readonly
// check happens after flushAndSwitchMu is acquired, not before: a caller
// queued behind an in-flight FlushAndSwitch must see the bucket's current
// status, not a stale snapshot taken before it was queued.
func TestFlushAndSwitchReadOnlyCheckInsideMutex(t *testing.T) {
	ctx := context.Background()
	b := createTestBucket(t, ctx, t.TempDir(), StrategyReplace)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))

	blocker := &blockingFlushMemtable{
		memtable: b.active,
		started:  make(chan struct{}),
		proceed:  make(chan struct{}),
	}
	b.active = blocker

	firstDone := make(chan error, 1)
	enterrors.GoWrapper(func() {
		firstDone <- b.FlushAndSwitch()
	}, nullLogger())

	<-blocker.started

	type result struct {
		switched bool
		err      error
	}
	secondDone := make(chan result, 1)
	enterrors.GoWrapper(func() {
		switched, err := b.flushAndSwitch()
		secondDone <- result{switched, err}
	}, nullLogger())

	// Give the second caller a chance to actually reach flushAndSwitchMu.Lock
	// before flipping the status: this is what makes the assertion below
	// exercise the "queued, then bucket goes READONLY" ordering rather than
	// racing it. On the fix this ordering doesn't matter — the second
	// caller always re-reads status after acquiring the mutex — but without
	// it a still-buggy check-before-lock could win the race often enough to
	// pass by luck.
	time.Sleep(50 * time.Millisecond)

	b.UpdateStatus(storagestate.StatusReadOnly)
	close(blocker.proceed)

	require.NoError(t, <-firstDone)

	res := <-secondDone
	require.False(t, res.switched)
	require.ErrorIs(t, res.err, storagestate.ErrStatusReadOnly)

	b.UpdateStatus(storagestate.StatusReady)
}

// blockingFlushMemtable wraps a real memtable and blocks inside flush()
// until released, signaling started once it has entered the call. Used to
// deterministically queue a second FlushAndSwitch caller behind the first.
type blockingFlushMemtable struct {
	memtable
	started chan struct{}
	proceed chan struct{}
	once    sync.Once
}

func (m *blockingFlushMemtable) flush() (string, error) {
	m.once.Do(func() { close(m.started) })
	<-m.proceed
	return m.memtable.flush()
}

// TestFlushAndSwitchIfThresholdsMetDoesNotForceFlushQuietActive pins that a
// retained flush is drained on its own: it must not force-flush an
// otherwise-quiet active memtable just because it happened to be there,
// discarding the active memtable's in-progress dirty state along with it.
func TestFlushAndSwitchIfThresholdsMetDoesNotForceFlushQuietActive(t *testing.T) {
	ctx := context.Background()
	b := createTestBucket(t, ctx, t.TempDir(), StrategyReplace)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))
	b.active = &failNFlushesMemtable{memtable: b.active, failN: 1}

	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)

	require.NoError(t, b.Put([]byte("B"), []byte("vB")))
	activeBefore := b.active

	noAbort := func() bool { return false }
	require.True(t, b.flushAndSwitchIfThresholdsMet(noAbort))

	require.Nil(t, b.flushing)

	// The active memtable is the same instance from before the callback,
	// still holding row B unflushed in memory: only the retained flush (row
	// A) was drained.
	require.Same(t, activeBefore, b.active)
	require.NotZero(t, b.active.Size())

	vA, err := b.Get([]byte("A"))
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), vA)

	vB, err := b.Get([]byte("B"))
	require.NoError(t, err)
	require.Equal(t, []byte("vB"), vB)
}

// TestFlushAndSwitchIfThresholdsMetRetryDoesNotResize pins that draining a
// retained flush alone never feeds the memtable size advisor: the quiet
// active memtable's (near-zero) cycle length is not a meaningful signal for
// resizing, and must not be treated as one.
func TestFlushAndSwitchIfThresholdsMetRetryDoesNotResize(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger := nullLogger()
	noopCB := cyclemanager.NewCallbackGroupNoop()
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil, noopCB, noopCB,
		WithStrategy(StrategyReplace),
		WithDynamicMemtableSizing(1, 2, 60, 120),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	thresholdBefore := b.memtableThreshold

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))
	b.active = &failNFlushesMemtable{memtable: b.active, failN: 1}

	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)

	noAbort := func() bool { return false }
	require.True(t, b.flushAndSwitchIfThresholdsMet(noAbort))

	require.Nil(t, b.flushing)
	require.Equal(t, thresholdBefore, b.memtableThreshold)
}

// TestFlushAndSwitchIfThresholdsMetReadonlyThenReady pins the READONLY
// backoff path for a retained flush: while the shard is READONLY, the
// retry must not run — and must hit the existing halted-flush backoff
// warning instead of error-logging on every callback tick — and once the
// shard returns to READY the retained flush completes.
func TestFlushAndSwitchIfThresholdsMetReadonlyThenReady(t *testing.T) {
	ctx := context.Background()
	logger, hook := logrustest.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))
	b.active = &failNFlushesMemtable{memtable: b.active, failN: 1}

	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)

	b.UpdateStatus(storagestate.StatusReadOnly)
	hook.Reset()

	noAbort := func() bool { return false }
	for i := 0; i < 3; i++ {
		require.False(t, b.flushAndSwitchIfThresholdsMet(noAbort))
		require.NotNil(t, b.flushing, "retained flush must not be dropped while READONLY")
	}

	for _, entry := range hook.AllEntries() {
		// logrus severity is inverted (ErrorLevel=2 < WarnLevel=3): this
		// rejects Error/Fatal/Panic while allowing the expected Warn.
		require.Greaterf(t, entry.Level, logrus.ErrorLevel, "unexpected error-level log while READONLY: %s", entry.Message)
	}

	b.UpdateStatus(storagestate.StatusReady)

	require.True(t, b.flushAndSwitchIfThresholdsMet(noAbort))
	require.Nil(t, b.flushing)

	v, err := b.Get([]byte("A"))
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), v)
}

// TestShutdownDrainsRetainedFlush pins that Shutdown does not strand a
// retained flush: with no flush-cycle worker left to retry it, the wait
// loop further down would otherwise just run out the clock on ctx.
func TestShutdownDrainsRetainedFlush(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	b := createTestBucket(t, ctx, dir, StrategyReplace)

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))
	b.active = &failNFlushesMemtable{memtable: b.active, failN: 1}

	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)

	require.NoError(t, b.Put([]byte("B"), []byte("vB")))

	shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	require.NoError(t, b.Shutdown(shutdownCtx))

	reopened, err := NewBucketCreator().NewBucket(ctx, dir, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyReplace))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Shutdown(ctx)) })

	vA, err := reopened.Get([]byte("A"))
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), vA)

	vB, err := reopened.Get([]byte("B"))
	require.NoError(t, err)
	require.Equal(t, []byte("vB"), vB)
}

// TestFlushAndSwitchRecoversFromTwoConsecutiveFailures pins that the retry
// is not a one-shot: a memtable can be retried across more than one failed
// attempt without losing data.
func TestFlushAndSwitchRecoversFromTwoConsecutiveFailures(t *testing.T) {
	ctx := context.Background()
	b := createTestBucket(t, ctx, t.TempDir(), StrategyReplace)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))
	b.active = &failNFlushesMemtable{memtable: b.active, failN: 2}

	require.Error(t, b.FlushAndSwitch())
	vA, err := b.Get([]byte("A"))
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), vA)

	require.NoError(t, b.Put([]byte("B"), []byte("vB")))
	require.Error(t, b.FlushAndSwitch())

	vA, err = b.Get([]byte("A"))
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), vA)
	vB, err := b.Get([]byte("B"))
	require.NoError(t, err)
	require.Equal(t, []byte("vB"), vB)

	require.NoError(t, b.Put([]byte("C"), []byte("vC")))
	require.NoError(t, b.FlushAndSwitch())

	require.Nil(t, b.flushing)
	for k, v := range map[string]string{"A": "vA", "B": "vB", "C": "vC"} {
		got, err := b.Get([]byte(k))
		require.NoError(t, err)
		require.Equal(t, []byte(v), got)
	}
}

// TestConcurrentFlushAndSwitchWithRetainedState drives two overlapping
// FlushAndSwitch callers against a bucket that already has a retained
// flush: flushAndSwitchMu must serialize them so neither loses data nor
// overwrites the other's retained memtable. Run with -race.
func TestConcurrentFlushAndSwitchWithRetainedState(t *testing.T) {
	ctx := context.Background()
	b := createTestBucket(t, ctx, t.TempDir(), StrategyReplace)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))
	b.active = &failNFlushesMemtable{memtable: b.active, failN: 1}
	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)

	require.NoError(t, b.Put([]byte("B"), []byte("vB")))

	blocker := &blockingFlushMemtable{
		memtable: b.active,
		started:  make(chan struct{}),
		proceed:  make(chan struct{}),
	}
	b.active = blocker

	firstDone := make(chan error, 1)
	enterrors.GoWrapper(func() {
		firstDone <- b.FlushAndSwitch()
	}, nullLogger())

	<-blocker.started

	secondDone := make(chan error, 1)
	enterrors.GoWrapper(func() {
		secondDone <- b.FlushAndSwitch()
	}, nullLogger())

	close(blocker.proceed)

	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)

	require.Nil(t, b.flushing)
	vA, err := b.Get([]byte("A"))
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), vA)
	vB, err := b.Get([]byte("B"))
	require.NoError(t, err)
	require.Equal(t, []byte("vB"), vB)
}

// corruptSegmentOnceMemtable lets flush() succeed for real (so the commit
// log is genuinely closed and the WAL genuinely deleted, exactly as a
// successful flush would), then truncates the segment file it just wrote,
// once. This simulates the segment becoming unreadable strictly after a
// successful flush() call — e.g. out-of-band corruption — so the failure
// surfaces one step later, in initAndPrecomputeNewSegment.
type corruptSegmentOnceMemtable struct {
	memtable
	corrupted bool
}

func (m *corruptSegmentOnceMemtable) flush() (string, error) {
	segmentPath, err := m.memtable.flush()
	if err != nil || m.corrupted {
		return segmentPath, err
	}
	m.corrupted = true
	if err := os.Truncate(segmentPath, 4); err != nil {
		return segmentPath, err
	}
	return segmentPath, nil
}

// TestFlushRetryRewritesSegmentAfterPostDurableFailure pins that a retry
// recovers even when the failure happens after flush() itself already
// succeeded (WAL closed and deleted, segment written and renamed): the
// retry re-runs flush() from the memtable's in-memory state regardless,
// which overwrites whatever partial/corrupt file is sitting at the target
// path with a fresh, complete one.
func TestFlushRetryRewritesSegmentAfterPostDurableFailure(t *testing.T) {
	ctx := context.Background()
	b := createTestBucket(t, ctx, t.TempDir(), StrategyReplace)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.Put([]byte("A"), []byte("vA")))
	b.active = &corruptSegmentOnceMemtable{memtable: b.active}

	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)

	require.NoError(t, b.Put([]byte("B"), []byte("vB")))
	require.NoError(t, b.FlushAndSwitch())

	require.Nil(t, b.flushing)
	vA, err := b.Get([]byte("A"))
	require.NoError(t, err)
	require.Equal(t, []byte("vA"), vA)
	vB, err := b.Get([]byte("B"))
	require.NoError(t, err)
	require.Equal(t, []byte("vB"), vB)
}

// TestFlushAndSwitchInvertedRetry pins the inverted-strategy-specific steps
// on the retry path: setAveragePropertyLength must be recomputed from disk
// truth (not compounded) on a retried flush, and a tombstone recorded in a
// retained memtable must still reach existing segments once the retry
// succeeds.
func TestFlushAndSwitchInvertedRetry(t *testing.T) {
	ctx := context.Background()
	b := createTestBucket(t, ctx, t.TempDir(), StrategyInverted)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(ctx)) })

	require.NoError(t, b.MapSet([]byte("term"), NewMapPairFromDocIdAndTf(1, 3, 5, false)))
	require.NoError(t, b.FlushAndSwitch())

	require.NoError(t, b.MapSet([]byte("term"), NewMapPairFromDocIdAndTf(2, 1, 7, false)))
	require.NoError(t, b.active.SetTombstone(1))

	b.active = &failNFlushesMemtable{memtable: b.active, failN: 1}
	require.Error(t, b.FlushAndSwitch())
	require.NotNil(t, b.flushing)

	require.NoError(t, b.FlushAndSwitch())
	require.Nil(t, b.flushing)

	avg, count := b.GetAveragePropertyLength()
	require.EqualValues(t, 2, count)
	require.InDelta(t, 6.0, avg, 0.0001)

	// Segment #1 (the one written before the failure, holding doc 1) must
	// have picked up the tombstone recorded in the retried memtable.
	require.Len(t, b.disk.segments, 2)
	tombstones, err := b.disk.segments[0].ReadOnlyTombstones()
	require.NoError(t, err)
	require.True(t, tombstones.Contains(1))
}
