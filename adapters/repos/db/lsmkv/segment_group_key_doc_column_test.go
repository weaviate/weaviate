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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// Shards of one property refusing a flush warn once between them, not once each.
func TestKeyDocColumnDetachWarnsOncePerName(t *testing.T) {
	ctx := context.Background()
	logger, hook := test.NewNullLogger()
	name := fmt.Sprintf("%s/%d", t.Name(), time.Now().UnixNano())
	for range 2 {
		b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyRoaringSet),
			WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			WithKeyDocColumn(name, func() uint64 { return 10_000 }))
		require.NoError(t, err)
		t.Cleanup(func() { b.Shutdown(ctx) })
		for i := range 1025 { // one past overflowFloor keys holding a second document
			require.NoError(t, b.RoaringSetAddList(fmt.Appendf(nil, "shared%04d", i),
				[]uint64{uint64(2 * i), uint64(2*i + 1)}))
		}
		require.NoError(t, b.FlushAndSwitch())
		require.Nil(t, b.KeyDocColumn())
	}

	warnings := 0
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel && strings.Contains(e.Message, "could not absorb a flush") {
			warnings++
		}
	}
	assert.Equal(t, 1, warnings)
}

// TestKeyDocColumnDetachedWhenAFlushCannotBeAbsorbed pins a column that refuses a flush being detached, not left answering.
func TestKeyDocColumnDetachedWhenAFlushCannotBeAbsorbed(t *testing.T) {
	ctx := context.Background()

	// one past overflowFloor (1024) keys holding a second document
	const sharedValueKeys = 1025

	b := createTestBucketRoaringSetWithKeyDocColumn(t, ctx, t.TempDir(), 10_000)
	defer b.Shutdown(ctx)

	require.NoError(t, b.RoaringSetAddOne([]byte("unique"), 1))
	require.NoError(t, b.FlushAndSwitch())
	require.NotNil(t, b.KeyDocColumn(), "a flush it can absorb leaves it attached")

	for i := range sharedValueKeys {
		key := fmt.Appendf(nil, "shared%04d", i)
		require.NoError(t, b.RoaringSetAddList(key, []uint64{uint64(2 * i), uint64(2*i + 1)}))
	}
	require.NoError(t, b.FlushAndSwitch())

	assert.Nil(t, b.KeyDocColumn(),
		"a flush it could not absorb leaves it holding less than the segments do")

	// every document is still there to be read; only the shortcut to them is gone
	bm, release, err := b.RoaringSetGet(ctx, []byte("shared0007"))
	require.NoError(t, err)
	defer release()
	assert.True(t, bm.Contains(14), "both documents under a shared value must resolve")
	assert.True(t, bm.Contains(15))
}

// TestBucketOpensWithoutAColumnItCannotBuild pins a failed column build leaving a working bucket, no column, and a warning.
func TestBucketOpensWithoutAColumnItCannotBuild(t *testing.T) {
	ctx := context.Background()

	logger, hook := test.NewNullLogger()
	dir := t.TempDir()
	b := createTestBucketWithOptionsAndLogger(t, ctx, dir, logger,
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		WithKeyDocColumn(dir, nil),
	)
	defer b.Shutdown(ctx)

	assert.Nil(t, b.KeyDocColumn(), "nothing was built")

	var warned *logrus.Entry
	for _, e := range hook.AllEntries() {
		if e.Data["action"] == "keydoccolumn_build" {
			warned = e
		}
	}
	require.NotNil(t, warned, "a property that quietly fell back is what this must not be")
	assert.Equal(t, logrus.WarnLevel, warned.Level)
	assert.Contains(t, warned.Message, "standard path")
	assert.NotEmpty(t, warned.Data["bucket"], "which property it is about")

	// and the bucket is a working roaringset bucket, before and after a flush
	require.NoError(t, b.RoaringSetAddOne([]byte("value"), 7))
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.RoaringSetAddOne([]byte("value"), 8))

	bm, release, err := b.RoaringSetGet(ctx, []byte("value"))
	require.NoError(t, err)
	defer release()
	assert.True(t, bm.Contains(7), "the flushed document")
	assert.True(t, bm.Contains(8), "and the unflushed one")
	assert.Nil(t, b.KeyDocColumn(), "a flush does not build what open could not")
}

// An unloadable lazy segment costs the column, not the bucket, and leaks no segment reference.
func TestBucketOpensWithoutAColumnOverAnUnloadableSegment(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	opts := []BucketOption{
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
		WithLazySegmentLoading(true),
		WithKeyDocColumn(dir, func() uint64 { return 100 }),
	}
	b := createTestBucketWithOptions(t, ctx, dir, opts...)
	for i := range 3 {
		require.NoError(t, b.RoaringSetAddOne(fmt.Appendf(nil, "k%d", i), uint64(i)))
		require.NoError(t, b.FlushAndSwitch())
	}
	require.NoError(t, b.Shutdown(ctx))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var segs []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".db" {
			segs = append(segs, e.Name())
		}
	}
	require.Len(t, segs, 3)
	require.NoError(t, os.Truncate(filepath.Join(dir, segs[len(segs)-1]), 5))

	b = createTestBucketWithOptions(t, ctx, dir, opts...)
	t.Cleanup(func() {
		// hand back what a failing run leaked, or Shutdown waits on it forever
		for _, seg := range b.disk.segments {
			for seg.getRefs() > 0 {
				seg.decRef()
			}
		}
		assert.NoError(t, b.Shutdown(ctx))
	})
	assert.Nil(t, b.disk.keyDocColumn.Load(), "the build declines")
	require.Len(t, b.disk.segments, 3)
	assert.True(t, b.disk.segments[1].(*lazySegment).loaded.Load(),
		"the build loaded the segment before the one that cannot load")
	refs := make([]int, len(b.disk.segments))
	for i, seg := range b.disk.segments {
		refs[i] = seg.getRefs()
	}
	require.Equal(t, []int{0, 0, 0}, refs, "segment references left behind: %v", refs)
}

// A declined build warns once per column name, not at every shard's open.
func TestKeyDocColumnDeclineWarnsOncePerName(t *testing.T) {
	ctx := context.Background()
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	shared := t.TempDir()
	for _, name := range []string{shared, shared, t.TempDir()} {
		// no docID count getter, so every open declines
		b := createTestBucketWithOptionsAndLogger(t, ctx, t.TempDir(), logger,
			WithStrategy(StrategyRoaringSet),
			WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			WithKeyDocColumn(name, nil))
		require.NoError(t, b.Shutdown(ctx))
	}

	levels := map[logrus.Level]int{}
	for _, e := range hook.AllEntries() {
		if e.Data["action"] == "keydoccolumn_build" {
			levels[e.Level]++
		}
	}
	assert.Equal(t, 2, levels[logrus.WarnLevel], "one warning per name")
	assert.Equal(t, 1, levels[logrus.DebugLevel], "the repeat stays at debug")
}

// flattenAllocChecker is a memory monitor a test can make refuse; an ask it
// does not refuse waits for the test to grant it, except the flattening's
// heap check, which asks for nothing.
type flattenAllocChecker struct {
	refuse  atomic.Bool
	asked   atomic.Int32
	permits chan struct{}
}

// newFlattenAllocChecker installs the checker on b's segment group.
func newFlattenAllocChecker(t *testing.T, b *Bucket) *flattenAllocChecker {
	c := &flattenAllocChecker{permits: make(chan struct{})}
	b.disk.allocChecker = c
	t.Cleanup(func() { close(c.permits) }) // runs before the bucket's shutdown, freeing any waiting ask
	return c
}

func (c *flattenAllocChecker) CheckAlloc(bytes int64) error {
	refused := c.refuse.Load()
	if !refused && bytes == 0 {
		return nil
	}
	c.asked.Add(1) // after the decision, so a test that sees the count sees the answer given
	if refused {
		return errors.New("no memory")
	}
	select {
	case <-c.permits:
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("never granted")
	}
}

func (c *flattenAllocChecker) CheckMappingAndReserve(int64, int) error { return nil }
func (c *flattenAllocChecker) Refresh(bool)                            {}

// grant lets one waiting ask through, failing if none arrives.
func (c *flattenAllocChecker) grant(t *testing.T, msg string, args ...any) {
	t.Helper()
	select {
	case c.permits <- struct{}{}:
	case <-time.After(5 * time.Second):
		require.FailNowf(t, "the memory monitor was never asked", msg, args...)
	}
}

// grantAny lets through an ask that is waiting now, if there is one.
func (c *flattenAllocChecker) grantAny() {
	select {
	case c.permits <- struct{}{}:
	default:
	}
}

// newKeyDocColumnBucketWithGauge opens a key/doc column over one flushed key,
// "base", reporting its size into the returned gauge.
func newKeyDocColumnBucketWithGauge(t *testing.T, ctx context.Context, opts ...BucketOption) (*Bucket, prometheus.Gauge) {
	t.Helper()
	dir := t.TempDir()
	seed := createTestBucketRoaringSet(t, ctx, dir)
	require.NoError(t, seed.RoaringSetAddOne([]byte("base"), 1))
	require.NoError(t, seed.FlushAndSwitch())
	require.NoError(t, seed.Shutdown(ctx))

	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_key_doc_column_bytes"})
	metrics := newTestMetrics(t, false, "KeyDocColumnGauge", "shard1")
	metrics.keyDocColumnSize = gauge
	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, metrics,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		append([]BucketOption{
			WithStrategy(StrategyRoaringSet),
			WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			WithKeyDocColumn(dir, func() uint64 { return 10_000 }),
		}, opts...)...)
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)
	require.NotNil(t, b.KeyDocColumn())
	return b, gauge
}

// TestKeyDocColumnGaugeFollowsTheColumn pins the gauge tracking the column through build, flattenings, drop and shutdown, each flattening returning its slot.
func TestKeyDocColumnGaugeFollowsTheColumn(t *testing.T) {
	ctx := context.Background()

	for _, tt := range []struct {
		name   string
		drop   bool // dropped before the shutdown, which then has nothing left to subtract
		rounds int
		// all slots but one held, so a slot not given back leaves the second round without one
		oneSlot bool
	}{
		{name: "dropped", drop: true, rounds: 1},
		{name: "shut down", rounds: 2, oneSlot: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, gauge := newKeyDocColumnBucketWithGauge(t, ctx)
			shut := false
			t.Cleanup(func() {
				if !shut {
					b.Shutdown(ctx)
				}
			})
			if tt.oneSlot {
				holdFlattenSlots(t, keyDocColumnFlattenSlots-1)
			}
			checker := newFlattenAllocChecker(t, b)

			built := testutil.ToFloat64(gauge)
			require.Positive(t, built, "the build adds the column")
			require.Equal(t, float64(b.KeyDocColumn().Info().SizeBytes), built)

			for round := range tt.rounds {
				unlayered := b.KeyDocColumn().Info().SizeBytes
				for i := range 8 { // flattenLayersThreshold
					require.NoError(t, b.RoaringSetAddOne(fmt.Appendf(nil, "r%02dk%d", round, i), uint64(10+8*round+i)))
					require.NoError(t, b.FlushAndSwitch())
				}
				// the flattening waits on the monitor, so the gauge still holds the layers
				require.Greater(t, b.KeyDocColumn().Info().SizeBytes, unlayered)
				require.Equal(t, float64(b.KeyDocColumn().Info().SizeBytes), testutil.ToFloat64(gauge))
				layeredBytes := b.KeyDocColumn().Info().SizeBytes
				checker.grant(t, "round %d", round)

				require.Eventually(t, func() bool {
					// as the compaction cycle would, should another test hold a slot
					checker.grantAny()
					b.disk.startPendingKeyDocColumnFlatten()
					size := b.KeyDocColumn().Info().SizeBytes
					return size < layeredBytes && testutil.ToFloat64(gauge) == float64(size)
				}, 5*time.Second, time.Millisecond, "round %d: the flattened column is what the gauge reports", round)
			}
			require.Greater(t, testutil.ToFloat64(gauge), built, "the base gained the flushed keys")

			if tt.drop {
				b.disk.dropKeyDocColumn()
				assert.Nil(t, b.KeyDocColumn())
				assert.Zero(t, testutil.ToFloat64(gauge), "a drop takes back everything the column added")
			}
			require.NoError(t, b.Shutdown(ctx))
			shut = true
			assert.Nil(t, b.KeyDocColumn())
			assert.Zero(t, testutil.ToFloat64(gauge), "the gauge ends at zero, subtracted once")
		})
	}
}

// A size report blocked on keyDocColumnSizeMu across a concurrent drop credits nothing to the dropped column.
func TestKeyDocColumnSizeReportBlockedAcrossADropCreditsNothing(t *testing.T) {
	ctx := context.Background()
	b := createTestBucketRoaringSetWithKeyDocColumn(t, ctx, t.TempDir(), 100)
	t.Cleanup(func() { assert.NoError(t, b.Shutdown(ctx)) })
	require.NoError(t, b.RoaringSetAddOne([]byte("k"), 1))
	require.NoError(t, b.FlushAndSwitch())
	sg := b.disk
	require.NotNil(t, sg.keyDocColumn.Load())
	require.Positive(t, sg.keyDocColumnSizeReported)

	sg.keyDocColumnSizeMu.Lock()
	locked := true
	defer func() {
		if locked {
			sg.keyDocColumnSizeMu.Unlock()
		}
	}()
	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(done)
		sg.reportKeyDocColumnSize()
	}, sg.logger)

	deadline := time.Now().Add(5 * time.Second)
	for !reporterBlockedOnLock() {
		require.True(t, time.Now().Before(deadline),
			"the reporter never blocked on keyDocColumnSizeMu")
		runtime.Gosched()
	}
	if idx := sg.keyDocColumn.Swap(nil); idx != nil {
		idx.Close()
	}
	locked = false
	sg.keyDocColumnSizeMu.Unlock()
	<-done

	assert.Zero(t, sg.keyDocColumnSizeReported,
		"a report blocked across a drop credits the dropped column")
}

// A flattening that finds the column no longer worth holding drops it, and its bytes with it.
func TestKeyDocColumnUnfitFlatteningDropsTheColumn(t *testing.T) {
	ctx := context.Background()
	b, gauge := newKeyDocColumnBucketWithGauge(t, ctx)
	t.Cleanup(func() { assert.NoError(t, b.Shutdown(ctx)) })

	// each flush stays under the overflow floor on its own; their union does not
	const perFlush = 130
	for f := range 8 { // flattenLayersThreshold
		from := uint64(2 + f*perFlush)
		docs := make([]uint64, perFlush)
		for i := range docs {
			docs[i] = from + uint64(i)
		}
		require.NoError(t, b.RoaringSetAddList([]byte("base"), docs))
		require.NoError(t, b.FlushAndSwitch())
		if f < 7 { // the eighth flush starts the flattening that drops the column
			require.NotNil(t, b.KeyDocColumn(), "flush %d is absorbed", f)
		}
	}

	require.Eventually(t, func() bool {
		return b.KeyDocColumn() == nil && testutil.ToFloat64(gauge) == 0
	}, 5*time.Second, time.Millisecond, "the unfit column is dropped and its bytes subtracted")

	bm, release, err := b.RoaringSetGet(ctx, []byte("base"))
	require.NoError(t, err)
	defer release()
	assert.Equal(t, 1+8*perFlush, bm.GetCardinality(), "every document still reads from the segments")
}

// A flattening deferred for memory is restarted by the compaction cycle, on its own goroutine.
func TestKeyDocColumnDeferredFlatteningStartedByCompaction(t *testing.T) {
	ctx := context.Background()
	// no pair of segments fits, so compaction never has work and the cycle's
	// answer is the flattening's alone
	b, _ := newKeyDocColumnBucketWithGauge(t, ctx, WithMaxSegmentSize(1))
	t.Cleanup(func() { assert.NoError(t, b.Shutdown(ctx)) })
	sg := b.disk
	checker := newFlattenAllocChecker(t, b)
	checker.refuse.Store(true)

	cycle := func() bool { t.Helper(); return compactionCycle(t, sg) }
	require.False(t, cycle(), "nothing to compact and nothing pending")
	// cleanup overdue, so this cycle takes the cleanup-first arm
	sg.lastCleanupCall, sg.lastCompactionCall = time.Time{}, time.Now()
	require.False(t, cycle(), "nothing to clean up either")

	flushPastFlattenThreshold(t, b)
	require.Eventually(t, func() bool { return checker.asked.Load() == 1 },
		5*time.Second, time.Millisecond, "the flush past the threshold asks for memory")
	checker.refuse.Store(false)

	// the deferred flattening marks itself pending just after it is refused
	deadline := time.Now().Add(5 * time.Second)
	for checker.asked.Load() < 2 {
		require.True(t, time.Now().Before(deadline), "the compaction cycle never restarted the flattening")
		require.False(t, cycle(), "a restart is not work: one refused again would pin the cycle at its fastest")
		runtime.Gosched()
	}
	assert.False(t, b.KeyDocColumn().StartPendingFlatten(), "one in flight is not started twice")

	layeredBytes := b.KeyDocColumn().Info().SizeBytes
	checker.grant(t, "the restarted flattening is waiting")
	require.Eventually(t, func() bool { return b.KeyDocColumn().Info().SizeBytes < layeredBytes },
		5*time.Second, time.Millisecond, "the restarted flattening folds the layers in")
	require.Eventually(t, func() bool { return !keyDocColumnFlattening() }, 5*time.Second, time.Millisecond)
	assert.False(t, b.KeyDocColumn().StartPendingFlatten(), "and leaves nothing pending")
}

// holdFlattenSlots takes n flatten slots until cleanup, and returns a func giving one back early.
func holdFlattenSlots(t *testing.T, n int) (releaseOne func()) {
	t.Helper()
	releases := make([]func(), 0, n)
	t.Cleanup(func() {
		for _, release := range releases {
			release()
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for len(releases) < n {
		require.True(t, time.Now().Before(deadline), "another flattening never gave its slot back")
		if release, ok := acquireKeyDocColumnFlattenSlot(); ok {
			releases = append(releases, release)
		}
		runtime.Gosched()
	}
	return func() {
		releases[0]()
		releases = releases[1:]
	}
}

// A cycle whose compaction has work must still restart a deferred flattening.
func TestKeyDocColumnDeferredFlatteningStartedAlongsideCompaction(t *testing.T) {
	ctx := context.Background()
	b, _ := newKeyDocColumnBucketWithGauge(t, ctx)
	t.Cleanup(func() { assert.NoError(t, b.Shutdown(ctx)) })
	checker := newFlattenAllocChecker(t, b)
	b.disk.allocChecker = compactionPassingChecker{checker}
	checker.refuse.Store(true)

	flushPastFlattenThreshold(t, b)
	require.Eventually(t, func() bool { return checker.asked.Load() == 1 && !keyDocColumnFlattening() },
		5*time.Second, time.Millisecond, "the flush past the threshold is refused and marked pending")
	checker.refuse.Store(false)

	require.True(t, compactionCycle(t, b.disk), "the flushed segments give compaction work")
	require.Eventually(t, func() bool { return checker.asked.Load() == 2 },
		5*time.Second, time.Millisecond, "the cycle that compacted restarted the flattening too")
	layeredBytes := b.KeyDocColumn().Info().SizeBytes
	checker.grant(t, "the restarted flattening is waiting")
	require.Eventually(t, func() bool { return b.KeyDocColumn().Info().SizeBytes < layeredBytes },
		5*time.Second, time.Millisecond, "the restarted flattening folds the layers in")
}

// flushPastFlattenThreshold flushes flattenLayersThreshold (8) layers of one new key each.
func flushPastFlattenThreshold(t *testing.T, b *Bucket) {
	t.Helper()
	for i := range 8 {
		require.NoError(t, b.RoaringSetAddOne(fmt.Appendf(nil, "k%d", i), uint64(10+i)))
		require.NoError(t, b.FlushAndSwitch())
	}
}

// A flattening refused a rebuild slot stays pending, so the process-wide bound holds.
func TestKeyDocColumnFlatteningWaitsForARebuildSlot(t *testing.T) {
	ctx := context.Background()
	b, _ := newKeyDocColumnBucketWithGauge(t, ctx)
	t.Cleanup(func() { assert.NoError(t, b.Shutdown(ctx)) })
	checker := newFlattenAllocChecker(t, b)

	releaseOne := holdFlattenSlots(t, keyDocColumnFlattenSlots)

	flushPastFlattenThreshold(t, b)
	layeredBytes := b.KeyDocColumn().Info().SizeBytes
	require.Eventually(t, b.KeyDocColumn().StartPendingFlatten, 5*time.Second, time.Millisecond,
		"refused a slot, the flattening is left pending")
	require.Eventually(t, func() bool { return !keyDocColumnFlattening() }, 5*time.Second, time.Millisecond)
	require.Equal(t, layeredBytes, b.KeyDocColumn().Info().SizeBytes, "nothing was flattened without a slot")
	require.Zero(t, checker.asked.Load(), "nor was memory asked for without one")

	releaseOne()
	// or the attempt started above, unseen by keyDocColumnFlattening until it is
	// scheduled, takes the freed slot itself
	require.Eventually(t, func() bool {
		return b.KeyDocColumn().StartPendingFlatten() || keyDocColumnFlattening()
	}, 5*time.Second, time.Millisecond, "the flattening is still pending")
	checker.grant(t, "with a slot, the restarted flattening asks for memory")
	require.Eventually(t, func() bool { return b.KeyDocColumn().Info().SizeBytes < layeredBytes },
		5*time.Second, time.Millisecond, "with a slot free the flattening folds the layers in")
}

// A query between a flush's merge and its swap must find the flushed documents, not miss them in both places.
func TestKeyDocColumnMergesAFlushBeforeItsSwap(t *testing.T) {
	ctx := context.Background()
	b, _ := newFlushedKeyDocColumnBucket(t, ctx, 1, 100)
	require.NoError(t, b.RoaringSetAddOne([]byte("flushed"), 7))
	unmerged := b.KeyDocColumn().Info().SizeBytes

	// FlushAndSwitch by hand, so the swap can be held off at a known point
	b.flushAndSwitchMu.Lock()
	switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
	if !switched || err != nil {
		b.flushAndSwitchMu.Unlock()
		require.FailNow(t, "the memtable did not switch", "%v", err)
	}
	b.flushLock.RLock() // the swap needs flushLock exclusively
	flushed := make(chan error, 1)
	enterrors.GoWrapper(func() {
		defer b.flushAndSwitchMu.Unlock()
		flushed <- b.flushFlushingLocked(b.atomicallyAddDiskSegmentAndRemoveFlushing)
	}, b.logger)
	finish := sync.OnceValue(func() error {
		b.flushLock.RUnlock()
		return <-flushed
	})
	t.Cleanup(func() { finish() })

	require.Eventually(t, func() bool { return b.KeyDocColumn().Info().SizeBytes > unmerged },
		5*time.Second, time.Millisecond, "the flush must reach the column while its swap is held off")

	view := RoaringSetConsistentView{
		BucketConsistentView: b.consistentViewNoLock(),
		KeyDocColumnState:    b.disk.keyDocColumn.Load().State(),
	}
	require.NotNil(t, view.Flushing, "the swap has not run")
	r, err := view.WithoutEmptyActiveMemtable().NewRoaringSetKeyDocColumnReader(sortedKeysOf(t, []string{"flushed"}))
	require.NoError(t, err)
	res, err := r.Resolve(ctx)
	require.NoError(t, err)
	bm, put := res.Materialize(roaringset.NewBitmapBufPoolNoop())
	assert.Equal(t, []uint64{7}, bm.ToArray(), "found in the memtable and the column, once")
	put()
	view.ReleaseView()

	require.NoError(t, finish())
}

// compactionPassingChecker grants compaction's fixed up-front ask without counting it.
type compactionPassingChecker struct{ *flattenAllocChecker }

func (c compactionPassingChecker) CheckAlloc(bytes int64) error {
	if bytes == 100*1024*1024 {
		return nil
	}
	return c.flattenAllocChecker.CheckAlloc(bytes)
}

// compactionCycle runs one compaction cycle, failing if it waits on a flattening.
func compactionCycle(t *testing.T, sg *SegmentGroup) bool {
	t.Helper()
	done := make(chan bool, 1)
	enterrors.GoWrapper(func() { done <- sg.compactOrCleanup(func() bool { return false }) }, sg.logger)
	select {
	case worked := <-done:
		return worked
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the compaction cycle ran the flattening on its own goroutine")
		return false
	}
}

func keyDocColumnFlattening() bool {
	buf := make([]byte, 1<<20)
	return strings.Contains(string(buf[:runtime.Stack(buf, true)]), "keydoccolumn.(*Index).startFlatten.func")
}

func reporterBlockedOnLock() bool {
	buf := make([]byte, 1<<20)
	for _, g := range strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
		if strings.Contains(g, "reportKeyDocColumnSize") && strings.Contains(g, "sync.(*Mutex).Lock") {
			return true
		}
	}
	return false
}
