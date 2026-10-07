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

package hnsw

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	monitoringhelpers "github.com/weaviate/weaviate/usecases/monitoring/testinghelpers"
)

// The startup metrics live on the default registry shared by every test in
// the binary, so every assertion below compares against a reading taken just
// before the action under test.

func hnswRestoreCount(t *testing.T) uint64 {
	t.Helper()
	n, err := monitoringhelpers.SampleCount(prometheus.DefaultGatherer,
		"weaviate_vector_index_restore_duration_seconds", nil)
	require.NoError(t, err)
	return n
}

func hnswPrefillCount(t *testing.T) uint64 {
	t.Helper()
	n, err := monitoringhelpers.SampleCount(prometheus.DefaultGatherer,
		"weaviate_vector_cache_prefill_duration_seconds", nil)
	require.NoError(t, err)
	return n
}

// startupMetricsHarness builds indexes over the same commit log directory
// whose VectorForID can be parked or made to panic, so a test can reopen an
// index and hold its prefill open or blow it up.
type startupMetricsHarness struct {
	cfg       Config
	uc        ent.UserConfig
	store     *lsmkv.Store
	vectors   [][]float32
	armed     atomic.Bool
	panicking atomic.Bool
	entered   chan struct{}
	released  chan struct{}
}

func newStartupMetricsHarness(t *testing.T, indexID string, waitForPrefill bool) *startupMetricsHarness {
	t.Helper()
	tempDir := t.TempDir()
	logger, _ := test.NewNullLogger()
	vectors, _ := testinghelpers.RandomVecs(5, 0, 8)

	h := &startupMetricsHarness{
		store:    testinghelpers.NewDummyStoreFromFolder(tempDir, t),
		vectors:  vectors,
		entered:  make(chan struct{}, 1),
		released: make(chan struct{}),
	}
	h.uc = ent.UserConfig{}
	h.uc.SetDefaults()
	h.cfg = Config{
		RootPath:            tempDir,
		ID:                  indexID,
		DistanceProvider:    distancer.NewL2SquaredProvider(),
		WaitForCachePrefill: waitForPrefill,
		AllocChecker:        memwatch.NewDummyMonitor(),
		MakeBucketOptions:   lsmkv.MakeNoopBucketOptions,
		MakeCommitLoggerThunk: func(opts ...CommitlogOption) (CommitLogger, error) {
			return NewCommitLogger(tempDir, indexID, logger, cyclemanager.NewCallbackGroupNoop(), opts...)
		},
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if h.panicking.Load() {
				panic("prefill panic injected by the test")
			}
			if h.armed.Load() {
				select {
				case h.entered <- struct{}{}:
				default:
				}
				<-h.released
			}
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[int(id)], nil
		},
		GetViewThunk: func() common.BucketView { return &noopBucketView{} },
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice, view common.BucketView) ([]float32, error) {
			copy(container.Slice, vectors[int(id)])
			return container.Slice, nil
		},
	}
	return h
}

func (h *startupMetricsHarness) newIndex(t *testing.T) *hnsw {
	t.Helper()
	index, err := New(h.cfg, h.uc, cyclemanager.NewCallbackGroupNoop(), h.store)
	require.NoError(t, err)
	return index
}

// fill writes the harness vectors through index and shuts it down, leaving
// commit-log state for the next newIndex to restore.
func (h *startupMetricsHarness) fill(t *testing.T, index *hnsw) {
	t.Helper()
	ctx := context.Background()
	for id, vec := range h.vectors {
		require.NoError(t, index.Add(ctx, uint64(id), vec))
	}
	require.NoError(t, index.Flush())
	require.NoError(t, index.Shutdown(ctx))
	h.store.FlushMemtables(ctx)
}

func TestStartupMetricsRestoreObservedOnlyWithState(t *testing.T) {
	ctx := context.Background()
	h := newStartupMetricsHarness(t, "restore_metrics_test", true)

	before := hnswRestoreCount(t)
	index := h.newIndex(t)
	require.Equal(t, before, hnswRestoreCount(t), "a fresh index has no state to restore")

	h.fill(t, index)
	index = h.newIndex(t)
	defer index.Shutdown(ctx)

	require.Equal(t, before+1, hnswRestoreCount(t),
		"reopening an index with commit-log state restores it exactly once")
}

func TestStartupMetricsPrefillObservedOnce(t *testing.T) {
	tests := []struct {
		name string
		wait bool
	}{
		{name: "a prefill the shard load waits for", wait: true},
		{name: "a prefill that runs in the background", wait: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			h := newStartupMetricsHarness(t, fmt.Sprintf("prefill_wait_%t", tt.wait), tt.wait)
			h.fill(t, h.newIndex(t))
			index := h.newIndex(t)
			defer index.Shutdown(ctx)

			before := hnswPrefillCount(t)

			index.PostStartup(ctx)
			index.prefillWg.Wait()

			require.Equal(t, before+1, hnswPrefillCount(t), "a completed prefill is observed once")
		})
	}
}

func TestStartupMetricsFreshIndexPrefillsNothing(t *testing.T) {
	ctx := context.Background()
	h := newStartupMetricsHarness(t, "prefill_fresh", true)
	index := h.newIndex(t)
	defer index.Shutdown(ctx)

	before := hnswPrefillCount(t)
	index.PostStartup(ctx)
	index.prefillWg.Wait()

	require.Equal(t, before, hnswPrefillCount(t),
		"an index with nothing restored has no cache to prefill")
}

func TestStartupMetricsAbortedPrefillNotObserved(t *testing.T) {
	ctx := context.Background()
	h := newStartupMetricsHarness(t, "prefill_abort", false)
	h.fill(t, h.newIndex(t))
	index := h.newIndex(t)

	before := hnswPrefillCount(t)

	// New() replays the commit log through the same thunk, so arm only once
	// the index is built.
	h.armed.Store(true)
	index.PostStartup(ctx)

	select {
	case <-h.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("prefill never reached VectorForID")
	}

	done := make(chan error, 1)
	go func() { done <- index.Shutdown(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned while the prefill was still reading: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(h.released)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Shutdown did not return after the prefill finished")
	}

	require.Equal(t, before, hnswPrefillCount(t),
		"a prefill cut short by shutdown must not record a misleadingly short duration")
}

// A prefill that panics is recovered by the goroutine wrapper (or by the
// shard's recover in sync mode) and the process keeps running, so the run
// must not count as a completed prefill.
func TestStartupMetricsPanickingPrefillRecordsNoDuration(t *testing.T) {
	// The integration CI job runs with DISABLE_RECOVERY_ON_PANIC, which makes
	// the goroutine wrapper re-raise instead of recover and would take the
	// whole test binary down; this test is about the production default.
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	ctx := context.Background()
	h := newStartupMetricsHarness(t, "prefill_panic", false)
	h.fill(t, h.newIndex(t))
	index := h.newIndex(t)
	defer index.Shutdown(ctx)

	before := hnswPrefillCount(t)

	// New() replays the commit log through the same thunk, so arm only once
	// the index is built.
	h.panicking.Store(true)
	index.PostStartup(ctx)
	index.prefillWg.Wait()

	require.Equal(t, before, hnswPrefillCount(t),
		"a prefill that panicked is not a completed prefill")
}

// startup_progress was never set, so minting a series for it exports a
// permanent zero per shard for nothing.
func TestStartupProgressSeriesNotMinted(t *testing.T) {
	prom := monitoring.GetMetrics()

	before := testutil.CollectAndCount(prom.StartupProgress)
	newMetrics(prom, "StartupProgressNotMinted", "not-minted-shard", false)

	require.Equal(t, before, testutil.CollectAndCount(prom.StartupProgress),
		"building the hnsw metrics must not mint a startup_progress series")
}
