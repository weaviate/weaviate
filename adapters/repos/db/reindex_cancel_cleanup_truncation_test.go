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
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// loadAttemptMonitor counts shard-load attempts and runs onNthCall on the nth
// one, so a cancellation or a collection delete can land in the middle of the
// walk. nthCall of 0 never runs it.
//
// It fails the load itself by default, which is a shard breaking for its own
// reason. admitLoad passes the load through instead, so the attempt goes on to
// the load-permit wait — the step that takes the sweep's context and is
// therefore the one an abort stops.
type loadAttemptMonitor struct {
	mu        sync.Mutex
	onNthCall func()
	nthCall   int
	calls     int
	admitLoad bool
}

func (m *loadAttemptMonitor) CheckAlloc(sizeInBytes int64) error { return nil }

func (m *loadAttemptMonitor) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls == m.nthCall && m.onNthCall != nil {
		m.onNthCall()
	}
	if m.admitLoad {
		return nil
	}
	return errors.New("memory pressure")
}

func (m *loadAttemptMonitor) Refresh(updateMappings bool) {}

// newSweepLoadLimiter is the limiter a real shard load waits on. A load that
// reaches it with the sweep's context already gone reports the cancellation
// the same way production does, rather than an error the test invented.
func newSweepLoadLimiter() *loadlimiter.LoadLimiter {
	return loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "sweep_test", 1)
}

// tenantShardNames builds n shard names, for the cases that need more of them
// than an operator-facing message is allowed to carry.
func tenantShardNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("tenant-%03d", i)
	}
	return out
}

// Pins CleanStalePartialReindexState's three outcomes — clean, truncated, and
// collection-dropped — against a walk that fails, aborts, or never starts. See
// the function doc for why each is reported differently.
func TestCleanStalePartialReindexStateReportsATruncatedSweep(t *testing.T) {
	tests := []struct {
		name   string
		shards []string
		// cancelOnCall aborts the sweep's context on the nth shard load; 0
		// lets the walk run to the end.
		cancelOnCall int
		// cancelStopsTheLoad makes the abort itself the reason the load fails:
		// the load is admitted past the memory check and stops on the permit
		// wait, which takes the sweep's context. Without it the load fails for
		// a reason of its own that the abort merely coincides with.
		cancelStopsTheLoad bool
		// dropOnCall deletes the collection on the nth shard load, so the
		// delete lands after that shard has already failed.
		dropOnCall int
		// closing closes the index (walk visits nothing); closeCause is what
		// it was signalled with, nil for an unsignalled close.
		closing    bool
		closeCause error
		// staleOnDisk writes a tracker dir for the swept tuple on every
		// shard, which is what puts them all on the sweep's list.
		staleOnDisk bool
		// indexType is "filterable" unless set.
		indexType     string
		wantErr       bool
		wantTruncated bool
		wantDropped   bool
		// wantShardErr expects the sweep to have reached a shard and failed on it.
		wantShardErr bool
		// wantShardNamed expects the shard the sweep stopped at to be named,
		// whichever marker carries it.
		wantShardNamed bool
		// wantShardsNamed is how many failing shards the message may name; 0
		// skips the check.
		wantShardsNamed int
	}{
		{
			name:        "no shards is a clean sweep",
			shards:      nil,
			staleOnDisk: true,
		},
		{
			name:   "one shard, nothing to clean",
			shards: []string{"shard-a"},
		},
		{
			// Refused before the walk starts; no shard is loaded or swept.
			name:          "an index type this build cannot map is refused, not swept",
			shards:        []string{"shard-a"},
			staleOnDisk:   true,
			indexType:     "an-index-type-this-build-does-not-know",
			wantErr:       true,
			wantTruncated: true,
		},
		{
			name:           "one shard that cannot be loaded",
			shards:         []string{"shard-a"},
			staleOnDisk:    true,
			wantErr:        true,
			wantShardErr:   true,
			wantShardNamed: true,
			wantTruncated:  false,
		},
		{
			// A full disk fails every tenant at once; the message caps how many it names.
			name:            "more failing shards than a message can carry",
			shards:          tenantShardNames(15),
			staleOnDisk:     true,
			wantErr:         true,
			wantShardErr:    true,
			wantShardNamed:  true,
			wantShardsNamed: maxReportedErrors,
		},
		{
			// The shard broke for its own reason; the abort only happened at the
			// same moment. Reading that as a run out of time hides a broken shard
			// behind the warning routine tenant churn produces, which is the one
			// failure mode worse than the false alarm. The second shard really
			// was left unvisited, so both markers are on the error.
			name:           "a shard breaks for its own reason as the abort lands",
			shards:         []string{"shard-a", "shard-b"},
			cancelOnCall:   1,
			staleOnDisk:    true,
			wantErr:        true,
			wantTruncated:  true,
			wantShardErr:   true,
			wantShardNamed: true,
		},
		{
			// The abort IS why this shard was not swept, so it is one the run
			// never finished rather than one it found broken. Tagging it failed
			// would report confirmed state on a shard nothing looked at, at the
			// severity reserved for an operator having to act.
			name:               "the abort stops the load itself",
			shards:             []string{"shard-a", "shard-b"},
			cancelOnCall:       1,
			cancelStopsTheLoad: true,
			staleOnDisk:        true,
			wantErr:            true,
			wantTruncated:      true,
			wantShardErr:       false,
			wantShardNamed:     true,
		},
		{
			// The walk visits nothing here, so "swept every shard" would be a
			// report about work that never happened.
			name:          "a node shutting down leaves every shard unswept",
			shards:        []string{"shard-a", "shard-b"},
			closing:       true,
			closeCause:    errIndexShutdown,
			staleOnDisk:   true,
			wantErr:       true,
			wantTruncated: true,
		},
		{
			// Nothing the walk skipped is reachable once the class is gone, so
			// this must not report truncation or promise a retry.
			name:        "a collection being deleted leaves nothing worth sweeping",
			shards:      []string{"shard-a", "shard-b"},
			closing:     true,
			closeCause:  errIndexDropped,
			staleOnDisk: true,
			wantErr:     true,
			wantDropped: true,
		},
		{
			// A shard fails, then the collection is deleted before the walk ends.
			name:           "a shard fails and the collection is deleted before the walk ends",
			shards:         []string{"shard-a", "shard-b"},
			dropOnCall:     1,
			staleOnDisk:    true,
			wantErr:        true,
			wantDropped:    true,
			wantShardErr:   true,
			wantShardNamed: true,
		},
		{
			// A close nobody named a cause for could still be a shutdown, and
			// the shards would then really be left behind.
			name:          "a close with no cause is treated as a shutdown",
			shards:        []string{"shard-a", "shard-b"},
			closing:       true,
			staleOnDisk:   true,
			wantErr:       true,
			wantTruncated: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			monitor := &loadAttemptMonitor{admitLoad: tc.cancelStopsTheLoad}
			var limiter *loadlimiter.LoadLimiter
			if tc.cancelStopsTheLoad {
				limiter = newSweepLoadLimiter()
			}

			logger := logrus.New()
			logger.SetOutput(io.Discard)
			closingCtx, closeIndex := context.WithCancel(context.Background())
			defer closeIndex()
			closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
			defer signalCloseRequested(nil)
			idx := &Index{
				Config:               IndexConfig{RootPath: t.TempDir(), ClassName: "Movies"},
				closingCtx:           closingCtx,
				closeRequestedCtx:    closeRequestedCtx,
				signalCloseRequested: signalCloseRequested,
				logger:               logger,
			}
			// The real teardowns signal the cause first and cancel closingCtx
			// once they hold the lock.
			dropCollection := func() {
				signalCloseRequested(errIndexDropped)
				closeIndex()
			}
			switch {
			case tc.cancelOnCall > 0:
				monitor.nthCall, monitor.onNthCall = tc.cancelOnCall, cancel
			case tc.dropOnCall > 0:
				monitor.nthCall, monitor.onNthCall = tc.dropOnCall, dropCollection
			}
			if tc.closing {
				if tc.closeCause != nil {
					signalCloseRequested(tc.closeCause)
				}
				closeIndex()
			}
			for _, name := range tc.shards {
				if tc.staleOnDisk {
					mkTrackerDir(t, shardPathLSM(idx.path(), name),
						"enable_filterable_title_1", "started.mig")
				}
				(*sync.Map)(&idx.shards).Store(name, &LazyLoadShard{
					shardOpts:        &deferredShardOpts{name: name, index: idx, class: &models.Class{Class: "Movies"}},
					memMonitor:       monitor,
					shardLoadLimiter: limiter,
				})
			}

			if tc.closing {
				require.NoError(t, idx.ForEachShard(func(name string, _ ShardLike) error {
					t.Errorf("a closing index walked shard %q", name)
					return nil
				}), "ForEachShard answers a closing index as a walk that reached every shard, "+
					"which is why the sweep cannot use it")
			}

			indexType := tc.indexType
			if indexType == "" {
				indexType = "filterable"
			}
			err := idx.cleanStalePartialReindexState(ctx, "title", indexType, nil)

			if !tc.wantErr {
				require.NoError(t, err,
					"a sweep that reached every shard must not report anything for the caller to act on")
				return
			}
			require.Error(t, err)
			if tc.wantTruncated {
				require.ErrorIs(t, err, ErrCleanupSweepTruncated,
					"shards the walk never reached were never swept, and the caller has to know")
			} else {
				require.NotErrorIs(t, err, ErrCleanupSweepTruncated,
					"every shard was visited, or the collection is being deleted, so nothing "+
						"is left unswept that a later sweep could still reach")
			}
			if tc.wantDropped {
				require.ErrorIs(t, err, ErrCleanupCollectionDropped,
					"a collection being deleted puts its leftover state out of reach, and the "+
						"caller must not report unswept shards or promise a retry for it")
			} else {
				require.NotErrorIs(t, err, ErrCleanupCollectionDropped,
					"the collection is not being deleted, so its state outlives this sweep")
			}
			if tc.wantShardNamed {
				require.Contains(t, err.Error(), "unwrap for partial-reindex cleanup",
					"the shard the sweep stopped at must still be reported")
			}
			if tc.wantShardErr {
				require.ErrorIs(t, err, ErrCleanupShardFailed,
					"a shard the sweep reached and could not sweep has to be tagged as one, "+
						"or a caller that asks only about the delete reports state as gone "+
						"while that shard's is still on disk")
			} else {
				require.NotErrorIs(t, err, ErrCleanupShardFailed,
					"no shard was reached and failed, so nothing was left on one")
			}
			if tc.wantShardsNamed > 0 {
				require.Equal(t, tc.wantShardsNamed,
					strings.Count(err.Error(), "unwrap for partial-reindex cleanup"),
					"an operator cannot read a message with one entry per tenant")
				require.Contains(t, err.Error(),
					fmt.Sprintf("(and %d more)", len(tc.shards)-tc.wantShardsNamed),
					"the count is what says how many shards were left behind")
			}
			require.Equal(t, tc.wantDropped && !tc.wantShardErr, IsCleanupCollectionDropped(err),
				"a delete only speaks for the whole sweep when the sweep left nothing behind")
		})
	}
}

// A sweep starting in the window between the delete signal and the teardown's
// closingCtx cancel must not hydrate cold tenants and report a clean sweep.
func TestCleanStalePartialReindexStateRefusesAnAlreadyRequestedClose(t *testing.T) {
	tests := []struct {
		name string
		// requestedCause is what DeleteIndex or Shutdown signalled; nil stays open.
		requestedCause error
		wantDropped    bool
		wantTruncated  bool
	}{
		{
			// The control: the same fixture hydrates when nothing is closing.
			name: "an open index hydrates the shards with state on them",
		},
		{
			name:           "a delete already requested",
			requestedCause: errIndexDropped,
			wantDropped:    true,
		},
		{
			name:           "a shutdown already requested",
			requestedCause: errIndexShutdown,
			wantTruncated:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := logrus.New()
			logger.SetOutput(io.Discard)
			closingCtx, closeIndex := context.WithCancel(context.Background())
			defer closeIndex()
			closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
			defer signalCloseRequested(nil)
			idx := &Index{
				Config:               IndexConfig{RootPath: t.TempDir(), ClassName: "Movies"},
				closingCtx:           closingCtx,
				closeRequestedCtx:    closeRequestedCtx,
				signalCloseRequested: signalCloseRequested,
				logger:               logger,
			}

			monitor := &loadAttemptMonitor{}
			for _, name := range []string{"shard-a", "shard-b"} {
				mkTrackerDir(t, shardPathLSM(idx.path(), name),
					"enable_filterable_title_1", "started.mig")
				(*sync.Map)(&idx.shards).Store(name, &LazyLoadShard{
					shardOpts:  &deferredShardOpts{name: name, index: idx, class: &models.Class{Class: "Movies"}},
					memMonitor: monitor,
				})
			}

			// Signalled only: closingCtx stays live for the whole window.
			if tc.requestedCause != nil {
				signalCloseRequested(tc.requestedCause)
			}

			err := idx.cleanStalePartialReindexState(
				context.Background(), "title", "filterable", nil)

			require.Error(t, err)
			if tc.requestedCause == nil {
				require.ErrorIs(t, err, ErrCleanupShardFailed)
				require.Positive(t, monitor.calls,
					"the fixture must be one the sweep would otherwise hydrate")
				return
			}
			require.Zero(t, monitor.calls,
				"a collection on its way out must not be hydrated to sweep state that is about to be deleted")
			require.ErrorIs(t, err, tc.requestedCause)
			if tc.wantDropped {
				require.True(t, IsCleanupCollectionDropped(err),
					"the state goes away with the collection, so this is not a retryable truncation")
			}
			if tc.wantTruncated {
				require.ErrorIs(t, err, ErrCleanupSweepTruncated,
					"a shutdown leaves the state on disk, so the caller has to know it was not swept")
			}
		})
	}
}

func TestClassifyIncompleteWalk(t *testing.T) {
	unmarked := errors.New("something no close cause covers")

	tests := []struct {
		name string
		err  error
		// wantMarker is the sweep-level error the cause must be tagged with;
		// nil means the error passes through untouched.
		wantMarker error
	}{
		{
			name:       "the collection is being deleted",
			err:        errIndexDropped,
			wantMarker: ErrCleanupCollectionDropped,
		},
		{
			name:       "the node is shutting down",
			err:        errIndexShutdown,
			wantMarker: ErrCleanupSweepTruncated,
		},
		{
			name:       "the index closed without signalling why",
			err:        errIndexClosed,
			wantMarker: ErrCleanupSweepTruncated,
		},
		{
			name:       "the walk skipped a shard nothing explained",
			err:        fmt.Errorf("%w: shard-b", errShardsSkipped),
			wantMarker: ErrCleanupSweepTruncated,
		},
		{
			name: "an error no close cause covers",
			err:  unmarked,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyIncompleteWalk(tc.err)
			require.ErrorIs(t, got, tc.err, "the cause must stay readable under the marker")
			if tc.wantMarker == nil {
				require.Equal(t, tc.err, got)
				return
			}
			require.ErrorIs(t, got, tc.wantMarker)
		})
	}
}

// Pins forEachShardStrict against a close landing mid-walk. See its doc for
// why a whole-index drop is the only deleter that signals a cause.
func TestForEachShardStrictReportsACloseThatLandsMidWalk(t *testing.T) {
	tests := []struct {
		name string
		// closeCause is signalled mid-walk, after the first shard; nil deletes
		// the remaining shards without signalling anything.
		closeCause error
		// deleteSiblings drops the shards the walk has not reached yet.
		deleteSiblings bool
		wantErr        error
	}{
		{
			name: "a walk nothing interrupted is clean",
		},
		{
			name:           "a collection deleted mid-walk is not a swept collection",
			closeCause:     errIndexDropped,
			deleteSiblings: true,
			wantErr:        errIndexDropped,
		},
		{
			name:           "a node shutting down mid-walk is not a swept collection",
			closeCause:     errIndexShutdown,
			deleteSiblings: true,
			wantErr:        errIndexShutdown,
		},
		{
			name:           "a tenant deleted mid-walk explains nothing and is still not swept",
			deleteSiblings: true,
			wantErr:        errShardsSkipped,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := logrus.New()
			logger.SetOutput(io.Discard)
			closingCtx, closeIndex := context.WithCancel(context.Background())
			defer closeIndex()
			closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
			defer signalCloseRequested(nil)
			idx := &Index{
				Config:               IndexConfig{RootPath: t.TempDir(), ClassName: "Movies"},
				closingCtx:           closingCtx,
				closeRequestedCtx:    closeRequestedCtx,
				signalCloseRequested: signalCloseRequested,
				logger:               logger,
			}
			shardNames := []string{"shard-a", "shard-b", "shard-c"}
			for _, name := range shardNames {
				(*sync.Map)(&idx.shards).Store(name, &LazyLoadShard{
					shardOpts: &deferredShardOpts{name: name, index: idx, class: &models.Class{Class: "Movies"}},
				})
			}

			var visited int
			err := idx.forEachShardStrict(func(name string, _ ShardLike) error {
				visited++
				if visited > 1 || !tc.deleteSiblings {
					return nil
				}
				// Whole-index drop order: signal the cause, then remove shards.
				if tc.closeCause != nil {
					signalCloseRequested(tc.closeCause)
					closeIndex()
				}
				for _, other := range shardNames {
					if other != name {
						idx.shards.LoadAndDelete(other)
					}
				}
				return nil
			})

			if tc.wantErr == nil {
				require.NoError(t, err)
				require.Equal(t, len(shardNames), visited)
				return
			}
			require.ErrorIs(t, err, tc.wantErr,
				"the shards the walk never reached were never swept, so a nil here would "+
					"report a sweep of a collection that is gone")
		})
	}
}

// Pins that closeCause never panics on an Index missing its close contexts —
// a test double, or a future construction path that skips them.
func TestCloseCauseAnswersAnIndexWithoutCloseContexts(t *testing.T) {
	closedCtx, closeIndex := context.WithCancel(context.Background())
	closeIndex()
	openCtx, keepOpen := context.WithCancel(context.Background())
	defer keepOpen()
	droppedCtx, signalDropped := context.WithCancelCause(context.Background())
	signalDropped(errIndexDropped)

	tests := []struct {
		name              string
		closingCtx        context.Context
		closeRequestedCtx context.Context
		want              error
	}{
		{
			name: "no contexts at all reads as open",
		},
		{
			name:       "an open index without a close-requested context is open",
			closingCtx: openCtx,
		},
		{
			name:              "a closed index without a close-requested context is a shutdown",
			closingCtx:        closedCtx,
			closeRequestedCtx: nil,
			want:              errIndexClosed,
		},
		{
			name:              "a closed index still reports the cause it has",
			closingCtx:        closedCtx,
			closeRequestedCtx: droppedCtx,
			want:              errIndexDropped,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := &Index{closingCtx: tc.closingCtx, closeRequestedCtx: tc.closeRequestedCtx}

			var cause error
			require.NotPanics(t, func() { cause = idx.closeCause() })

			if tc.want == nil {
				require.NoError(t, cause)
				return
			}
			require.ErrorIs(t, cause, tc.want)
		})
	}
}

// The count of skipped shards must survive the [maxReportedErrors] cap.
func TestReportedShardNames(t *testing.T) {
	nameSet := func(names ...string) map[string]struct{} {
		out := map[string]struct{}{}
		for _, name := range names {
			out[name] = struct{}{}
		}
		return out
	}

	tests := []struct {
		name  string
		names map[string]struct{}
		want  []string
	}{
		{
			name:  "a walk that reached every shard",
			names: nameSet(),
			want:  []string{},
		},
		{
			name:  "one shard skipped",
			names: nameSet("tenant-001"),
			want:  []string{"tenant-001"},
		},
		{
			name:  "more skipped shards than a message can carry",
			names: nameSet(tenantShardNames(30)...),
			want:  append(tenantShardNames(maxReportedErrors), "(and 20 more)"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, reportedShardNames(tc.names))
		})
	}
}

// newSweepTestIndex builds the smallest Index a sweep can walk. The returned
// funcs are the two halves of a teardown: the cause a delete or a shutdown
// signals, and the close itself.
func newSweepTestIndex(t *testing.T, logger *logrus.Logger) (
	idx *Index, signalCloseRequested func(error), closeIndex func(),
) {
	t.Helper()
	closingCtx, closeIndex := context.WithCancel(context.Background())
	closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
	t.Cleanup(func() { signalCloseRequested(nil) })
	return &Index{
		Config:               IndexConfig{RootPath: t.TempDir(), ClassName: "Movies"},
		closingCtx:           closingCtx,
		closeRequestedCtx:    closeRequestedCtx,
		signalCloseRequested: signalCloseRequested,
		logger:               logger,
	}, signalCloseRequested, closeIndex
}

// storeUnloadableTenant adds a tenant whose every load attempt fails, so a
// sweep that decides to hydrate it says so through an error.
func storeUnloadableTenant(idx *Index, name string) {
	idx.shards.Store(name, &LazyLoadShard{
		shardOpts:  &deferredShardOpts{name: name, index: idx, class: &models.Class{Class: "Movies"}},
		memMonitor: &loadAttemptMonitor{},
	})
}

// onlySweepSummary returns the one line the index-level sweep leaves. Only that
// line carries skipped_shards, which is what tells it apart from the per-shard
// lines filed under the same operation.
func onlySweepSummary(t *testing.T, hook *test.Hook) *logrus.Entry {
	t.Helper()
	var found []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if _, ok := entry.Data["skipped_shards"]; ok {
			found = append(found, entry)
		}
	}
	require.Len(t, found, 1, "one sweep leaves exactly one summary line")
	return found[0]
}

// One sweep, one line, and that line names the outcome. A reassuring line the
// classification then contradicts is one an operator reads first and stops at.
func TestIndexCleanStalePartialReindexStateLogsOneSummaryPerSweep(t *testing.T) {
	const cleanMsg = "partial-reindex cleanup: sweep finished, unloaded shards with nothing to sweep left unloaded"

	tests := []struct {
		name string
		// staleOnDisk puts a tracker dir on every tenant, which is what makes
		// the gate hydrate rather than skip.
		staleOnDisk bool
		// requestedCause is what a delete or a shutdown signalled before the sweep.
		requestedCause error
		wantMsg        string
		wantLevel      logrus.Level
		wantSkipped    int
	}{
		{
			name:        "nothing on disk to sweep",
			wantMsg:     cleanMsg,
			wantLevel:   logrus.InfoLevel,
			wantSkipped: 2,
		},
		{
			name:        "a shard the sweep reached and could not load",
			staleOnDisk: true,
			wantMsg:     "partial-reindex cleanup: a shard could not be swept, so it is left partly swept with nothing scheduled to finish it",
			wantLevel:   logrus.ErrorLevel,
		},
		{
			name:           "a collection already being deleted",
			staleOnDisk:    true,
			requestedCause: errIndexDropped,
			wantMsg:        "partial-reindex cleanup: the collection is not on this node, so whatever is left here is removed with the collection directory, unless a backup in flight is keeping those files",
			wantLevel:      logrus.InfoLevel,
		},
		{
			name:           "a node already shutting down",
			staleOnDisk:    true,
			requestedCause: errIndexShutdown,
			wantMsg:        "partial-reindex cleanup: the sweep did not reach every shard, so what is on the ones it missed or did not finish is unverified",
			wantLevel:      logrus.WarnLevel,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			idx, signalCloseRequested, closeIndex := newSweepTestIndex(t, logger)
			defer closeIndex()

			for _, name := range []string{"tenant-a", "tenant-b"} {
				if tc.staleOnDisk {
					mkTrackerDir(t, shardPathLSM(idx.path(), name),
						"enable_filterable_title_1", "started.mig")
				}
				storeUnloadableTenant(idx, name)
			}
			if tc.requestedCause != nil {
				signalCloseRequested(tc.requestedCause)
			}

			_ = idx.cleanStalePartialReindexState(context.Background(), "title", "filterable", nil)

			summary := onlySweepSummary(t, hook)
			require.Equal(t, tc.wantMsg, summary.Message)
			require.Equal(t, tc.wantLevel, summary.Level)
			require.Equal(t, tc.wantSkipped, summary.Data["skipped_shards"],
				"a truncated sweep must not lose the numbers it did gather")
		})
	}
}

// The sweep's own line has to carry what the gate paid, whichever way the gate
// then answered. Counting only the shards it skipped reports zero reads on the
// node doing the most reading: thousands of cold tenants each holding one
// tracker dir only a payload can attribute.
func TestIndexCleanStalePartialReindexStateReportsGatePayloadReads(t *testing.T) {
	logger, hook := test.NewNullLogger()
	idx, _, closeIndex := newSweepTestIndex(t, logger)
	defer closeIndex()

	// ["cat","dog"] sorts to exactly this name, so only the payload can say
	// whether the dir belongs to the swept property.
	lsm := shardPathLSM(idx.path(), "tenant-a")
	mkTrackerDir(t, lsm, "enable_filterable_cat_dog_1", "started.mig")
	mkRecoveryPayload(t, lsm, "enable_filterable_cat_dog_1", "cat", "dog")
	storeUnloadableTenant(idx, "tenant-a")

	err := idx.cleanStalePartialReindexState(context.Background(), "cat", "filterable", nil)

	require.ErrorIs(t, err, ErrCleanupShardFailed,
		"the gate must have answered stale, or the read this pins was never paid")
	summary := onlySweepSummary(t, hook)
	require.Equal(t, 1, summary.Data["payload_reads"])
	require.Equal(t, 0, summary.Data["skipped_shards"])
}

// A ShardLike that is neither implementation is a shard the sweep reached and
// could not sweep. Reporting it as a clean walk would tell the operator every
// shard was swept while one was not touched at all.
func TestIndexCleanStalePartialReindexStateFailsOnAnUnknownShardImplementation(t *testing.T) {
	logger, hook := test.NewNullLogger()
	idx, _, closeIndex := newSweepTestIndex(t, logger)
	defer closeIndex()
	idx.shards.Store("tenant-a", NewMockShardLike(t))

	err := idx.cleanStalePartialReindexState(context.Background(), "title", "filterable", nil)

	require.ErrorIs(t, err, ErrCleanupShardFailed)
	require.Contains(t, err.Error(), "tenant-a", "the shard nothing swept has to be named")
	require.Equal(t, logrus.ErrorLevel, onlySweepSummary(t, hook).Level)
}

// A cache at its bound answers off the filesystem from then on, which nothing
// else reports.
func TestDirNamesCacheReportsRefusedListings(t *testing.T) {
	lsm := t.TempDir()
	mkTrackerDir(t, lsm, "enable_filterable_title_1", "started.mig")

	full := &dirNamesCache{cost: maxCachedDirNames}
	names, err := full.list(filepath.Join(lsm, ".migrations"))

	require.NoError(t, err)
	require.Equal(t, []string{"enable_filterable_title_1"}, names,
		"a listing the bound refuses is still answered, just not remembered")
	require.Equal(t, 1, full.refused)
	require.Empty(t, full.listings)
	require.Zero(t, (*dirNamesCache)(nil).refusedListings(),
		"a nil cache admits nothing, so it refuses nothing")
}

// A sweep whose cache stopped caching says so on its own line, at a level that
// is not "everything is fine".
func TestIndexCleanStalePartialReindexStateReportsAFullDirNamesCache(t *testing.T) {
	logger, hook := test.NewNullLogger()
	idx, _, closeIndex := newSweepTestIndex(t, logger)
	defer closeIndex()
	storeUnloadableTenant(idx, "tenant-a")

	full := &dirNamesCache{cost: maxCachedDirNames}
	require.NoError(t,
		idx.cleanStalePartialReindexState(context.Background(), "title", "filterable", full))

	summary := onlySweepSummary(t, hook)
	require.Equal(t, 1, summary.Data["uncached_listings"])
	require.Equal(t, logrus.WarnLevel, summary.Level,
		"the sweep is otherwise clean, so only the refused listing can raise this")
}

// One cache serves every sweep of a request, so a count taken off its total
// re-reports the first sweep's refusals on every later sweep — and raises them
// to Warn on a sweep that refused nothing.
func TestIndexCleanStalePartialReindexStateReportsRefusedListingsPerSweep(t *testing.T) {
	logger, hook := test.NewNullLogger()
	idx, _, closeIndex := newSweepTestIndex(t, logger)
	defer closeIndex()
	mkTrackerDir(t, shardPathLSM(idx.path(), "tenant-a"),
		"enable_filterable_title_1", "started.mig")
	storeUnloadableTenant(idx, "tenant-a")

	full := &dirNamesCache{cost: maxCachedDirNames}
	require.Error(t,
		idx.cleanStalePartialReindexState(context.Background(), "title", "filterable", full))
	first := onlySweepSummary(t, hook)
	require.Positive(t, full.refusedListings(), "the first sweep has to fill the cache")
	require.Equal(t, full.refusedListings(), first.Data["uncached_listings"],
		"every refusal so far is this sweep's")

	// The tenant left this node between the two sweeps, so the second asks the
	// cache nothing at all.
	hook.Reset()
	_, dropped := idx.shards.LoadAndDelete("tenant-a")
	require.True(t, dropped)
	require.NoError(t,
		idx.cleanStalePartialReindexState(context.Background(), "title", "filterable", full))

	second := onlySweepSummary(t, hook)
	require.Equal(t, 0, second.Data["uncached_listings"],
		"a sweep reports the listings it was refused, not the ones an earlier sweep was")
	require.Equal(t, logrus.InfoLevel, second.Level,
		"nothing about this sweep warrants raising it above its own outcome")
	require.Positive(t, full.refusedListings(),
		"the cache still carries the first sweep's refusals")
}

// Pins that a collection already gone is reported as dropped, not clean.
func TestDBCleanStalePartialReindexStateOnACollectionThatIsNotHere(t *testing.T) {
	db := &DB{indices: map[string]*Index{}}

	err := db.NewStalePartialReindexSweep()(context.Background(), "Movies", "title", "filterable")

	require.True(t, IsCleanupCollectionDropped(err),
		"the caller has nothing to act on, and no later sweep to retry")
}

// Pins that a walk which couldn't reach every shard (e.g. a shutdown
// mid-check) answers "promotable" rather than "nothing found" — false here
// would silently suppress the operator's repair guidance.
func TestIndexAnyPromotableReindexStateOnAWalkThatCouldNotLook(t *testing.T) {
	const (
		propName  = "title"
		indexType = "filterable"
		tracker   = "enable_filterable_title_1"
	)

	for _, tc := range []struct {
		name       string
		closing    bool
		closeCause error
		promotable bool
		want       bool
	}{
		{
			name:       "a walk that reached every shard and found the generation",
			promotable: true,
			want:       true,
		},
		{
			// The one answer that earns silence: the shards were read.
			name: "a walk that reached every shard and found nothing",
		},
		{
			name:       "a node shutting down over a promotable generation",
			closing:    true,
			closeCause: errIndexShutdown,
			promotable: true,
			want:       true,
		},
		{
			// Nothing on disk here either, but the walk did not establish
			// that, and the caller cannot tell the two apart.
			name:       "a node shutting down over shards holding nothing",
			closing:    true,
			closeCause: errIndexShutdown,
			want:       true,
		},
		{
			name:       "a close nobody named a cause for",
			closing:    true,
			promotable: true,
			want:       true,
		},
		{
			// The one stop that is not a gap: Index.drop renames the
			// collection's directory away, so the generation on disk here has
			// nothing left to be promoted onto.
			name:       "a collection being deleted",
			closing:    true,
			closeCause: errIndexDropped,
			promotable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := logrus.New()
			logger.SetOutput(io.Discard)
			closingCtx, closeIndex := context.WithCancel(context.Background())
			defer closeIndex()
			closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
			defer signalCloseRequested(nil)
			idx := &Index{
				Config:               IndexConfig{RootPath: t.TempDir(), ClassName: "Movies"},
				closingCtx:           closingCtx,
				closeRequestedCtx:    closeRequestedCtx,
				signalCloseRequested: signalCloseRequested,
				logger:               logger,
			}
			for _, name := range []string{"shard-a", "shard-b"} {
				if tc.promotable {
					mkTrackerDir(t, shardPathLSM(idx.path(), name), tracker,
						"started.mig", "merged.mig")
				}
				(*sync.Map)(&idx.shards).Store(name, &LazyLoadShard{
					shardOpts: &deferredShardOpts{
						name: name, index: idx, class: &models.Class{Class: "Movies"},
					},
				})
			}
			if tc.closing {
				if tc.closeCause != nil {
					signalCloseRequested(tc.closeCause)
				}
				closeIndex()
			}

			require.Equal(t, tc.want,
				idx.anyPromotableReindexState(propName, indexType, ReindexTypeChangeTokenization, nil))
		})
	}
}
