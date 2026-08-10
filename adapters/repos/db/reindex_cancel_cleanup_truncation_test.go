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
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// failToLoadMonitor makes every shard load fail, and runs onNthCall on the nth
// attempt so a cancellation or a collection delete can land in the middle of
// the walk. nthCall of 0 never runs it.
type failToLoadMonitor struct {
	mu        sync.Mutex
	onNthCall func()
	nthCall   int
	calls     int
}

func (m *failToLoadMonitor) CheckAlloc(sizeInBytes int64) error { return nil }

func (m *failToLoadMonitor) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls == m.nthCall && m.onNthCall != nil {
		m.onNthCall()
	}
	return errors.New("memory pressure")
}

func (m *failToLoadMonitor) Refresh(updateMappings bool) {}

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
		// dropOnCall deletes the collection on the nth shard load, so the
		// delete lands after that shard has already failed.
		dropOnCall int
		// closing closes the index, which is what makes the shard walk visit
		// nothing. closeCause is what the close was signalled with; nil stands
		// for a close nobody named a cause for.
		closing    bool
		closeCause error
		// indexType "filterable" is one the bucket-name mapping knows, so no
		// shard is loaded and the sweep is clean; anything else puts every
		// shard on the list.
		indexType     string
		wantErr       bool
		wantTruncated bool
		wantDropped   bool
		// wantShardErr expects the failing shard to still be named.
		wantShardErr bool
		// wantShardsNamed is how many failing shards the message may name; 0
		// skips the check.
		wantShardsNamed int
	}{
		{
			name:      "no shards is a clean sweep",
			shards:    nil,
			indexType: "an-index-type-this-build-does-not-know",
		},
		{
			name:      "one shard, nothing to clean",
			shards:    []string{"shard-a"},
			indexType: "filterable",
		},
		{
			name:          "one shard that cannot be loaded",
			shards:        []string{"shard-a"},
			indexType:     "an-index-type-this-build-does-not-know",
			wantErr:       true,
			wantShardErr:  true,
			wantTruncated: false,
		},
		{
			// A full disk fails every tenant of the node at once, and the
			// result is rendered into an operator-facing log line.
			name:            "more failing shards than a message can carry",
			shards:          tenantShardNames(15),
			indexType:       "an-index-type-this-build-does-not-know",
			wantErr:         true,
			wantShardErr:    true,
			wantShardsNamed: maxReportedErrors,
		},
		{
			name:          "the abort lands on the first of two shards",
			shards:        []string{"shard-a", "shard-b"},
			cancelOnCall:  1,
			indexType:     "an-index-type-this-build-does-not-know",
			wantErr:       true,
			wantTruncated: true,
			wantShardErr:  true,
		},
		{
			name:          "the abort lands mid-walk with three shards",
			shards:        []string{"shard-a", "shard-b", "shard-c"},
			cancelOnCall:  2,
			indexType:     "an-index-type-this-build-does-not-know",
			wantErr:       true,
			wantTruncated: true,
			wantShardErr:  true,
		},
		{
			// The walk visits nothing here, so "swept every shard" would be a
			// report about work that never happened.
			name:          "a node shutting down leaves every shard unswept",
			shards:        []string{"shard-a", "shard-b"},
			closing:       true,
			closeCause:    errIndexShutdown,
			indexType:     "an-index-type-this-build-does-not-know",
			wantErr:       true,
			wantTruncated: true,
		},
		{
			// The state this sweep removes lives under the collection's own
			// directory, so the delete removes it. Reporting truncation here
			// would send the operator after shards that are going away, and
			// promise a retry on a submit that will never come.
			name:        "a collection being deleted has nothing left to sweep",
			shards:      []string{"shard-a", "shard-b"},
			closing:     true,
			closeCause:  errIndexDropped,
			indexType:   "an-index-type-this-build-does-not-know",
			wantErr:     true,
			wantDropped: true,
		},
		{
			// The delete removes the state of every shard the sweep never
			// reached, but not of the one it already failed on: that shard's
			// files are gone only once the delete gets to them, and a caller
			// that reads this as a plain delete stops looking.
			name:         "a shard fails and the collection is deleted before the walk ends",
			shards:       []string{"shard-a", "shard-b"},
			dropOnCall:   1,
			indexType:    "an-index-type-this-build-does-not-know",
			wantErr:      true,
			wantDropped:  true,
			wantShardErr: true,
		},
		{
			// A close nobody named a cause for could still be a shutdown, and
			// the shards would then really be left behind.
			name:          "a close with no cause is treated as a shutdown",
			shards:        []string{"shard-a", "shard-b"},
			closing:       true,
			indexType:     "an-index-type-this-build-does-not-know",
			wantErr:       true,
			wantTruncated: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			monitor := &failToLoadMonitor{}

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
				(*sync.Map)(&idx.shards).Store(name, &LazyLoadShard{
					shardOpts:  &deferredShardOpts{name: name, index: idx, class: &models.Class{Class: "Movies"}},
					memMonitor: monitor,
				})
			}

			if tc.closing {
				require.NoError(t, idx.ForEachShard(func(name string, _ ShardLike) error {
					t.Errorf("a closing index walked shard %q", name)
					return nil
				}), "ForEachShard answers a closing index as a walk that reached every shard, "+
					"which is why the sweep cannot use it")
			}

			// An index type the bucket-name mapping does not know reads as
			// "cannot tell whether there is state here", which is what puts
			// every shard on the sweep's list without any on-disk fixture.
			err := idx.CleanStalePartialReindexState(ctx, "title", tc.indexType)

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
					"a collection being deleted takes its state with it, and the caller must not "+
						"report unswept shards or promise a retry for it")
			} else {
				require.NotErrorIs(t, err, ErrCleanupCollectionDropped,
					"the collection is not being deleted, so its state outlives this sweep")
			}
			if tc.wantShardErr {
				require.ErrorIs(t, err, ErrCleanupShardFailed,
					"a shard the sweep reached and could not sweep has to be tagged as one, "+
						"or a caller that asks only about the delete reports state as gone "+
						"while that shard's is still on disk")
				require.Contains(t, err.Error(), "unwrap for partial-reindex cleanup",
					"the shard that failed before the abort must still be reported")
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

// Pins forEachShardStrict against a close landing mid-walk. See its doc for
// why a whole-index drop is the only deleter that signals a cause.
func TestForEachShardStrictReportsACloseThatLandsMidWalk(t *testing.T) {
	tests := []struct {
		name string
		// closeCause is signalled from inside the walk, after the first shard.
		// nil deletes the remaining shards without signalling anything, which
		// is what every deleter other than a whole-index drop does.
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
				// What DeleteIndex and Index.drop do, in their order: the
				// cause first, then the shards leave the map one by one.
				// Every other deleter skips the first step.
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

// The names a walk did not reach go into an operator-facing message, and a node
// runs tens of thousands of tenants. The count has to survive the cap: it is
// what says how much of the collection is unaccounted for.
func TestUnvisitedShards(t *testing.T) {
	names := tenantShardNames
	visitedSet := func(names ...string) map[string]struct{} {
		out := map[string]struct{}{}
		for _, name := range names {
			out[name] = struct{}{}
		}
		return out
	}

	tests := []struct {
		name    string
		before  []string
		visited map[string]struct{}
		want    []string
	}{
		{
			name:    "a walk that reached every shard",
			before:  names(3),
			visited: visitedSet(names(3)...),
		},
		{
			name:    "a walk over an empty map",
			visited: visitedSet(),
		},
		{
			name:    "one shard skipped",
			before:  names(3),
			visited: visitedSet("tenant-000", "tenant-002"),
			want:    []string{"tenant-001"},
		},
		{
			name:    "a shard that arrived mid-walk is not one the walk skipped",
			before:  names(2),
			visited: visitedSet("tenant-000", "tenant-001", "tenant-999"),
		},
		{
			name:    "more skipped shards than a message can carry",
			before:  names(30),
			visited: visitedSet("tenant-000"),
			want:    append(names(11)[1:], "and 19 more"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, unvisitedShards(tc.before, tc.visited))
		})
	}
}
