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
	"io"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// failToLoadMonitor makes every shard load fail, and cancels the sweep's
// context on the nth attempt so the walk aborts on the shard after it.
// cancelOnCall of 0 never cancels.
type failToLoadMonitor struct {
	mu           sync.Mutex
	cancel       context.CancelFunc
	cancelOnCall int
	calls        int
}

func (m *failToLoadMonitor) CheckAlloc(sizeInBytes int64) error { return nil }

func (m *failToLoadMonitor) CheckMappingAndReserve(numberMappings int64, reservationTimeInS int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls == m.cancelOnCall && m.cancel != nil {
		m.cancel()
	}
	return errors.New("memory pressure")
}

func (m *failToLoadMonitor) Refresh(updateMappings bool) {}

// The caller logs what the sweep gives it. A sweep that stopped part-way
// through left the shards after that point untouched — if the only thing it
// reports is the shard that failed before it, the caller reads a bounded
// failure where the truth is "unknown, from here on". A sweep that visited nothing at all,
// because the node is shutting down, is the same answer in its worst form. A
// collection being deleted is the one close that is neither: its state is
// deleted with it, so there is nothing left for any sweep to remove.
func TestCleanStalePartialReindexStateReportsATruncatedSweep(t *testing.T) {
	tests := []struct {
		name   string
		shards []string
		// cancelOnCall aborts the sweep's context on the nth shard load; 0
		// lets the walk run to the end.
		cancelOnCall int
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

			monitor := &failToLoadMonitor{cancel: cancel, cancelOnCall: tc.cancelOnCall}

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
			if tc.closing {
				// The real teardowns signal the cause first and cancel
				// closingCtx once they hold the lock.
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
				require.Contains(t, err.Error(), "unwrap for partial-reindex cleanup",
					"the shard that failed before the abort must still be reported")
			}
		})
	}
}

// A close that lands mid-walk is the same false clean as one that lands before
// it. Index.drop signals its cause, then deletes each shard from the map as it
// goes — and a sync.Map range may skip entries deleted while it runs, so the
// walk can end early with every remaining shard unswept and nothing to report.
func TestForEachShardStrictReportsACloseThatLandsMidWalk(t *testing.T) {
	tests := []struct {
		name string
		// closeCause is signalled from inside the walk, after the first shard.
		// nil leaves the index open for the whole walk.
		closeCause error
		wantErr    error
	}{
		{
			name: "a walk nothing interrupted is clean",
		},
		{
			name:       "a collection deleted mid-walk is not a swept collection",
			closeCause: errIndexDropped,
			wantErr:    errIndexDropped,
		},
		{
			name:       "a node shutting down mid-walk is not a swept collection",
			closeCause: errIndexShutdown,
			wantErr:    errIndexShutdown,
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
				if visited > 1 || tc.closeCause == nil {
					return nil
				}
				// What DeleteIndex and Index.drop do, in their order: the
				// cause first, then the shards leave the map one by one.
				signalCloseRequested(tc.closeCause)
				closeIndex()
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

// Every shard walk now asks closeCause first, and the two contexts it reads are
// set by the Index constructor rather than by the zero value. Calling Err or
// Cause on a nil context panics, so an Index assembled without them — a test
// double, or any future construction path that skips one — would take down the
// walk instead of answering it.
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
