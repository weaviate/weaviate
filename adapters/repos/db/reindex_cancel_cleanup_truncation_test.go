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

// ForEachShard is documented as safe on an index that is dropping or shutting
// down, and about ten test builders in this package construct an Index with no
// closeRequestedCtx at all. Asking such an index why it is closing has to
// answer, and a shutdown is the answer that leaves the shards accounted for.
func TestCloseCauseAnswersAnIndexBuiltWithoutOne(t *testing.T) {
	tests := []struct {
		name string
		// wired builds the index with a closeRequestedCtx, as production does.
		wired bool
		// cause is signalled on that ctx before the close.
		cause     error
		closing   bool
		wantCause error
	}{
		{
			name:  "an open index is not closing, wired or not",
			wired: true,
		},
		{
			name: "an open index with no closeRequestedCtx is not closing either",
		},
		{
			name:      "a wired index reports the cause it was closed with",
			wired:     true,
			cause:     errIndexDropped,
			closing:   true,
			wantCause: errIndexDropped,
		},
		{
			name:      "a wired index closed with no cause reads as closed",
			wired:     true,
			closing:   true,
			wantCause: errIndexClosed,
		},
		{
			name:      "an index with no closeRequestedCtx reads as closed",
			closing:   true,
			wantCause: errIndexClosed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := logrus.New()
			logger.SetOutput(io.Discard)
			closingCtx, closeIndex := context.WithCancel(context.Background())
			defer closeIndex()

			idx := &Index{
				Config:     IndexConfig{RootPath: t.TempDir(), ClassName: "Movies"},
				closingCtx: closingCtx,
				logger:     logger,
			}
			if tc.wired {
				closeRequestedCtx, signalCloseRequested := context.WithCancelCause(context.Background())
				defer signalCloseRequested(nil)
				idx.closeRequestedCtx = closeRequestedCtx
				idx.signalCloseRequested = signalCloseRequested
				if tc.cause != nil {
					signalCloseRequested(tc.cause)
				}
			}
			if tc.closing {
				closeIndex()
			}

			var walked int
			(*sync.Map)(&idx.shards).Store("shard-a", &LazyLoadShard{
				shardOpts: &deferredShardOpts{name: "shard-a", index: idx, class: &models.Class{Class: "Movies"}},
			})

			require.NoError(t, idx.ForEachShard(func(string, ShardLike) error {
				walked++
				return nil
			}), "ForEachShard absorbs a closing index rather than reporting it")

			if tc.wantCause == nil {
				require.NoError(t, idx.closeCause())
				require.Equal(t, 1, walked, "an open index still walks its shards")
				return
			}
			require.ErrorIs(t, idx.closeCause(), tc.wantCause)
			require.Zero(t, walked, "a closing index walks nothing")
			require.ErrorIs(t, idx.forEachShardStrict(func(string, ShardLike) error { return nil }),
				tc.wantCause, "the strict walk reports what ForEachShard swallows")
		})
	}
}

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

// The sweep runs with the collection's backup and restore gate closed, and the
// caller logs what it got. A sweep that stopped part-way through left the
// shards after that point untouched — if the only thing it reports is the
// shard that failed before it, the caller reads a bounded failure where the
// truth is "unknown, from here on". A sweep that visited nothing at all,
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
		// cancelAtEntry aborts the sweep's context before the call, so the
		// walk stops on the first shard it looks at rather than mid-way.
		cancelAtEntry bool
		// closing closes the index, which is what makes the shard walk visit
		// nothing. closeCause is what the close was signalled with; nil stands
		// for a close nobody named a cause for.
		closing    bool
		closeCause error
		// indexType "filterable" is one the bucket-name mapping knows, so no
		// shard is loaded.
		indexType     string
		wantErr       bool
		wantTruncated bool
		wantDropped   bool
		// wantShardErr expects the failing shard to still be named.
		wantShardErr bool
		// wantShardsNamed expects each of these shards to be named in the
		// error, so a sweep that gave up after the first failure is caught.
		wantShardsNamed []string
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
			// Nothing failed, because nothing was tried. The caller must not
			// read the absence of a shard name as "no shard had a problem".
			name:          "a cancel that arrives before the first shard names no shard",
			shards:        []string{"shard-a", "shard-b"},
			cancelAtEntry: true,
			indexType:     "an-index-type-this-build-does-not-know",
			wantErr:       true,
			wantTruncated: true,
			wantShardErr:  false,
		},
		{
			// The same cancel over shards this sweep had no work on. The
			// shards are still unswept, and a later sweep is still the only
			// thing that can say so.
			name:          "a cancel before the first shard is truncation even with nothing to clean",
			shards:        []string{"shard-a", "shard-b"},
			cancelAtEntry: true,
			indexType:     "filterable",
			wantErr:       true,
			wantTruncated: true,
			wantShardErr:  false,
		},
		{
			// Every shard failed, and every one of them was still visited.
			// The caller has a complete answer, so it must not also be told
			// that something was left unreached.
			name:            "every shard fails and the sweep still reaches the end",
			shards:          []string{"shard-a", "shard-b"},
			indexType:       "an-index-type-this-build-does-not-know",
			wantErr:         true,
			wantTruncated:   false,
			wantShardErr:    true,
			wantShardsNamed: []string{"shard-a", "shard-b"},
		},
		{
			// The abort lands on the last shard, so there is no shard after
			// it to leave unswept.
			name:            "a cancel on the last shard leaves nothing unvisited",
			shards:          []string{"shard-a", "shard-b"},
			cancelOnCall:    2,
			indexType:       "an-index-type-this-build-does-not-know",
			wantErr:         true,
			wantTruncated:   false,
			wantShardErr:    true,
			wantShardsNamed: []string{"shard-a", "shard-b"},
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

			if tc.cancelAtEntry {
				cancel()
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
			} else {
				require.NotContains(t, err.Error(), "unwrap for partial-reindex cleanup",
					"no shard was reached, so naming one would blame a shard that was never tried")
			}
			for _, name := range tc.wantShardsNamed {
				require.Contains(t, err.Error(), name,
					"one shard failing must not stop the sweep from reporting the others")
			}
		})
	}
}
