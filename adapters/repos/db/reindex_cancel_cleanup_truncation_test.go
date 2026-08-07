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

// The sweep runs with the collection's backup and restore gate closed, and the
// caller logs what it got. A sweep that stopped part-way through left the
// shards after that point untouched — if the only thing it reports is the
// shard that failed before it, the caller reads a bounded failure where the
// truth is "unknown, from here on". A sweep that visited nothing at all,
// because the collection is closing, is the same answer in its worst form.
func TestCleanStalePartialReindexStateReportsATruncatedSweep(t *testing.T) {
	tests := []struct {
		name   string
		shards []string
		// cancelOnCall aborts the sweep's context on the nth shard load; 0
		// lets the walk run to the end.
		cancelOnCall int
		// closing makes the collection closing or being dropped, which is what
		// makes the shard walk visit nothing.
		closing bool
		// indexType "" means one this build knows, so no shard is loaded and
		// the sweep is clean.
		indexType     string
		wantErr       bool
		wantTruncated bool
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
			name:          "a closing collection is swept not at all",
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
			if tc.closing {
				closeIndex()
			}
			idx := &Index{
				Config:     IndexConfig{RootPath: t.TempDir(), ClassName: "Movies"},
				closingCtx: closingCtx,
				logger:     logger,
			}
			for _, name := range tc.shards {
				(*sync.Map)(&idx.shards).Store(name, &LazyLoadShard{
					shardOpts:  &deferredShardOpts{name: name, index: idx, class: &models.Class{Class: "Movies"}},
					memMonitor: monitor,
				})
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
					"every shard was visited, so the failure is bounded to the ones that failed")
			}
			if tc.wantShardErr {
				require.Contains(t, err.Error(), "unwrap for partial-reindex cleanup",
					"the shard that failed before the abort must still be reported")
			}
		})
	}
}
