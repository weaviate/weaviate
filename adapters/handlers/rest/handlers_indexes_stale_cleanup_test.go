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

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// fakeStaleCleaner records the index types it was asked to scrub and returns a
// canned error.
type fakeStaleCleaner struct {
	calls []string
	err   error
}

func (f *fakeStaleCleaner) NewStalePartialReindexSweep() db.StalePartialReindexSweep {
	return func(_ context.Context, _, _, indexType string) error {
		f.calls = append(f.calls, indexType)
		return f.err
	}
}

// fakeDrainSealer answers per task: never drains the ones in stuck, and names
// what it sealed and released so a test can tell a seal taken for the right
// task from one taken for the wrong one.
type fakeDrainSealer struct {
	stuck    map[string]bool
	sealed   []string
	released int
	// sweptWhileSealed is what the cleaner had scrubbed at each release, so a
	// seal dropped before the sweep is distinguishable from one held across it.
	sweptWhileSealed []int
	cleaner          *fakeStaleCleaner
}

func (f *fakeDrainSealer) SealLocalTaskDrain(_ context.Context, desc distributedtask.TaskDescriptor) (func(), error) {
	if f.stuck[desc.ID] {
		// What the real drain reports when a worker outlives its timeout.
		return nil, context.DeadlineExceeded
	}
	f.sealed = append(f.sealed, desc.ID)
	return func() {
		f.released++
		f.sweptWhileSealed = append(f.sweptWhileSealed, len(f.cleaner.calls))
	}, nil
}

func reindexTaskOn(t *testing.T, id, collection, property string) *distributedtask.Task {
	t.Helper()
	payload, err := json.Marshal(db.ReindexTaskPayload{
		Collection: collection,
		Properties: []string{property},
	})
	require.NoError(t, err)
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Payload:        payload,
	}
}

// TestCleanStalePartialStateOrFail pins that the pre-submit stale-state scrub
// fails closed on an unknown migration type or a scrub error, holds every
// local worker of an earlier task on the property while it runs, and otherwise
// cleans every index type the migration touches before proceeding.
func TestCleanStalePartialStateOrFail(t *testing.T) {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	h := &indexesHandlers{appState: &state.State{Logger: logger}}

	tests := []struct {
		name       string
		sweepErr   error
		mtype      db.ReindexMigrationType
		tasks      func(t *testing.T) []*distributedtask.Task
		stuck      map[string]bool
		wantCode   int // 0 means the submit proceeds
		wantCalls  []string
		wantSealed []string
		because    string
	}{
		{
			name:     "unknown migration type",
			mtype:    db.ReindexMigrationType("not-a-real-type"),
			wantCode: http.StatusInternalServerError,
			because:  "an unknown migration type must fail closed, not silently skip cleanup",
		},
		{
			name:      "the scrub itself fails",
			mtype:     db.ReindexTypeEnableFilterable,
			sweepErr:  errors.New("disk unavailable"),
			wantCode:  http.StatusInternalServerError,
			wantCalls: []string{"filterable"},
		},
		{
			name:      "a truncated sweep proceeds instead of refusing the submit",
			mtype:     db.ReindexTypeChangeTokenization,
			sweepErr:  fmt.Errorf("%w: shards skipped mid-walk: tenant-a", db.ErrCleanupSweepTruncated),
			wantCalls: []string{"searchable", "filterable"},
			because:   "unvisited shards are unverified rather than known stale, and a truncation on the first index type must not stop the second",
		},
		{
			name:      "change-tokenization scrubs both inverted index dirs",
			mtype:     db.ReindexTypeChangeTokenization,
			wantCalls: []string{"searchable", "filterable"},
		},
		{
			// The sweep shuts these buckets down and removes their
			// directories. A task goes terminal cluster-wide without waiting
			// for the local unit, and the cancel handler hands this sweep
			// exactly the case where its own drain timed out.
			name:  "an earlier task on this property is held for the sweep",
			mtype: db.ReindexTypeEnableFilterable,
			tasks: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					reindexTaskOn(t, "Books:enable-filterable:price:ab12", "Books", "price"),
					reindexTaskOn(t, "Books:enable-filterable:title:cd34", "Books", "title"),
					reindexTaskOn(t, "Authors:enable-filterable:price:ef56", "Authors", "price"),
				}
			},
			wantCalls:  []string{"filterable"},
			wantSealed: []string{"Books:enable-filterable:price:ab12"},
			because:    "only a task on this collection and property can be writing into the directories the sweep removes",
		},
		{
			name:  "a local worker that will not exit refuses the submit",
			mtype: db.ReindexTypeEnableFilterable,
			tasks: func(t *testing.T) []*distributedtask.Task {
				return []*distributedtask.Task{
					reindexTaskOn(t, "Books:enable-filterable:price:ab12", "Books", "price"),
				}
			},
			stuck:    map[string]bool{"Books:enable-filterable:price:ab12": true},
			wantCode: http.StatusServiceUnavailable,
			because:  "sweeping under a live worker takes the directories out from under writes it already acknowledged",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleaner := &fakeStaleCleaner{err: tt.sweepErr}
			sealer := &fakeDrainSealer{stuck: tt.stuck, cleaner: cleaner}
			var tasks []*distributedtask.Task
			if tt.tasks != nil {
				tasks = tt.tasks(t)
			}

			resp := h.cleanStalePartialStateOrFail(context.Background(), nil, cleaner, sealer,
				"Books", "price", tt.mtype, tasks)

			if tt.wantCode == 0 {
				require.Nil(t, resp, tt.because)
			} else {
				code, _ := statusOf(t, resp)
				require.Equal(t, tt.wantCode, code, tt.because)
			}
			require.Equal(t, tt.wantCalls, cleaner.calls, tt.because)
			require.Equal(t, tt.wantSealed, sealer.sealed)
			require.Equal(t, len(sealer.sealed), sealer.released,
				"a leaked seal refuses this task for the life of the process")
			for _, swept := range sealer.sweptWhileSealed {
				require.Equal(t, len(cleaner.calls), swept,
					"the seal is held until the sweep is done, not just until it starts")
			}
		})
	}
}
