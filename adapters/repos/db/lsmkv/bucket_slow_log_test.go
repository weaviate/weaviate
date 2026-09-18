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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestReduceSlowLogEntriesDuringAppends: reduceSlowLogEntries gets the live list
// without the lock, so -race fails if it sorts or writes its input.
func TestReduceSlowLogEntriesDuringAppends(t *testing.T) {
	ctx := helpers.InitSlowQueryDetails(context.Background())
	const (
		logKey                          = "lsm_get_by_secondary"
		appenders, appendsEach, readers = 4, 250, 2
	)

	appendsDone := make(chan struct{})
	appendWG := &sync.WaitGroup{}
	for i := 0; i < appenders; i++ {
		appendWG.Add(1)
		go func() {
			defer appendWG.Done()
			// Entries go in descending, so a reducer sorting its input moves them.
			for j := appendsEach; j > 0; j-- {
				helpers.AnnotateSlowQueryLogAppendReducible(ctx, logKey,
					BucketSlowLogEntry{Total: time.Duration(j)}, reduceSlowLogEntries)
			}
		}()
	}

	readWG := &sync.WaitGroup{}
	for i := 0; i < readers; i++ {
		readWG.Add(1)
		go func() {
			defer readWG.Done()
			for {
				select {
				case <-appendsDone:
					return
				default:
				}

				if stats, ok := helpers.ExtractSlowQueryDetails(ctx)[logKey].(BucketSlowLogEntryStats); ok {
					assert.GreaterOrEqual(t, stats.Total.Min, time.Duration(1))
					assert.LessOrEqual(t, stats.Total.Max, time.Duration(appendsEach))
				}
			}
		}()
	}

	appendWG.Wait()
	close(appendsDone)
	readWG.Wait()

	stats, ok := helpers.ExtractSlowQueryDetails(ctx)[logKey].(BucketSlowLogEntryStats)
	require.True(t, ok, "the log must carry the summary")
	require.Equal(t, time.Duration(1), stats.Total.Min)
	require.Equal(t, time.Duration(appendsEach), stats.Total.Max)
}

// TestGetBySecondaryReducesSlowQueryLogEntries: unreduced, a query fetching 25k
// objects logs 25k entries as one line megabytes wide.
func TestGetBySecondaryReducesSlowQueryLogEntries(t *testing.T) {
	ctx := context.Background()
	noopCB := cyclemanager.NewCallbackGroupNoop()

	bucket, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", testLogger(), nil, noopCB, noopCB,
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	const lookups = 3
	secondaryKey := func(i int) []byte { return []byte(fmt.Sprintf("sec-%d", i)) }

	for i := 0; i < lookups; i++ {
		require.NoError(t, bucket.Put([]byte(fmt.Sprintf("key-%d", i)), []byte("value"),
			WithSecondaryKey(0, secondaryKey(i))))
	}
	// getBySecondaryCore records timings only when a segment serves the lookup.
	require.NoError(t, bucket.FlushAndSwitch())

	tests := []struct {
		name   string
		logKey string
		lookup func(ctx context.Context, seckey []byte) error
	}{
		{
			name:   "GetBySecondaryWithBuffer",
			logKey: "lsm_get_by_secondary",
			lookup: func(ctx context.Context, seckey []byte) error {
				_, _, err := bucket.GetBySecondaryWithBuffer(ctx, 0, seckey, nil)
				return err
			},
		},
		{
			name:   "GetBySecondaryWithBufferAndView",
			logKey: "lsm_get_by_secondary_with_view",
			lookup: func(ctx context.Context, seckey []byte) error {
				view := bucket.GetConsistentView()
				defer view.ReleaseView()
				_, _, err := bucket.GetBySecondaryWithBufferAndView(ctx, 0, seckey, nil, view)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queryCtx := helpers.InitSlowQueryDetails(ctx)
			for i := 0; i < lookups; i++ {
				require.NoError(t, test.lookup(queryCtx, secondaryKey(i)))
			}

			logged, ok := helpers.ExtractSlowQueryDetails(queryCtx)[test.logKey]
			require.True(t, ok, "the lookups must record timings under %q", test.logKey)

			stats, ok := logged.(BucketSlowLogEntryStats)
			require.True(t, ok, "the log must carry the summary, not one entry per lookup, got %T", logged)
			require.Positive(t, stats.Total.Max, "the summary must reduce the recorded timings")
		})
	}
}
