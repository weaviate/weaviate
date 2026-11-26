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

package helpers

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlowQueryDetailsJourney(t *testing.T) {
	ctx := InitSlowQueryDetails(context.Background())

	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			AnnotateSlowQueryLog(ctx, fmt.Sprintf("key_%d", i), fmt.Sprintf("value_%d", i))
		}()
	}

	wg.Wait()

	details := ExtractSlowQueryDetails(ctx)
	require.Len(t, details, 100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		assert.Equal(t, value, details[key])
	}
}

func TestAnnotateSlowQueryLogAppendFunc(t *testing.T) {
	t.Run("built values append into one list with eager values", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		calls := 0
		AnnotateSlowQueryLogAppendFunc(ctx, "k", func() string {
			calls++
			return "lazy"
		})
		AnnotateSlowQueryLogAppend(ctx, "k", "eager")
		require.Equal(t, 1, calls, "build must run exactly once")
		require.Equal(t, []string{"lazy", "eager"}, ExtractSlowQueryDetails(ctx)["k"])
	})

	t.Run("nil build is tolerated even with details present", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLogAppendFunc[string](ctx, "k", nil)
		require.NotContains(t, ExtractSlowQueryDetails(ctx), "k")
	})

	t.Run("build is skipped when ctx carries no details", func(t *testing.T) {
		calls := 0
		build := func() string {
			calls++
			return "lazy"
		}
		AnnotateSlowQueryLogAppendFunc(context.Background(), "k", build)
		AnnotateSlowQueryLogAppendFunc(nil, "k", build) //nolint:staticcheck // pins the nil-ctx guard
		require.Zero(t, calls)
	})

	t.Run("skip path allocates nothing", func(t *testing.T) {
		ctx := context.Background()
		reason := "declined"
		allocs := testing.AllocsPerRun(100, func() {
			AnnotateSlowQueryLogAppendFunc(ctx, "k", func() map[string]any {
				return map[string]any{"reason": reason}
			})
		})
		require.Zero(t, allocs, "guards must bail before build; closure must not escape")
	})
}

func TestAnnotateSlowQueryLogAppendReducible(t *testing.T) {
	sum := func(values []int) int {
		total := 0
		for _, v := range values {
			total += v
		}
		return total
	}

	t.Run("a second extract reduces the same entries again", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLogAppendReducible(ctx, "k", 7, sum)
		require.Equal(t, 7, ExtractSlowQueryDetails(ctx)["k"])
		AnnotateSlowQueryLogAppendReducible(ctx, "k", 5, sum)
		require.Equal(t, 12, ExtractSlowQueryDetails(ctx)["k"],
			"a second extract must return the same summary, because a query can both log and report a profile")
	})

	t.Run("skip path allocates nothing", func(t *testing.T) {
		ctx := context.Background()
		allocs := testing.AllocsPerRun(100, func() {
			AnnotateSlowQueryLogAppendReducible(ctx, "k", 1, sum)
		})
		require.Zero(t, allocs, "a query the gate closed must pay nothing per lookup")
	})

	t.Run("nil reduce is tolerated", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLogAppendReducible[int, int](ctx, "k", 1, nil)
		require.NotContains(t, ExtractSlowQueryDetails(ctx), "k")
	})
}

func TestAnnotateSlowQueryLogAppendManyReducible(t *testing.T) {
	const key = "entries"
	sum := func(values []int) int {
		total := 0
		for _, v := range values {
			total += v
		}
		return total
	}
	count := func(values []int) int { return len(values) }

	t.Run("a nil context is ignored", func(t *testing.T) {
		// A typed nil variable rather than a nil literal: staticcheck's SA1012
		// rejects the literal, and the callers these guards exist for arrive with
		// an unchecked context, which is the same input.
		var noCtx context.Context
		require.NotPanics(t, func() { AnnotateSlowQueryLogAppendManyReducible(noCtx, key, []int{1}, sum) })
	})

	t.Run("a context without details is ignored", func(t *testing.T) {
		ctx := context.Background()
		AnnotateSlowQueryLogAppendManyReducible(ctx, key, []int{1}, sum)
		require.Nil(t, ExtractSlowQueryDetails(ctx))
	})

	t.Run("no values writes no key", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLogAppendManyReducible(ctx, key, []int{}, sum)
		AnnotateSlowQueryLogAppendManyReducible(ctx, key, nil, sum)
		require.NotContains(t, ExtractSlowQueryDetails(ctx), key)
	})

	t.Run("nil reduce is tolerated", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLogAppendManyReducible[int, int](ctx, key, []int{1}, nil)
		require.NotContains(t, ExtractSlowQueryDetails(ctx), key)
	})

	t.Run("values join the ones appended one at a time and are reduced together", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLogAppendReducible(ctx, key, 1, sum)
		AnnotateSlowQueryLogAppendManyReducible(ctx, key, []int{2, 3}, sum)
		AnnotateSlowQueryLogAppendManyReducible(ctx, key, []int{4}, sum)
		require.Equal(t, 10, ExtractSlowQueryDetails(ctx)[key])
	})

	t.Run("a key holding another type is left alone", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLog(ctx, key, "set directly")
		AnnotateSlowQueryLogAppendManyReducible(ctx, key, []int{1, 2}, sum)
		require.Equal(t, "set directly", ExtractSlowQueryDetails(ctx)[key])
	})

	t.Run("concurrent appends all arrive", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		wg := &sync.WaitGroup{}
		for range 50 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				AnnotateSlowQueryLogAppendManyReducible(ctx, key, []int{1, 1}, count)
			}()
		}
		wg.Wait()
		require.Equal(t, 100, ExtractSlowQueryDetails(ctx)[key])
	})

	t.Run("skip path allocates nothing", func(t *testing.T) {
		ctx := context.Background()
		values := []int{1, 2}
		allocs := testing.AllocsPerRun(100, func() {
			AnnotateSlowQueryLogAppendManyReducible(ctx, key, values, sum)
		})
		require.Zero(t, allocs, "a query the gate closed must pay nothing per chunk")
	})
}

const slowestTestKey = "slowest"

func TestSlowestTracker(t *testing.T) {
	const ms = time.Millisecond
	tests := []struct {
		name       string
		tooks      []time.Duration
		wantIDs    []int
		wantBuilds int
	}{
		{"fewer than the bound", []time.Duration{1 * ms, 3 * ms, 2 * ms}, []int{1, 2, 0}, 3},
		{
			"ascending replaces the last kept and raises the floor",
			[]time.Duration{1 * ms, 2 * ms, 3 * ms, 4 * ms, 5 * ms, 6 * ms, 7 * ms, 3 * ms},
			[]int{6, 5, 4, 3, 2},
			7,
		},
		{
			"descending builds only the first five",
			[]time.Duration{7 * ms, 6 * ms, 5 * ms, 4 * ms, 3 * ms, 2 * ms, 1 * ms},
			[]int{0, 1, 2, 3, 4},
			5,
		},
		{
			"mixed",
			[]time.Duration{4 * ms, 9 * ms, 1 * ms, 7 * ms, 2 * ms, 8 * ms, 3 * ms},
			[]int{1, 5, 3, 0, 6},
			7,
		},
		{
			"all tied keep arrival order",
			[]time.Duration{7 * ms, 7 * ms, 7 * ms, 7 * ms, 7 * ms, 7 * ms, 7 * ms},
			[]int{0, 1, 2, 3, 4},
			5,
		},
		{
			"ties at the floor are rejected, ties above it are not",
			[]time.Duration{5 * ms, 5 * ms, 5 * ms, 5 * ms, 5 * ms, 5 * ms, 6 * ms, 5 * ms, 6 * ms},
			[]int{6, 8, 0, 1, 2},
			7,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := InitSlowQueryDetails(context.Background())
			tracker := slowQueryDetailsFromContext(ctx).trackerFor(slowestTestKey)
			builds := 0
			floor := int64(0)
			for id, took := range tc.tooks {
				tracker.observe(took, func() map[string]any {
					builds++
					return map[string]any{"id": id}
				})
				require.GreaterOrEqual(t, tracker.floor.Load(), floor, "the floor must only rise")
				floor = tracker.floor.Load()
			}

			want := make([]map[string]any, len(tc.wantIDs))
			for i, id := range tc.wantIDs {
				took := tc.tooks[id]
				want[i] = map[string]any{"id": id, "took": took, "took_string": took.String()}
			}
			require.Equal(t, SlowestEntries{Count: len(tc.tooks), Slowest: want},
				ExtractSlowQueryDetails(ctx)[slowestTestKey])
			require.Equal(t, tc.wantBuilds, builds)
		})
	}
}

func TestSlowestTracker_Concurrent(t *testing.T) {
	const workers, leavesPerWorker = 16, 500
	ctx := InitSlowQueryDetails(context.Background())
	tracker := slowQueryDetailsFromContext(ctx).trackerFor(slowestTestKey)

	extractorDone := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(extractorDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			summary, ok := ExtractSlowQueryDetails(ctx)[slowestTestKey].(SlowestEntries)
			if ok {
				assert.True(t, slices.IsSortedFunc(summary.Slowest, func(a, b map[string]any) int {
					return int(entryTook(b) - entryTook(a))
				}), "a concurrent extract must see a sorted list")
			}
		}
	}()

	wg := &sync.WaitGroup{}
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for leaf := range leavesPerWorker {
				took := time.Duration(worker + leaf*workers + 1)
				tracker.observe(took, func() map[string]any { return map[string]any{} })
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-extractorDone

	summary := ExtractSlowQueryDetails(ctx)[slowestTestKey].(SlowestEntries)
	require.Equal(t, workers*leavesPerWorker, summary.Count)
	slowest := time.Duration(workers * leavesPerWorker)
	require.Len(t, summary.Slowest, slowestEntriesKept)
	for i, entry := range summary.Slowest {
		require.Equal(t, slowest-time.Duration(i), entry["took"])
	}
}

func TestExtractSlowQueryDetails_Slowest(t *testing.T) {
	t.Run("no leaf observed omits the key", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		require.NotContains(t, ExtractSlowQueryDetails(ctx), slowestTestKey)
	})

	t.Run("extracting leaves the tracker intact and detaches the result", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		tracker := slowQueryDetailsFromContext(ctx).trackerFor(slowestTestKey)
		for took := 1; took <= slowestEntriesKept; took++ {
			tracker.observe(time.Duration(took), func() map[string]any { return map[string]any{} })
		}

		first := ExtractSlowQueryDetails(ctx)[slowestTestKey].(SlowestEntries)
		require.Equal(t, first, ExtractSlowQueryDetails(ctx)[slowestTestKey], "a second extract must see the same summary")

		kept := slices.Clone(first.Slowest)
		tracker.observe(time.Hour, func() map[string]any { return map[string]any{} })
		require.Equal(t, kept, first.Slowest, "a later leaf must not rewrite a summary already handed out")

		third := ExtractSlowQueryDetails(ctx)[slowestTestKey].(SlowestEntries)
		require.Equal(t, slowestEntriesKept+1, third.Count)
		require.Equal(t, time.Hour, third.Slowest[0]["took"])
	})
}

func TestAnnotateSlowQueryLogSlowest(t *testing.T) {
	reason := "declined"
	build := func() map[string]any { return map[string]any{"reason": reason} }

	withFullTracker := func() (context.Context, *slowestTracker) {
		ctx := InitSlowQueryDetails(context.Background())
		tracker := slowQueryDetailsFromContext(ctx).trackerFor(slowestTestKey)
		for range slowestEntriesKept {
			tracker.observe(time.Hour, build)
		}
		return ctx, tracker
	}

	t.Run("a measured leaf is kept with its took", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		start := time.Now().Add(-time.Minute)
		AnnotateSlowQueryLogSlowest(ctx, slowestTestKey, start, build)

		summary := ExtractSlowQueryDetails(ctx)[slowestTestKey].(SlowestEntries)
		require.Equal(t, 1, summary.Count)
		require.Equal(t, reason, summary.Slowest[0]["reason"])
		require.GreaterOrEqual(t, summary.Slowest[0]["took"], time.Minute)
	})

	t.Run("a rejected leaf is counted and allocates nothing", func(t *testing.T) {
		ctx, tracker := withFullTracker()
		start := time.Now()
		const runs = 100
		allocs := testing.AllocsPerRun(runs, func() { AnnotateSlowQueryLogSlowest(ctx, slowestTestKey, start, build) })
		require.Zero(t, allocs, "a leaf at or below the floor must return before building anything")
		// AllocsPerRun adds one warm-up call.
		require.EqualValues(t, slowestEntriesKept+runs+1, tracker.count.Load())
	})

	t.Run("a rejected leaf takes neither lock", func(t *testing.T) {
		ctx, tracker := withFullTracker()
		tracker.mu.Lock()
		t.Cleanup(tracker.mu.Unlock)
		details := slowQueryDetailsFromContext(ctx)
		details.Lock()
		t.Cleanup(details.Unlock)

		done := make(chan struct{})
		go func() {
			defer close(done)
			AnnotateSlowQueryLogSlowest(ctx, slowestTestKey, time.Now(), build)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("a leaf at or below the floor must not wait on the tracker lock")
		}
	})

	t.Run("a leaf overtaken by the floor after its check is dropped", func(t *testing.T) {
		_, tracker := withFullTracker()
		before := slices.Clone(tracker.kept)

		tracker.mu.Lock()
		tracker.keepLocked(map[string]any{"took": time.Minute})
		tracker.mu.Unlock()

		require.Equal(t, before, tracker.kept)
	})

	t.Run("an admitted leaf builds outside the lock", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		tracker := slowQueryDetailsFromContext(ctx).trackerFor(slowestTestKey)
		tracker.mu.Lock()
		t.Cleanup(tracker.mu.Unlock)

		built := make(chan struct{})
		go AnnotateSlowQueryLogSlowest(ctx, slowestTestKey, time.Now(), func() map[string]any {
			close(built)
			return map[string]any{}
		})
		select {
		case <-built:
		case <-time.After(5 * time.Second):
			t.Fatal("build must not run under the tracker lock")
		}
	})

	t.Run("each key keeps its own entries", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		for _, key := range []string{"a", "b", "a"} {
			AnnotateSlowQueryLogSlowest(ctx, key, time.Now(), build)
		}

		details := ExtractSlowQueryDetails(ctx)
		require.Equal(t, 2, details["a"].(SlowestEntries).Count)
		require.Equal(t, 1, details["b"].(SlowestEntries).Count)
	})

	t.Run("concurrent first use of a key counts every leaf", func(t *testing.T) {
		const rounds, workers = 200, 16
		for range rounds {
			ctx := InitSlowQueryDetails(context.Background())
			release := make(chan struct{})
			wg := &sync.WaitGroup{}
			for range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-release
					AnnotateSlowQueryLogSlowest(ctx, slowestTestKey, time.Now().Add(-time.Hour), build)
				}()
			}
			close(release)
			wg.Wait()
			require.Equal(t, workers, ExtractSlowQueryDetails(ctx)[slowestTestKey].(SlowestEntries).Count)
		}
	})

	t.Run("no details cost nothing", func(t *testing.T) {
		var noCtx context.Context
		start := time.Now()
		for _, ctx := range []context.Context{noCtx, context.Background()} {
			built := false
			allocs := testing.AllocsPerRun(100, func() {
				AnnotateSlowQueryLogSlowest(ctx, slowestTestKey, start, func() map[string]any {
					built = true
					return nil
				})
			})
			require.Zero(t, allocs)
			require.False(t, built, "build must not run without details")
		}
	})
}

func TestHasSlowQueryDetails(t *testing.T) {
	var noCtx context.Context
	require.False(t, HasSlowQueryDetails(noCtx))
	require.False(t, HasSlowQueryDetails(context.Background()))
	require.True(t, HasSlowQueryDetails(InitSlowQueryDetails(context.Background())))
}
