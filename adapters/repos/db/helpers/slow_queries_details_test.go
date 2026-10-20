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

func TestReduceToSlowest(t *testing.T) {
	entries := func(tooks ...time.Duration) []map[string]any {
		out := make([]map[string]any, len(tooks))
		for i, took := range tooks {
			out[i] = map[string]any{"took": took}
		}
		return out
	}
	tests := []struct {
		name        string
		in          []map[string]any
		wantSlowest []map[string]any
	}{
		{"empty", nil, nil},
		{"fewer than the bound keeps all, slowest first", entries(1, 3, 2), entries(3, 2, 1)},
		{"more than the bound keeps the slowest", entries(4, 9, 1, 7, 2, 8, 3), entries(9, 8, 7, 4, 3)},
		{
			"an entry without took sorts last",
			[]map[string]any{{"x": 1}, {"took": time.Duration(1)}},
			[]map[string]any{{"took": time.Duration(1)}, {"x": 1}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := slices.Clone(tc.in)
			got := ReduceToSlowest(in)
			require.Equal(t, len(tc.in), got.Count)
			require.Equal(t, tc.wantSlowest, got.Slowest)
			require.Equal(t, tc.in, in, "the reducer must not reorder its input")
		})
	}
}

func TestHasSlowQueryDetails(t *testing.T) {
	var noCtx context.Context
	require.False(t, HasSlowQueryDetails(noCtx))
	require.False(t, HasSlowQueryDetails(context.Background()))
	require.True(t, HasSlowQueryDetails(InitSlowQueryDetails(context.Background())))
}
