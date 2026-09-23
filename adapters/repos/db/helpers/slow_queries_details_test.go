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
	"sync"
	"testing"

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
