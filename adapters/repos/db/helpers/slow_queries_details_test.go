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

func TestAnnotateSlowQueryLogAppendMany(t *testing.T) {
	const key = "entries"

	t.Run("a nil context is ignored", func(t *testing.T) {
		// A typed nil variable rather than a nil literal: staticcheck's SA1012
		// rejects the literal, and the callers these guards exist for arrive with
		// an unchecked context, which is the same input.
		var noCtx context.Context
		require.NotPanics(t, func() { AnnotateSlowQueryLogAppendMany(noCtx, key, []int{1}) })
	})

	t.Run("a context without details is ignored", func(t *testing.T) {
		ctx := context.Background()
		AnnotateSlowQueryLogAppendMany(ctx, key, []int{1})
		require.Nil(t, ExtractSlowQueryDetails(ctx))
	})

	t.Run("no values writes no key", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLogAppendMany(ctx, key, []int{})
		AnnotateSlowQueryLogAppendMany[int](ctx, key, nil)
		require.NotContains(t, ExtractSlowQueryDetails(ctx), key)
	})

	t.Run("values land under a fresh key", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLogAppendMany(ctx, key, []int{1, 2})
		require.Equal(t, []int{1, 2}, ExtractSlowQueryDetails(ctx)[key])
	})

	t.Run("values append to what is already there", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLogAppend(ctx, key, 1)
		AnnotateSlowQueryLogAppendMany(ctx, key, []int{2, 3})
		AnnotateSlowQueryLogAppendMany(ctx, key, []int{4})
		require.Equal(t, []int{1, 2, 3, 4}, ExtractSlowQueryDetails(ctx)[key])
	})

	t.Run("a key holding another type is left alone", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLog(ctx, key, "already reduced")

		AnnotateSlowQueryLogAppendMany(ctx, key, []int{1, 2})

		// The append is dropped rather than clobbering the value that is there,
		// which is what a reduce leaves behind.
		require.Equal(t, "already reduced", ExtractSlowQueryDetails(ctx)[key])
	})

	t.Run("concurrent appends all arrive", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		wg := &sync.WaitGroup{}
		for range 50 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				AnnotateSlowQueryLogAppendMany(ctx, key, []int{1, 1})
			}()
		}
		wg.Wait()
		require.Len(t, ExtractSlowQueryDetails(ctx)[key], 100)
	})
}

func TestDropSlowQueryEntry(t *testing.T) {
	const key = "entries"

	t.Run("a nil context is ignored", func(t *testing.T) {
		var noCtx context.Context
		require.NotPanics(t, func() { DropSlowQueryEntry(noCtx, key) })
	})

	t.Run("a context without details is ignored", func(t *testing.T) {
		require.NotPanics(t, func() { DropSlowQueryEntry(context.Background(), key) })
	})

	t.Run("an absent key is ignored", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		DropSlowQueryEntry(ctx, key)
		require.Empty(t, ExtractSlowQueryDetails(ctx))
	})

	t.Run("the key is gone and its neighbours are not", func(t *testing.T) {
		ctx := InitSlowQueryDetails(context.Background())
		AnnotateSlowQueryLogAppendMany(ctx, key, []int{1, 2})
		AnnotateSlowQueryLog(ctx, "other", "kept")

		DropSlowQueryEntry(ctx, key)

		details := ExtractSlowQueryDetails(ctx)
		require.NotContains(t, details, key)
		require.Equal(t, "kept", details["other"])
	})
}
