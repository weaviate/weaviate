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
