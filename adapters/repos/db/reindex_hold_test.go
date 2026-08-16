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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReindexHoldRegistry_RefcountAndScope(t *testing.T) {
	t.Run("raised then released", func(t *testing.T) {
		r := &ReindexHoldRegistry{}
		require.Equal(t, ReindexHoldNone, r.HoldFor("Movies"))
		release := r.acquire("Movies", ReindexHoldCleanup)
		require.Equal(t, ReindexHoldCleanup, r.HoldFor("Movies"))
		release()
		require.Equal(t, ReindexHoldNone, r.HoldFor("Movies"))
	})
	t.Run("overlapping holds do not release each other", func(t *testing.T) {
		r := &ReindexHoldRegistry{}
		first := r.acquire("Movies", ReindexHoldCleanup)
		second := r.acquire("Movies", ReindexHoldCleanup)
		first()
		first() // a released handle is spent; repeats must not consume the second hold
		require.Equal(t, ReindexHoldCleanup, r.HoldFor("Movies"),
			"the second hold must still close the collection")
		second()
		require.Equal(t, ReindexHoldNone, r.HoldFor("Movies"))
	})
	t.Run("scoped to its collection", func(t *testing.T) {
		r := &ReindexHoldRegistry{}
		release := r.acquire("Movies", ReindexHoldCleanup)
		defer release()
		require.Equal(t, ReindexHoldNone, r.HoldFor("Shows"))
	})
	t.Run("no collection means every one", func(t *testing.T) {
		r := &ReindexHoldRegistry{}
		require.Equal(t, ReindexHoldNone, r.HoldFor())
		release := r.acquire("Movies", ReindexHoldCleanup)
		defer release()
		require.Equal(t, ReindexHoldCleanup, r.HoldFor(),
			"an unscoped read must report a hold on any collection")
	})
	t.Run("keys are case-folded", func(t *testing.T) {
		// A hold is raised from a task payload and read with the schema's
		// spelling of the class name; nothing guarantees the two match.
		r := &ReindexHoldRegistry{}
		release := r.acquire("movies", ReindexHoldCleanup)
		defer release()
		require.Equal(t, ReindexHoldCleanup, r.HoldFor("Movies"))
		require.Equal(t, ReindexHoldCleanup, r.HoldFor("MOVIES"))
	})
}

func TestReindexHoldRegistry_HoldReleasesOnEveryReturnPath(t *testing.T) {
	t.Run("normal return", func(t *testing.T) {
		r := &ReindexHoldRegistry{}
		ran := false
		r.Hold("Movies", ReindexHoldCleanup, func() {
			ran = true
			require.Equal(t, ReindexHoldCleanup, r.HoldFor("Movies"),
				"the hold must be raised for the length of the work")
		})
		require.True(t, ran)
		require.Equal(t, ReindexHoldNone, r.HoldFor("Movies"))
	})
	t.Run("panic", func(t *testing.T) {
		r := &ReindexHoldRegistry{}
		require.PanicsWithValue(t, "sweep exploded", func() {
			r.Hold("Movies", ReindexHoldCleanup, func() {
				panic("sweep exploded")
			})
		}, "the panic must keep propagating")
		require.Equal(t, ReindexHoldNone, r.HoldFor("Movies"),
			"a panicking sweep must not leave the collection held")
	})
	t.Run("panic with a hold still outstanding beside it", func(t *testing.T) {
		r := &ReindexHoldRegistry{}
		other := r.acquire("Movies", ReindexHoldCleanup)
		defer other()
		require.Panics(t, func() {
			r.Hold("Movies", ReindexHoldCleanup, func() { panic("sweep exploded") })
		})
		require.Equal(t, ReindexHoldCleanup, r.HoldFor("Movies"),
			"unwinding must release its own hold and only its own")
	})
}

// TestReindexHoldRegistry_Concurrent asserts the end state; -race covers
// the rest.
func TestReindexHoldRegistry_Concurrent(t *testing.T) {
	r := &ReindexHoldRegistry{}
	const (
		goroutines = 32
		ops        = 64
	)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range ops {
				r.Hold("Movies", ReindexHoldCleanup, func() {})
			}
		}()
	}
	wg.Wait()
	require.Equal(t, ReindexHoldNone, r.HoldFor("Movies"))
	r.mu.RLock()
	defer r.mu.RUnlock()
	require.Empty(t, r.holds)
}

// An unrecognized kind has to be nameable too, or the one entry reporting
// a fail-closed refusal says nothing about what closed it.
func TestReindexHoldString(t *testing.T) {
	assert.Equal(t, "none", ReindexHoldNone.String())
	assert.Equal(t, "cleanup", ReindexHoldCleanup.String())
	assert.Equal(t, "unrecognized_hold_99", ReindexHold(99).String())
}

// The gates read the registry live, so a hold raised after a gate call
// started still refuses the next one.
func TestReindexHoldForReadsTheLiveRegistry(t *testing.T) {
	db := &DB{}
	require.Equal(t, ReindexHoldNone, db.ReindexHoldFor("Movies"))
	release := db.reindexHolds.acquire("Movies", ReindexHoldCleanup)
	require.Equal(t, ReindexHoldCleanup, db.ReindexHoldFor("Movies"))
	release()
	require.Equal(t, ReindexHoldNone, db.ReindexHoldFor("Movies"))
}
