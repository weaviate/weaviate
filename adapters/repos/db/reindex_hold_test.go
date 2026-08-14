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

// TestReindexHoldRegistry_RefcountAndScope walks what the registry has to
// get right for the gate to answer correctly: a hold is visible while it
// is raised, overlapping holds do not release each other, and a hold on
// one collection never bleeds into a sibling.
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
		// Two terminal transitions on different properties of one
		// collection overlap routinely. A boolean would let the first to
		// finish reopen the gate under the second.
		r := &ReindexHoldRegistry{}
		first := r.acquire("Movies", ReindexHoldCleanup)
		second := r.acquire("Movies", ReindexHoldCleanup)
		first()
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
	t.Run("keys are case-folded", func(t *testing.T) {
		// A hold is raised from a task payload and read with the class
		// name the schema hands the gate. Nothing guarantees the two
		// spellings match.
		r := &ReindexHoldRegistry{}
		release := r.acquire("movies", ReindexHoldCleanup)
		defer release()
		require.Equal(t, ReindexHoldCleanup, r.HoldFor("Movies"))
		require.Equal(t, ReindexHoldCleanup, r.HoldFor("MOVIES"))
	})
}

// TestReindexHoldRegistry_ReleaseIsIdempotent pins that releasing twice
// costs nothing. A caller that releases explicitly and again from a defer
// would otherwise drop a hold a concurrent teardown is still counting on.
func TestReindexHoldRegistry_ReleaseIsIdempotent(t *testing.T) {
	r := &ReindexHoldRegistry{}
	outer := r.acquire("Movies", ReindexHoldCleanup)
	inner := r.acquire("Movies", ReindexHoldCleanup)
	inner()
	inner()
	inner()
	require.Equal(t, ReindexHoldCleanup, r.HoldFor("Movies"),
		"repeat releases must not consume the other hold")
	outer()
	require.Equal(t, ReindexHoldNone, r.HoldFor("Movies"))
}

// TestReindexHoldRegistry_HoldReleasesOnEveryReturnPath pins that the
// hold is dropped whether the work returns or panics. A hold leaked by a
// panicking sweep closes the collection to every backup and restore until
// the process restarts, which is the one state an operator cannot clear.
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

// TestReindexHoldRegistry_Concurrent pins the registry under the shape a
// real node produces: many teardowns raising and dropping holds on one
// collection at once. The observable contract is the end state; -race
// covers the rest.
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

// TestReindexHoldString pins the names the gate's log line carries. An
// unrecognized kind has to be nameable too, or the one entry reporting a
// fail-closed refusal says nothing about what closed it.
func TestReindexHoldString(t *testing.T) {
	assert.Equal(t, "none", ReindexHoldNone.String())
	assert.Equal(t, "cleanup", ReindexHoldCleanup.String())
	assert.Equal(t, "unrecognized_hold_99", ReindexHold(99).String())
}

// TestReindexHoldLookupBuilder pins the wiring contract: the closure
// reads the live registry on every call, so the gate sees a hold raised
// after the builder was installed.
func TestReindexHoldLookupBuilder(t *testing.T) {
	p := &ReindexProvider{}
	lookup := p.ReindexHoldLookupBuilder()()
	require.NotNil(t, lookup)
	require.Equal(t, ReindexHoldNone, lookup([]string{"Movies"}))
	release := p.holds.acquire("Movies", ReindexHoldCleanup)
	require.Equal(t, ReindexHoldCleanup, lookup([]string{"Movies"}),
		"the lookup must observe a hold raised after it was built")
	release()
	require.Equal(t, ReindexHoldNone, lookup([]string{"Movies"}))
}
