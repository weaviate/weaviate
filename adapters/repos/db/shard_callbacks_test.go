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
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
)

// onAddToPropertyValueIndex is a test-only shorthand that fires the registered
// add callbacks against the shard's default (no-scope) index state. Production
// code fires callbacks via fireAddToPropertyValueIndex directly with an
// explicit scope state.
func (s *Shard) onAddToPropertyValueIndex(docID uint64, property *inverted.Property) error {
	return s.fireAddToPropertyValueIndex(s.loadPropValueIndexState().add, docID, property)
}

// TestShardCallbacks_ConcurrentRegistrationAndWrites verifies that arming and
// disarming a mirror while another goroutine is firing callbacks does not
// race, and that every disarm removes its registration rather than flagging
// it. Run with -race.
func TestShardCallbacks_ConcurrentRegistrationAndWrites(t *testing.T) {
	s := &Shard{index: &Index{logger: logrus.New()}}

	// A witness registration stays armed for the whole test.
	var baseCount atomic.Int64
	s.registerDoubleWriteWithScope([]string{"p"}, nil,
		func(map[string]struct{}) (onAddToPropertyValueIndex, onDeleteFromPropertyValueIndex) {
			count := func(_ *Shard, _ uint64, _ *inverted.Property) error {
				baseCount.Add(1)
				return nil
			}
			return count, count
		})

	const (
		numWriters       = 4
		writesPerWriter  = 500
		numRegistrations = 20
	)

	var wg sync.WaitGroup
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerWriter; i++ {
				_ = s.onAddToPropertyValueIndex(uint64(i), &inverted.Property{Name: "p"})
			}
		}()
	}
	for r := 0; r < numRegistrations; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.registerDoubleWriteWithScope([]string{"p"}, nil, noopMirrorCallbacks)("p")
		}()
	}
	wg.Wait()

	assert.Greater(t, baseCount.Load(), int64(0), "the witness registration must have fired")
	assert.Len(t, s.loadPropValueIndexState().add, 1,
		"disarm must REMOVE each registration; the slice must not accumulate disarmed closures")
}

// TestShardCallbacks_DisarmRemovesCallbacks_NoUnboundedGrowth is the regression
// test for the callback-slice leak (weaviate/0-weaviate-issues#298 family).
// Disarm must REMOVE a registration's closures from the folded write-path
// snapshot, not merely flag them disabled: flagged closures would stay on the
// hot write path for the life of the shard, one set per past migration.
//
// The shrink is asserted through observable behavior — firing the snapshot
// invokes only the currently armed callbacks — so an append-without-remove
// regression makes the leaked closures fire and inflates the count.
func TestShardCallbacks_DisarmRemovesCallbacks_NoUnboundedGrowth(t *testing.T) {
	const numMigrations = 100

	t.Run("double-write scope path", func(t *testing.T) {
		s := &Shard{}
		var addInvocations, delInvocations atomic.Int64
		props := []string{"p"}

		// Each migration arms the add+delete pair AND the scope, then disarms
		// all three in one atomic mutate.
		for i := 0; i < numMigrations; i++ {
			s.registerDoubleWriteWithScope(props, nil,
				func(map[string]struct{}) (onAddToPropertyValueIndex, onDeleteFromPropertyValueIndex) {
					return func(_ *Shard, _ uint64, _ *inverted.Property) error { addInvocations.Add(1); return nil },
						func(_ *Shard, _ uint64, _ *inverted.Property) error { delInvocations.Add(1); return nil }
				})("p")
		}

		st := s.loadPropValueIndexState()
		assert.Empty(t, st.add, "every disarm must remove its add closure")
		assert.Empty(t, st.del, "every disarm must remove its delete closure")
		assert.Empty(t, st.scope.props, "disarm must collapse the scope back to the idle fast path")

		// Firing must invoke nothing — all pairs were removed, not just flagged.
		require.NoError(t, s.fireAddToPropertyValueIndex(st.add, 1, &inverted.Property{Name: "p"}))
		require.NoError(t, s.fireDeleteFromPropertyValueIndex(st.del, 1, &inverted.Property{Name: "p"}))
		assert.Equal(t, int64(0), addInvocations.Load(),
			"all add closures must be removed on disarm; a leak would fire %d of them", numMigrations)
		assert.Equal(t, int64(0), delInvocations.Load(),
			"all delete closures must be removed on disarm; a leak would fire %d of them", numMigrations)
	})
}

// TestDeriveScope pins that the write path's scope is the union of the
// surviving registrations — one migration's disarm can't strip a property
// another still mirrors — and that the most recent arm wins where two
// overlay one property differently.
func TestDeriveScope(t *testing.T) {
	filterable := inverted.PropertyOverlay{ForceFilterable: true}
	rangeable := inverted.PropertyOverlay{ForceRangeable: true}
	searchable := inverted.PropertyOverlay{ForceSearchable: true}

	tests := []struct {
		name          string
		regs          []migrationScopeReg
		wantProps     []string
		wantOverlay   map[string]inverted.PropertyOverlay
		wantConflicts []string
	}{
		{
			name:      "no registrations leave the idle fast path",
			wantProps: nil,
		},
		{
			name: "two registrations union their properties",
			regs: []migrationScopeReg{
				{id: 1, props: map[string]struct{}{"title": {}}},
				{id: 2, props: map[string]struct{}{"title": {}, "body": {}}},
			},
			wantProps: []string{"body", "title"},
		},
		{
			name: "the same overlay twice is not a conflict",
			regs: []migrationScopeReg{
				{id: 1, props: map[string]struct{}{"title": {}}, overlay: map[string]inverted.PropertyOverlay{"title": filterable}},
				{id: 2, props: map[string]struct{}{"title": {}}, overlay: map[string]inverted.PropertyOverlay{"title": filterable}},
			},
			wantProps:   []string{"title"},
			wantOverlay: map[string]inverted.PropertyOverlay{"title": filterable},
		},
		{
			name: "the most recent arm's overlay wins",
			regs: []migrationScopeReg{
				{id: 1, props: map[string]struct{}{"title": {}}, overlay: map[string]inverted.PropertyOverlay{"title": filterable}},
				{id: 2, props: map[string]struct{}{"title": {}}, overlay: map[string]inverted.PropertyOverlay{"title": rangeable}},
			},
			wantProps:     []string{"title"},
			wantOverlay:   map[string]inverted.PropertyOverlay{"title": rangeable},
			wantConflicts: []string{"title"},
		},
		{
			// The registration carrying no overlay is asking for the schema's
			// own analysis, so the two disagree and each needs its own.
			name: "an arm with no overlay disagrees with one that forces a flag",
			regs: []migrationScopeReg{
				{id: 1, props: map[string]struct{}{"title": {}}},
				{id: 2, props: map[string]struct{}{"title": {}}, overlay: map[string]inverted.PropertyOverlay{"title": filterable}},
			},
			wantProps:     []string{"title"},
			wantOverlay:   map[string]inverted.PropertyOverlay{"title": filterable},
			wantConflicts: []string{"title"},
		},
		{
			name: "the arm with no overlay is the more recent one",
			regs: []migrationScopeReg{
				{id: 1, props: map[string]struct{}{"title": {}}, overlay: map[string]inverted.PropertyOverlay{"title": filterable}},
				{id: 2, props: map[string]struct{}{"title": {}}},
			},
			wantProps:     []string{"title"},
			wantOverlay:   map[string]inverted.PropertyOverlay{"title": filterable},
			wantConflicts: []string{"title"},
		},
		{
			name: "an arm with no overlay on a property it does not share is nobody's disagreement",
			regs: []migrationScopeReg{
				{id: 1, props: map[string]struct{}{"body": {}}},
				{id: 2, props: map[string]struct{}{"title": {}}, overlay: map[string]inverted.PropertyOverlay{"title": filterable}},
			},
			wantProps:   []string{"body", "title"},
			wantOverlay: map[string]inverted.PropertyOverlay{"title": filterable},
		},
		{
			name: "three registrations disagreeing on one property report it once",
			regs: []migrationScopeReg{
				{id: 1, props: map[string]struct{}{"title": {}}, overlay: map[string]inverted.PropertyOverlay{"title": filterable}},
				{id: 2, props: map[string]struct{}{"title": {}}, overlay: map[string]inverted.PropertyOverlay{"title": rangeable}},
				{id: 3, props: map[string]struct{}{"title": {}}, overlay: map[string]inverted.PropertyOverlay{"title": searchable}},
			},
			wantProps:     []string{"title"},
			wantOverlay:   map[string]inverted.PropertyOverlay{"title": searchable},
			wantConflicts: []string{"title"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scope, conflicts := deriveScope(tt.regs)

			props := make([]string, 0, len(scope.props))
			for prop := range scope.props {
				props = append(props, prop)
			}
			sort.Strings(props)
			assert.Equal(t, tt.wantProps, emptyToNil(props))
			assert.Equal(t, tt.wantOverlay, scope.overlay)
			assert.Equal(t, tt.wantConflicts, conflicts)
		})
	}
}

// emptyToNil lets a row say "nothing armed" once rather than distinguishing an
// empty slice from a nil one.
func emptyToNil(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return s
}
