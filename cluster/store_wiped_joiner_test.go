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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNoteWipedJoinerProgress covers the catch-up state machine that decides
// whether a node is a wiped node rejoining via log replay (must replay
// schema-only, then start the catch-up watcher) versus a fresh bootstrap or a
// node with prior state (which must not).
func TestNoteWipedJoinerProgress(t *testing.T) {
	t.Run("wiped joiner: schema-only across the backlog, watcher started once", func(t *testing.T) {
		st := &Store{}
		st.startedEmpty.Store(true) // no durable RAFT state at process start
		// lastAppliedIndexToDB stays 0 (no prior DB / no snapshot)

		// First applied command at index 3, RAFT log tip already at 10:
		// a committed backlog exists -> this is a wiped joiner catching up.
		schemaOnly, startWatcher := st.noteWipedJoinerProgress(3, 10)
		assert.True(t, schemaOnly, "backlog entries must apply schema-only")
		assert.True(t, startWatcher, "the catch-up watcher must be started once")
		assert.Equal(t, uint64(10), st.catchUpTarget.Load())

		// Remaining backlog entries: still schema-only, watcher not re-started.
		for idx := uint64(4); idx <= 10; idx++ {
			so, sw := st.noteWipedJoinerProgress(idx, 10)
			assert.True(t, so)
			assert.False(t, sw, "watcher is started exactly once")
		}

		// Once the watcher has run the reload, later (runtime) entries apply
		// normally: never schema-only.
		st.wipedJoinerReloaded.Store(true)
		so, sw := st.noteWipedJoinerProgress(11, 11)
		assert.False(t, so, "runtime entries after catch-up apply normally")
		assert.False(t, sw)
	})

	t.Run("wiped joiner: backlog delivered in multiple chunks extends the target", func(t *testing.T) {
		st := &Store{}
		st.startedEmpty.Store(true)

		// First chunk: tip at 5.
		so, sw := st.noteWipedJoinerProgress(3, 5)
		assert.True(t, so)
		assert.True(t, sw)
		assert.Equal(t, uint64(5), st.catchUpTarget.Load())

		// Second chunk arrives before we reached 5: tip jumps to 12, the
		// target must extend so the later entries are still schema-only.
		so, sw = st.noteWipedJoinerProgress(4, 12)
		assert.True(t, so)
		assert.False(t, sw)
		assert.Equal(t, uint64(12), st.catchUpTarget.Load())
	})

	t.Run("fresh bootstrap: never schema-only, no watcher", func(t *testing.T) {
		st := &Store{}
		st.startedEmpty.Store(true)

		// First applied command is live (index == log tip): no backlog,
		// so this is a fresh bootstrap, not a wiped rejoin.
		so, sw := st.noteWipedJoinerProgress(3, 3)
		assert.False(t, so)
		assert.False(t, sw)
		assert.Equal(t, uint64(0), st.catchUpTarget.Load())
		assert.True(t, st.catchUpDecided.Load())

		// Further runtime class creations must stay unaffected.
		so, sw = st.noteWipedJoinerProgress(4, 4)
		assert.False(t, so)
		assert.False(t, sw)
	})

	t.Run("decision is locked on the first command: a later catch-up is not a rejoin", func(t *testing.T) {
		st := &Store{}
		st.startedEmpty.Store(true)

		// Fresh bootstrap decided on the first command.
		so, sw := st.noteWipedJoinerProgress(3, 3)
		assert.False(t, so)
		assert.False(t, sw)

		// Much later the node falls behind and catches up (e.g. after a
		// network partition). This must NOT be misread as a wiped rejoin.
		so, sw = st.noteWipedJoinerProgress(20, 500)
		assert.False(t, so, "a runtime catch-up must not trigger schema-only replay")
		assert.False(t, sw)
		assert.Equal(t, uint64(0), st.catchUpTarget.Load())
	})

	t.Run("single-entry backlog edge: degrades to no recovery", func(t *testing.T) {
		st := &Store{}
		st.startedEmpty.Store(true)

		// The only committed entry is the first command itself: there is no
		// earlier entry to reveal the backlog, so it is treated as a fresh
		// entry. Documented edge - degrades to today's behaviour.
		so, sw := st.noteWipedJoinerProgress(3, 3)
		assert.False(t, so)
		assert.False(t, sw)
	})

	t.Run("node with prior state is excluded", func(t *testing.T) {
		// Not started empty (intact restart): the existing catch-up
		// machinery handles it.
		st := &Store{}
		st.startedEmpty.Store(false)
		so, sw := st.noteWipedJoinerProgress(3, 100)
		assert.False(t, so)
		assert.False(t, sw)

		// Started empty but a snapshot was installed (lastAppliedIndexToDB
		// != 0): Store.Restore already ran the DB load, so this path is off.
		snapRestored := &Store{}
		snapRestored.startedEmpty.Store(true)
		snapRestored.lastAppliedIndexToDB.Store(42)
		so, sw = snapRestored.noteWipedJoinerProgress(3, 100)
		assert.False(t, so)
		assert.False(t, sw)
	})
}

func TestRaftLastIndex_NilRaft(t *testing.T) {
	st := &Store{}
	assert.Equal(t, uint64(0), st.raftLastIndex(),
		"raftLastIndex must be 0 before the raft node exists")
}
