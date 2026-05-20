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

// TestNoteWipedJoinerProgress covers the schemaOnly + catchUpTarget
// state machine. The wiped-joiner decision itself is made deterministically
// at Store.Open from filesystem state (wipedJoinerCandidate); this function
// only maintains catchUpTarget as new AppendEntries arrive and reports
// whether the entry must be applied schema-only.
func TestNoteWipedJoinerProgress(t *testing.T) {
	t.Run("candidate: schema-only across the backlog, target tracks log tip", func(t *testing.T) {
		st := &Store{}
		st.wipedJoinerCandidate.Store(true)

		schemaOnly := st.noteWipedJoinerProgress(3, 10)
		assert.True(t, schemaOnly, "candidate entries must apply schema-only")
		assert.Equal(t, uint64(10), st.catchUpTarget.Load())

		// More entries in the backlog: still schema-only; target stays at the
		// highest seen tip.
		for idx := uint64(4); idx <= 10; idx++ {
			assert.True(t, st.noteWipedJoinerProgress(idx, 10))
		}
		assert.Equal(t, uint64(10), st.catchUpTarget.Load())

		// Once the watcher has run the reload, later (runtime) entries apply
		// normally: never schema-only.
		st.wipedJoinerReloaded.Store(true)
		assert.False(t, st.noteWipedJoinerProgress(11, 11),
			"runtime entries after catch-up apply normally")
	})

	t.Run("candidate: backlog delivered in multiple chunks extends the target", func(t *testing.T) {
		st := &Store{}
		st.wipedJoinerCandidate.Store(true)

		assert.True(t, st.noteWipedJoinerProgress(3, 5))
		assert.Equal(t, uint64(5), st.catchUpTarget.Load())

		// Second chunk: tip jumps to 12.
		assert.True(t, st.noteWipedJoinerProgress(4, 12))
		assert.Equal(t, uint64(12), st.catchUpTarget.Load())

		// A later entry with a smaller raftLastIndex must NOT shrink the
		// target (the high-water mark is monotonic during catch-up).
		assert.True(t, st.noteWipedJoinerProgress(5, 11))
		assert.Equal(t, uint64(12), st.catchUpTarget.Load())
	})

	t.Run("not a candidate: never schema-only", func(t *testing.T) {
		st := &Store{}
		// wipedJoinerCandidate is false (default); intact-data restart or
		// snapshot-restored node.

		assert.False(t, st.noteWipedJoinerProgress(3, 100))
		assert.Equal(t, uint64(0), st.catchUpTarget.Load(),
			"non-candidate must not touch catchUpTarget")
	})

	t.Run("reloaded: candidate + reloaded → no longer schema-only", func(t *testing.T) {
		st := &Store{}
		st.wipedJoinerCandidate.Store(true)
		st.wipedJoinerReloaded.Store(true)

		assert.False(t, st.noteWipedJoinerProgress(3, 100),
			"once reloadDBFromSchema has run, future Applies are normal")
	})
}

func TestRaftLastIndex_NilRaft(t *testing.T) {
	st := &Store{}
	assert.Equal(t, uint64(0), st.raftLastIndex(),
		"raftLastIndex must be 0 before the raft node exists")
}
