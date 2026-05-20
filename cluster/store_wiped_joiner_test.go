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

// TestMarkWipedJoinerCatchUp covers how the wiped-joiner catch-up
// boundary is captured on each Apply by extending lastAppliedIndexToDB.
// The decision (candidate vs not) is made at Store.Open from filesystem
// state; this function only maintains the high-water mark so the
// existing Apply-deferred dbReloadRequired trigger fires at the right
// time.
func TestMarkWipedJoinerCatchUp(t *testing.T) {
	t.Run("candidate: lastAppliedIndexToDB tracks the highest raftLastIndex", func(t *testing.T) {
		st := &Store{}
		st.wipedJoinerCandidate.Store(true)

		st.markWipedJoinerCatchUp(10)
		assert.Equal(t, uint64(10), st.lastAppliedIndexToDB.Load())

		// Same value: no change.
		st.markWipedJoinerCatchUp(10)
		assert.Equal(t, uint64(10), st.lastAppliedIndexToDB.Load())

		// Higher value: extends.
		st.markWipedJoinerCatchUp(20)
		assert.Equal(t, uint64(20), st.lastAppliedIndexToDB.Load())

		// Lower value (e.g. an older AppendEntries replay): never shrinks.
		st.markWipedJoinerCatchUp(15)
		assert.Equal(t, uint64(20), st.lastAppliedIndexToDB.Load(),
			"the high-water mark is monotonic during catch-up")
	})

	t.Run("not a candidate: lastAppliedIndexToDB untouched", func(t *testing.T) {
		st := &Store{}
		// Pre-seed a value so we can detect any unwanted overwrite.
		st.lastAppliedIndexToDB.Store(7)

		st.markWipedJoinerCatchUp(100)
		assert.Equal(t, uint64(7), st.lastAppliedIndexToDB.Load(),
			"non-candidate must not touch lastAppliedIndexToDB")
	})

	t.Run("reloaded: candidate + reloaded → lastAppliedIndexToDB untouched", func(t *testing.T) {
		st := &Store{}
		st.wipedJoinerCandidate.Store(true)
		st.wipedJoinerReloaded.Store(true)
		st.lastAppliedIndexToDB.Store(5)

		st.markWipedJoinerCatchUp(100)
		assert.Equal(t, uint64(5), st.lastAppliedIndexToDB.Load(),
			"once reloadDBFromSchema has run, future Applies must not extend the target")
	})
}

func TestRaftLastIndex_NilRaft(t *testing.T) {
	st := &Store{}
	assert.Equal(t, uint64(0), st.raftLastIndex(),
		"raftLastIndex must be 0 before the raft node exists")
}
