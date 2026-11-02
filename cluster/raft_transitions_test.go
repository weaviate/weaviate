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

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransitionsCaughtUp(t *testing.T) {
	st := NewMockStore(t, "node1", 0).store

	assert.False(t, st.transitionsCaughtUp(), "the DB has not loaded")

	st.dbLoaded.Store(true)
	assert.False(t, st.transitionsCaughtUp(), "raft is not open")

	st.lastAppliedIndex.Store(10)
	assert.True(t, st.fsmReachedIndex(10))
	assert.False(t, st.fsmReachedIndex(11), "raft is not open, so nothing shows the queue is empty")

	st.joined(11)
	assert.Equal(t, uint64(11), st.joinCommitIndex.Load(), "a node that joined with no state waits for the leader's index too")
}

func TestFSMReachedIndexWaitsForTheLastCommand(t *testing.T) {
	entries := []raft.LogType{raft.LogConfiguration, raft.LogCommand, raft.LogCommand, raft.LogNoop, raft.LogConfiguration}
	tests := []struct {
		name      string
		compacted uint64
		applied   uint64
		index     uint64
		want      bool
	}{
		{name: "the last command is applied", applied: 3, index: 5, want: true},
		{name: "the last command is queued", applied: 2, index: 5},
		{name: "the log does not reach the index", applied: 3, index: 6},
		{name: "the snapshot holds every command and raft has not applied it", compacted: 3, applied: 0, index: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewMockStore(t, "node1", 0).store
			logs := raft.NewInmemStore()
			for i, typ := range entries {
				require.NoError(t, logs.StoreLog(&raft.Log{Index: uint64(i + 1), Type: typ}))
			}
			if tt.compacted > 0 {
				require.NoError(t, logs.DeleteRange(1, tt.compacted))
			}
			cache, err := raft.NewLogCache(16, logs)
			require.NoError(t, err)
			st.logCache = cache
			st.lastAppliedIndex.Store(tt.applied)

			assert.Equal(t, tt.want, st.fsmReachedIndex(tt.index))
		})
	}
}
