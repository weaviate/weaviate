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
	"github.com/hashicorp/raft"
)

// fsmReachedIndex checks the last command at or below index: config and noop entries never advance
// lastAppliedIndex, and raft's own applied index moves on enqueue, before the FSM runs the entry.
func (st *Store) fsmReachedIndex(index uint64) bool {
	applied := st.lastAppliedIndex.Load()
	if applied >= index {
		return true
	}
	if last, found := st.lastCommandAtOrBelow(index); found {
		return applied >= last
	}
	// A snapshot covers index, and raft counts it applied only once the restore returned.
	rn := st.raft.Load()
	return rn != nil && rn.AppliedIndex() >= index
}

func (st *Store) lastCommandAtOrBelow(index uint64) (last uint64, found bool) {
	if st.logCache == nil {
		return 0, false
	}
	first, err := st.logCache.FirstIndex()
	if err != nil || first == 0 {
		return 0, false
	}
	var entry raft.Log
	for i := index; i >= first; i-- {
		if err := st.logCache.GetLog(i, &entry); err != nil {
			return 0, false
		}
		if entry.Type == raft.LogCommand {
			return i, true
		}
	}
	return 0, false
}
