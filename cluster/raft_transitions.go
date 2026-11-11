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
	"errors"
	"fmt"

	"github.com/hashicorp/raft"
)

// fsmReachedIndex checks the last command at or below index: config and noop entries never advance
// lastAppliedIndex, and raft's own applied index moves on enqueue, before the FSM runs the entry.
func (st *Store) fsmReachedIndex(index uint64) (bool, error) {
	applied := max(st.lastAppliedIndex.Load(), st.restoredIndex.Load())
	if applied >= index {
		return true, nil
	}
	last, found, err := st.lastCommandAtOrBelow(index)
	if err != nil {
		return false, err
	}
	if found {
		return applied >= last, nil
	}
	// A snapshot covers index, and raft counts it applied only once the restore returned.
	rn := st.raft.Load()
	return rn != nil && rn.AppliedIndex() >= index, nil
}

// lastCommandAtOrBelow reports found false when the log holds no command at or below index.
func (st *Store) lastCommandAtOrBelow(index uint64) (last uint64, found bool, err error) {
	if st.logCache == nil {
		return 0, false, nil
	}
	first, err := st.logCache.FirstIndex()
	if err != nil {
		return 0, false, fmt.Errorf("read first log index: %w", err)
	}
	if first == 0 {
		return 0, false, nil
	}
	var entry raft.Log
	for i := index; i >= first; i-- {
		if err := st.logCache.GetLog(i, &entry); err != nil {
			if errors.Is(err, raft.ErrLogNotFound) {
				return 0, false, nil
			}
			return 0, false, fmt.Errorf("read log %d: %w", i, err)
		}
		if entry.Type == raft.LogCommand {
			return i, true, nil
		}
	}
	return 0, false, nil
}
