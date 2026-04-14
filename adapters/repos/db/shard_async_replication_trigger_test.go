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
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestHasNewElement covers the core predicate that controls whether the
// hashbeat trigger goroutine fires when the set of alive nodes changes.
//
// The invariant being enforced: a hashbeat should only be triggered when at
// least one node that was absent in the previous check is now alive.  A node
// going down is not a reason to trigger, because there is nothing to propagate
// to an unreachable peer.
func TestHasNewElement(t *testing.T) {
	tests := []struct {
		name       string
		candidates []string // current alive nodes
		existing   []string // nodes from last comparison
		want       bool
	}{
		{
			name:       "no change - same nodes",
			candidates: []string{"node1", "node2"},
			existing:   []string{"node1", "node2"},
			want:       false,
		},
		{
			name:       "no change - both empty",
			candidates: []string{},
			existing:   []string{},
			want:       false,
		},
		{
			name:       "node went down - should NOT trigger",
			candidates: []string{"node1"},
			existing:   []string{"node1", "node2"},
			want:       false,
		},
		{
			name:       "all nodes went down - should NOT trigger",
			candidates: []string{},
			existing:   []string{"node1", "node2"},
			want:       false,
		},
		{
			name:       "new node joined - should trigger",
			candidates: []string{"node1", "node2", "node3"},
			existing:   []string{"node1", "node2"},
			want:       true,
		},
		{
			name:       "first node appeared from empty - should trigger",
			candidates: []string{"node1"},
			existing:   []string{},
			want:       true,
		},
		{
			name:       "node went down AND a different new node joined - should trigger",
			candidates: []string{"node1", "node3"},
			existing:   []string{"node1", "node2"},
			want:       true, // node3 is new even though node2 left
		},
		{
			name:       "completely different node set - should trigger",
			candidates: []string{"node3", "node4"},
			existing:   []string{"node1", "node2"},
			want:       true,
		},
		{
			name:       "order does not matter - same nodes different order",
			candidates: []string{"node2", "node1"},
			existing:   []string{"node1", "node2"},
			want:       false,
		},
		{
			name:       "nil candidates - should NOT trigger",
			candidates: nil,
			existing:   []string{"node1"},
			want:       false,
		},
		{
			name:       "nil existing - new node should trigger",
			candidates: []string{"node1"},
			existing:   nil,
			want:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := hasNewElement(tc.candidates, tc.existing)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestHashbeatTriggerStateUpdate verifies the condition used to decide whether
// setLastComparedNodes should be called: it must update whenever the topology
// changes in either direction (new node OR node gone), not only when a new
// node appears.
func TestHashbeatTriggerStateUpdate(t *testing.T) {
	shouldUpdateState := func(aliveHosts, comparedHosts []string) bool {
		return hasNewElement(aliveHosts, comparedHosts) || len(aliveHosts) != len(comparedHosts)
	}

	tests := []struct {
		name          string
		aliveHosts    []string
		comparedHosts []string
		wantTrigger   bool // should fire hashbeat
		wantUpdate    bool // should update stored state
	}{
		{
			name:          "no change",
			aliveHosts:    []string{"node1", "node2"},
			comparedHosts: []string{"node1", "node2"},
			wantTrigger:   false,
			wantUpdate:    false,
		},
		{
			name:          "node went down",
			aliveHosts:    []string{"node1"},
			comparedHosts: []string{"node1", "node2"},
			wantTrigger:   false,
			wantUpdate:    true, // state should still be updated for next comparison
		},
		{
			name:          "node came back up",
			aliveHosts:    []string{"node1", "node2"},
			comparedHosts: []string{"node1"},
			wantTrigger:   true,
			wantUpdate:    true,
		},
		{
			name:          "node replaced by different node",
			aliveHosts:    []string{"node1", "node3"},
			comparedHosts: []string{"node1", "node2"},
			wantTrigger:   true,
			wantUpdate:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotTrigger := hasNewElement(tc.aliveHosts, tc.comparedHosts)
			gotUpdate := shouldUpdateState(tc.aliveHosts, tc.comparedHosts)

			assert.Equal(t, tc.wantTrigger, gotTrigger, "hashbeat trigger mismatch")
			assert.Equal(t, tc.wantUpdate, gotUpdate, "state update mismatch")
		})
	}
}
