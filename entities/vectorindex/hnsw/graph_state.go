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

package hnsw

// GraphState holds the complete graph structure from deserialization.
// This structure contains only graph-related data, separate from compression state.
type GraphState struct {
	// Nodes is the graph structure indexed by node ID.
	Nodes []*Vertex

	// NodesDeleted tracks which node slots have been explicitly deleted.
	NodesDeleted map[uint64]struct{}

	// Entrypoint is the entry node ID for the HNSW graph.
	Entrypoint uint64

	// Level is the maximum level of the graph.
	Level uint16

	// Tombstones marks nodes that are logically deleted but not yet cleaned up.
	Tombstones map[uint64]struct{}

	// TombstonesDeleted tracks tombstones that have been cleaned up.
	TombstonesDeleted map[uint64]struct{}

	// EntrypointChanged indicates if the entrypoint was modified during deserialization.
	EntrypointChanged bool

	// LinksReplaced tracks which nodes and levels have had their links replaced
	// rather than appended. This is critical for correctly handling .condensed
	// files, as described in https://github.com/weaviate/weaviate/issues/1868.
	//
	// If there is no entry for a node/level, we must assume that all links were
	// appended and prior state must exist. When we encounter a ReplaceLinksAtLevel
	// or ClearLinksAtLevel operation, we explicitly set the replace flag so that
	// future operations (including condensing multiple logs) correctly interpret
	// whether links should be replaced or appended.
	LinksReplaced map[uint64]map[uint16]struct{}
}

// ReplaceLinks checks if links at the given node and level should be replaced.
func (gs *GraphState) ReplaceLinks(node uint64, level uint16) bool {
	if gs == nil || gs.LinksReplaced == nil {
		return false
	}

	levels, ok := gs.LinksReplaced[node]
	if !ok {
		return false
	}

	_, ok = levels[level]
	return ok
}

// NewGraphState creates a new GraphState with initialized maps.
func NewGraphState(initialCapacity int) *GraphState {
	return &GraphState{
		Nodes:             make([]*Vertex, initialCapacity),
		NodesDeleted:      make(map[uint64]struct{}),
		Tombstones:        make(map[uint64]struct{}),
		TombstonesDeleted: make(map[uint64]struct{}),
		LinksReplaced:     make(map[uint64]map[uint16]struct{}),
	}
}
