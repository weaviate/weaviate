package hnsw

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
)

// Delete attaches a tombstone to an item so it can be periodically cleaned up
// later and the edges reassigned
func (h *hnsw) Delete(id int) error {
	h.addTombstone(id)
	return nil
}

// CleanUpTombstonedNodes removes nodes with a tombstone and reassignes edges
// that were previously pointing to the tombstoned nodes
func (h *hnsw) CleanUpTombstonedNodes() error {
	deleteList := inverted.AllowList{}

	h.RLock()
	lenOfNodes := len(h.nodes)
	tombstones := h.tombstones
	h.RUnlock()

	for id := range tombstones {
		if lenOfNodes <= id {
			// we're trying to delete an id outside the possible range, nothing to do
			continue
		}

		deleteList.Insert(uint32(id))
	}

	if err := h.reassignNeighborsOf(deleteList); err != nil {
		return errors.Wrap(err, "reassign neighbor edges")
	}

	for id := range tombstones {
		if h.entryPointID == id {
			// this a special case because:
			//
			// 1. we need to find a new entrypoint, if this is the last point on this
			// level, we need to find an entyrpoint on a lower level
			// 2. there is a risk that this is the only node in the entire graph. In
			// this case we must reverse the special behavior of inserting the first
			// node
			h.RLock()
			node := h.nodes[id]
			h.RUnlock()
			if err := h.deleteEntrypoint(node, deleteList); err != nil {
				return errors.Wrap(err, "delete entrypoint")
			}
		}
	}

	for id := range tombstones {
		h.Lock()
		h.nodes[id] = nil
		delete(h.tombstones, id)
		h.Unlock()
		h.commitLog.RemoveTombstone(id)
	}

	if h.isEmpty() {
		h.Lock()
		h.nodes = make([]*vertex, 0)
		h.entryPointID = 0
		h.currentMaximumLayer = 0
		h.Unlock()
	}

	return nil
}

func (h *hnsw) countOutgoing(label string, needle int) {
	count := 0
	var ids []int

	for _, node := range h.nodes {
		if node == nil || node.connections == nil {
			continue
		}
		for _, connectionsAtLevel := range node.connections {
			for level, outgoing := range connectionsAtLevel {
				if int(outgoing) == needle {
					count++
					ids = append(ids, node.id)
					fmt.Printf("node id: %d, all connections at level %d: %v\n", node.id, level, connectionsAtLevel)
				}

			}

		}

	}

	fmt.Printf("%s: %d with node to be deleted: %d\n ", label, count, needle)
	fmt.Printf("probelamtic ids: %v\n", ids)

}

func (h *hnsw) reassignNeighborsOf(deleteList inverted.AllowList) error {

	h.RLock()
	size := len(h.nodes)
	currentEntrypoint := h.entryPointID
	h.RUnlock()

	for neighbor := 0; neighbor < size; neighbor++ {
		h.RLock()
		neighborNode := h.nodes[neighbor]
		h.RUnlock()

		if neighborNode == nil || deleteList.Contains(uint32(neighborNode.id)) {
			continue
		}

		neighborVec, err := h.vectorForID(context.Background(), int32(neighbor))
		if err != nil {
			return errors.Wrap(err, "get neighbor vec")
		}
		neighborNode.RLock()
		neighborLevel := neighborNode.level
		connections := neighborNode.connections
		neighborNode.RUnlock()

		if !connectionsPointTo(connections, deleteList) {
			// nothing needs to be changed, skip
			continue
		}

		entryPointID, err := h.findBestEntrypointForNode(h.currentMaximumLayer,
			neighborLevel, currentEntrypoint, neighborVec)
		if err != nil {
			return errors.Wrap(err, "find best entrypoint")
		}

		if entryPointID == neighbor {
			// if we use ourselves as entrypoint and delete all connections in the
			// next step, we won't find any neighbors, so we need to use an
			// alternative entryPoint in this round

			if h.isOnlyNode(&vertex{id: neighbor}, deleteList) {
				neighborNode.Lock()
				// delete all existing connections before re-assigning
				neighborNode.connections = map[int][]uint32{}
				neighborNode.Unlock()
				continue
			}

			tmpDenyList := deleteList
			tmpDenyList.Insert(uint32(entryPointID))
			alternative, _ := h.findNewEntrypoint(tmpDenyList, h.currentMaximumLayer)
			entryPointID = alternative
		}

		neighborNode.Lock()
		// delete all existing connections before re-assigning
		neighborNode.connections = map[int][]uint32{}
		neighborNode.Unlock()

		if err := h.findAndConnectNeighbors(neighborNode, entryPointID, neighborVec,
			neighborLevel, h.currentMaximumLayer, deleteList); err != nil {
			return errors.Wrap(err, "find and connect neighbors")
		}
	}

	return nil
}

func connectionsPointTo(connections map[int][]uint32, needles inverted.AllowList) bool {
	for _, atLevel := range connections {
		for _, pointer := range atLevel {
			if needles.Contains(pointer) {
				return true
			}
		}
	}

	return false
}

// deleteEntrypoint deletes the current entrypoint and replaces it with a new
// one. It respects the attached denyList, so that it doesn't assign another
// node which also has a tombstone and is also in the process of being cleaned
// up
func (h *hnsw) deleteEntrypoint(node *vertex, denyList inverted.AllowList) error {
	if h.isOnlyNode(node, denyList) {
		// no point in finding another entrypoint if this is the only node
		return nil
	}

	node.Lock()
	level := node.level
	node.Unlock()

	newEntrypoint, level := h.findNewEntrypoint(denyList, level)

	h.Lock()
	h.entryPointID = newEntrypoint
	h.currentMaximumLayer = level
	h.Unlock()

	return nil
}

// returns entryPointID, level
func (h *hnsw) findNewEntrypoint(denyList inverted.AllowList, targetLevel int) (int, int) {
	for l := targetLevel; l >= 0; l-- {
		// ideally we can find a new entrypoint at the same level of the
		// to-be-deleted node. However, there is a chance it was the only node on
		// that level, in that case we need to look at the next lower level for a
		// better candidate

		h.RLock()
		maxNodes := len(h.nodes)
		h.RUnlock()

		for i := 0; i < maxNodes; i++ {
			if denyList.Contains(uint32(i)) {
				continue
			}
			h.RLock()
			candidate := h.nodes[i]
			h.RUnlock()

			if candidate == nil {
				continue
			}

			candidate.RLock()
			candidateLevel := candidate.level
			candidate.RUnlock()

			if candidateLevel != l {
				// not reaching up to the current level, skip in hope of finding another candidate
				continue
			}

			// we have a node that matches
			return i, l
		}
	}

	// we made it thorugh the entire graph and didn't find a new entrypoint all
	// the way down to level 0. This can only mean the graph is emtpy, which is
	// unexpected.
	panic("findNewEntrypoint called on an empty hnsw graph")
}

func (h *hnsw) isOnlyNode(needle *vertex, denyList inverted.AllowList) bool {
	h.RLock()
	defer h.RUnlock()

	for _, node := range h.nodes {
		if node == nil || node.id == needle.id || denyList.Contains(uint32(node.id)) {
			continue
		}

		return false
	}

	return true
}

func (h *hnsw) isEmpty() bool {
	h.RLock()
	defer h.RUnlock()

	for _, node := range h.nodes {
		if node != nil {
			return false
		}
	}

	return true
}

func (h *hnsw) hasTombstone(id int) bool {
	h.RLock()
	defer h.RUnlock()
	_, ok := h.tombstones[id]
	return ok
}

func (h *hnsw) addTombstone(id int) {
	h.Lock()
	h.tombstones[id] = struct{}{}
	h.Unlock()
	h.commitLog.AddTombstone(id)
}
