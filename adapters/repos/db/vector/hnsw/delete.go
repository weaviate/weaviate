package hnsw

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
)

func (h *hnsw) Delete(id int) error {
	h.RLock()
	lenOfNodes := len(h.nodes)
	h.RUnlock()
	if lenOfNodes <= id {
		// we're trying to delete an id outside the possible range, nothing to do
		return nil
	}

	h.RLock()
	node := h.nodes[id]
	h.RUnlock()
	if node == nil {
		// this node doesn't exist, we can't do anything else since without a node
		// we also don't have connections to follow for cleaning up of neighbors
		return nil
	}

	// h.countOutgoing("before", node.id)

	if err := h.reassignNeighborsOf(id); err != nil {
		return errors.Wrap(err, "reassign neighbor edges")
	}

	if h.entryPointID == id {
		// this a special case because:
		//
		// 1. we need to find a new entrypoint, if this is the last point on this
		// level, we need to find an entyrpoint on a lower level
		// 2. there is a risk that this is the only node in the entire graph. In
		// this case we must reverse the special behavior of inserting the first
		// node
		if err := h.deleteEntrypoint(node); err != nil {
			return errors.Wrap(err, "delete entrypoint")
		}
	}

	h.Lock()
	h.nodes[id] = nil
	h.Unlock()

	// h.countOutgoing("after", node.id)

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

func (h *hnsw) reassignNeighborsOf(toBeDeleted int) error {
	denyList := inverted.AllowList{}
	denyList.Insert(uint32(toBeDeleted))

	h.RLock()
	size := len(h.nodes)
	h.RUnlock()

	for neighbor := 0; neighbor < size; neighbor++ {
		// TODO: pass through context, instead of spawning a new one
		h.RLock()
		neighborNode := h.nodes[neighbor]
		h.RUnlock()

		if neighborNode == nil || neighborNode.id == toBeDeleted {
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

		if !connectionsPointTo(connections, toBeDeleted) {
			// nothing needs to be changed, skip
			continue
		}

		entryPointID, err := h.findBestEntrypointForNode(h.currentMaximumLayer,
			neighborLevel, h.entryPointID, neighborVec)
		if err != nil {
			return errors.Wrap(err, "find best entrypoint")
		}

		neighborNode.Lock()
		// delete all existing connections before re-assigning
		neighborNode.connections = map[int][]uint32{}
		neighborNode.Unlock()

		if err := h.findAndConnectNeighbors(neighborNode, entryPointID, neighborVec,
			neighborLevel, h.currentMaximumLayer, denyList); err != nil {
			return errors.Wrap(err, "find and connect neighbors")
		}
	}

	return nil
}

func connectionsPointTo(connections map[int][]uint32, needle int) bool {
	for _, atLevel := range connections {
		for _, pointer := range atLevel {
			if int(pointer) == needle {
				return true
			}
		}
	}

	return false
}

func (h *hnsw) deleteEntrypoint(node *hnswVertex) error {
	if h.isOnlyNode(node) {
		return fmt.Errorf("deleting the only node in the graph not supported yet")
	}

	node.Lock()
	level := node.level
	toBeDeleted := node.id
	node.Unlock()

	denyList := inverted.AllowList{}
	denyList.Insert(uint32(toBeDeleted))

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

func (h *hnsw) isOnlyNode(needle *hnswVertex) bool {
	h.RLock()
	defer h.RUnlock()

	for _, node := range h.nodes {
		if node == nil || node.id == needle.id {
			continue
		}

		return false
	}

	return true
}
