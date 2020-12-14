//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
)

// Delete attaches a tombstone to an item so it can be periodically cleaned up
// later and the edges reassigned
func (h *hnsw) Delete(id uint64) error {
	h.deleteLock.Lock()
	defer h.deleteLock.Unlock()

	h.addTombstone(id)

	// Adding a tombstone might not be enough in some cases, if the tombstoned
	// entry was the entrypoint this might lead to issues for following inserts:
	// On a nearly empty graph the entrypoint might be the only viable element to
	// connect to, however, because the entrypoint itself is tombstones
	// connections to it are impossible. So, unless we find a new entrypoint,
	// subsequent inserts might end up isolated (without edges) in the graph.
	// This is especially true if the tombstoned entrypoint is the only node in
	// the graph. In this case we must reset the graph, so it acts like an empty
	// one. Otherwise we'd insert the next id and have only one possible node to
	// connect it to (the entrypoint). With that one being tombstoned, the new
	// node would be guaranteed to have zero edges
	denyList := h.tombstonesAsDenyList()

	node := h.nodeByID(id)
	if node == nil {
		// node was already deleted/cleaned up
		return nil
	}

	if h.getEntrypoint() == id {
		if h.isOnlyNode(node, denyList) {
			h.reset()
		} else {
			if err := h.deleteEntrypoint(node, denyList); err != nil {
				return errors.Wrap(err, "delete entrypoint")
			}
		}
	}
	return nil
}

func (h *hnsw) reset() {
	h.Lock()
	defer h.Unlock()
	h.entryPointID = 0
	h.currentMaximumLayer = 0
	h.nodes = make([]*vertex, initialSize)
	h.commitLog.Reset()
}

func (h *hnsw) tombstonesAsDenyList() helpers.AllowList {
	deleteList := helpers.AllowList{}
	h.RLock()
	defer h.RUnlock()

	tombstones := h.tombstones
	for id := range tombstones {
		deleteList.Insert(id)
	}

	return deleteList
}

func (h *hnsw) getEntrypoint() uint64 {
	h.RLock()
	defer h.RUnlock()

	return h.entryPointID
}

func (h *hnsw) copyTombstonesToAllowList() helpers.AllowList {
	h.RLock()
	defer h.RUnlock()

	deleteList := helpers.AllowList{}
	lenOfNodes := uint64(len(h.nodes))

	for id := range h.tombstones {
		if lenOfNodes <= id {
			// we're trying to delete an id outside the possible range, nothing to do
			continue
		}

		deleteList.Insert(id)
	}

	return deleteList
}

// CleanUpTombstonedNodes removes nodes with a tombstone and reassignes edges
// that were previously pointing to the tombstoned nodes
func (h *hnsw) CleanUpTombstonedNodes() error {
	deleteList := h.copyTombstonesToAllowList()
	if len(deleteList) == 0 {
		return nil
	}

	if err := h.reassignNeighborsOf(deleteList); err != nil {
		return errors.Wrap(err, "reassign neighbor edges")
	}

	for id := range deleteList {
		if h.getEntrypoint() == id {
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

	for id := range deleteList {
		h.Lock()
		h.nodes[id] = nil
		delete(h.tombstones, id)
		h.Unlock()
		h.commitLog.DeleteNode(id)
		h.commitLog.RemoveTombstone(id)
	}

	if h.isEmpty() {
		h.Lock()
		h.entryPointID = 0
		h.currentMaximumLayer = 0
		h.Unlock()
		h.commitLog.Reset()
	}

	return nil
}

func (h *hnsw) reassignNeighborsOf(deleteList helpers.AllowList) error {
	h.RLock()
	size := len(h.nodes)
	currentEntrypoint := h.entryPointID
	h.RUnlock()

	for n := 0; n < size; n++ {
		neighbor := uint64(n)
		h.RLock()
		neighborNode := h.nodes[neighbor]
		h.RUnlock()

		if neighborNode == nil || deleteList.Contains(neighborNode.id) {
			continue
		}

		neighborVec, err := h.vectorForID(context.Background(), neighbor)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID)
				continue
			} else {
				// not a typed error, we can recover from, return with err
				return errors.Wrap(err, "get neighbor vec")
			}
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
				neighborNode.connections = map[int][]uint64{}
				neighborNode.Unlock()
				h.commitLog.ClearLinks(neighbor)
				continue
			}

			tmpDenyList := deleteList.DeepCopy()
			tmpDenyList.Insert(entryPointID)
			alternative, _, _ := h.findNewEntrypoint(tmpDenyList, h.currentMaximumLayer,
				entryPointID)
			entryPointID = alternative
		}

		neighborNode.Lock()
		// delete all existing connections before re-assigning
		neighborNode.connections = map[int][]uint64{}
		neighborNode.Unlock()
		h.commitLog.ClearLinks(neighbor)

		if err := h.findAndConnectNeighbors(neighborNode, entryPointID, neighborVec,
			neighborLevel, h.currentMaximumLayer, deleteList); err != nil {
			return errors.Wrap(err, "find and connect neighbors")
		}
	}

	return nil
}

func connectionsPointTo(connections map[int][]uint64, needles helpers.AllowList) bool {
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
func (h *hnsw) deleteEntrypoint(node *vertex, denyList helpers.AllowList) error {
	if h.isOnlyNode(node, denyList) {
		// no point in finding another entrypoint if this is the only node
		return nil
	}

	node.Lock()
	level := node.level
	id := node.id
	node.Unlock()

	newEntrypoint, level, ok := h.findNewEntrypoint(denyList, level, id)
	if !ok {
		return nil
	}

	h.Lock()
	h.entryPointID = newEntrypoint
	h.currentMaximumLayer = level
	h.Unlock()
	h.commitLog.SetEntryPointWithMaxLayer(newEntrypoint, level)

	return nil
}

// returns entryPointID, level
func (h *hnsw) findNewEntrypoint(denyList helpers.AllowList, targetLevel int,
	oldEntrypoint uint64) (uint64, int, bool) {
	if h.getEntrypoint() != oldEntrypoint {
		// entrypoint has already been changed (this could be due to a new import
		// for example, nothing to do for us
		return 0, 0, false
	}

	for l := targetLevel; l >= 0; l-- {
		// ideally we can find a new entrypoint at the same level of the
		// to-be-deleted node. However, there is a chance it was the only node on
		// that level, in that case we need to look at the next lower level for a
		// better candidate

		h.RLock()
		maxNodes := len(h.nodes)
		h.RUnlock()

		for i := 0; i < maxNodes; i++ {
			if h.getEntrypoint() != oldEntrypoint {
				// entrypoint has already been changed (this could be due to a new import
				// for example, nothing to do for us
				return 0, 0, false
			}

			if denyList.Contains(uint64(i)) {
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
			return uint64(i), l, true
		}
	}

	// we made it thorugh the entire graph and didn't find a new entrypoint all
	// the way down to level 0. This can only mean the graph is empty, which is
	// unexpected. This situation should have been prevented by the deleteLock.
	panic("findNewEntrypoint called on an empty hnsw graph")
}

func (h *hnsw) isOnlyNode(needle *vertex, denyList helpers.AllowList) bool {
	h.RLock()
	defer h.RUnlock()

	for _, node := range h.nodes {
		if node == nil || node.id == needle.id || denyList.Contains(node.id) {
			continue
		}

		return false
	}

	return true
}

func (h *hnsw) hasTombstone(id uint64) bool {
	h.RLock()
	defer h.RUnlock()
	_, ok := h.tombstones[id]
	return ok
}

func (h *hnsw) addTombstone(id uint64) {
	h.Lock()
	h.tombstones[id] = struct{}{}
	h.Unlock()
	h.commitLog.AddTombstone(id)
}
