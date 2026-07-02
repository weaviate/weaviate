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

import (
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
)

// resolvedEntrypoint contains the result of resolving a valid entrypoint for search.
type resolvedEntrypoint struct {
	id       uint64
	distance float32
}

// resolveEntrypoint attempts to get a valid entrypoint for search operations.
// It first tries the global entrypoint. If that fails with storobj.ErrNotFound
// (indicating the node was deleted), it triggers an async repair and finds a
// local fallback for the current query.
//
// Returns:
//   - resolved: contains the entrypoint ID and distance, or empty if graph is empty
//   - ok: true if a valid entrypoint was found (even via fallback)
//   - err: only for non-recoverable errors (not ErrNotFound)
func (h *hnsw) resolveEntrypoint(
	compressorDistancer compressionhelpers.CompressorDistancer,
	searchVec []float32,
) (resolved resolvedEntrypoint, ok bool, err error) {
	h.RLock()
	entryPointID := h.entryPointID
	h.RUnlock()

	// Try the global entrypoint first (fast path)
	entryPointDistance, err := h.distToNode(compressorDistancer, entryPointID, searchVec)
	if err == nil {
		// Global entrypoint is valid
		return resolvedEntrypoint{
			id:       entryPointID,
			distance: entryPointDistance,
		}, true, nil
	}

	// Check if error is ErrNotFound (deleted node)
	var e storobj.ErrNotFound
	if !errors.As(err, &e) {
		// Non-recoverable error
		return resolvedEntrypoint{}, false, err
	}

	// Global entrypoint is invalid - handle deleted node
	h.handleDeletedNode(e.DocID, "resolveEntrypoint")

	// Trigger async repair (single-flight via entrypointRepairPending)
	h.triggerAsyncEntrypointRepair(entryPointID)

	// Find a local fallback for THIS query
	fallbackID, fallbackDist, found := h.findFallbackEntrypoint(compressorDistancer, searchVec, entryPointID)
	if !found {
		// No valid nodes in the graph - return empty (not an error for searches)
		return resolvedEntrypoint{}, false, nil
	}

	return resolvedEntrypoint{
		id:       fallbackID,
		distance: fallbackDist,
	}, true, nil
}

// triggerAsyncEntrypointRepair spawns a background goroutine to repair the global
// entrypoint. Uses entrypointRepairPending atomic as a labeled optimization to
// prevent goroutine accumulation when many concurrent searches hit the same
// invalid entrypoint. The CAS inside repairGlobalEntrypoint provides correctness.
func (h *hnsw) triggerAsyncEntrypointRepair(oldEntrypoint uint64) {
	// Single-flight optimization: only one repair goroutine at a time
	if !h.entrypointRepairPending.CompareAndSwap(false, true) {
		return
	}

	enterrors.GoWrapper(func() {
		defer h.entrypointRepairPending.Store(false)

		// Use tombstones as deny-list to prevent selecting an already-tombstoned node.
		// findNewGlobalEntrypoint does NOT filter tombstones itself, so without this
		// the repair could pick a tombstoned node, leading to repeated fallback scans.
		denyList := h.tombstonesAsDenyList()
		_, err := h.repairGlobalEntrypoint(oldEntrypoint, denyList)
		if err != nil {
			h.logger.WithField("action", "async_entrypoint_repair").
				WithField("old_entrypoint", oldEntrypoint).
				WithError(err).
				Warn("failed to repair entrypoint")
		}
	}, h.logger)
}

// findFallbackEntrypoint scans all nodes to find a valid node for the current
// search query. This is a read-only local fallback that doesn't modify global
// state. The scan is unbounded to guarantee correctness: if ANY valid node
// exists, it will be found. This expensive scan only occurs until the async
// repair completes, after which subsequent queries use the fast path.
//
// Returns:
//   - id: the fallback node ID
//   - distance: distance from searchVec to the fallback node
//   - found: true if a valid fallback was found
func (h *hnsw) findFallbackEntrypoint(
	compressorDistancer compressionhelpers.CompressorDistancer,
	searchVec []float32,
	skipID uint64,
) (id uint64, distance float32, found bool) {
	h.RLock()
	maxNodes := len(h.nodes)
	h.RUnlock()

	if maxNodes == 0 {
		return 0, 0, false
	}

	for i := 0; i < maxNodes; i++ {
		nodeID := uint64(i)
		if nodeID == skipID {
			continue
		}

		// Use nodeByID for consistency with other node access patterns.
		// Note: The original shardedNodeLocks.RLock(nodeID) was also safe because
		// the write path (growIndexToAccomodateNode) reassigns h.nodes under
		// shardedNodeLocks.LockAll(), which is mutually exclusive with RLock(nodeID),
		// establishing a happens-before relationship. nodeByID is clearer.
		node := h.nodeByID(nodeID)
		if node == nil {
			continue
		}

		if node.isUnderMaintenance() {
			continue
		}

		// Try to compute distance to this node
		dist, err := h.distToNode(compressorDistancer, nodeID, searchVec)
		if err != nil {
			// Node might be deleted or have other issues, skip it
			continue
		}

		// Found a valid fallback
		return nodeID, dist, true
	}

	return 0, 0, false
}
