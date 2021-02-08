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
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
)

func (v *vertex) linkAtLevel(level int, target uint64, cl CommitLogger) error {
	v.Lock()
	defer v.Unlock()

	if err := cl.AddLinkAtLevel(v.id, level, target); err != nil {
		return err
	}

	if targetContained(v.connections[level], target) {
		// already linked, nothing to do
		return nil
	}

	v.connections[level] = append(v.connections[level], target)
	return nil
}

func targetContained(haystack []uint64, needle uint64) bool {
	for _, candidate := range haystack {
		if candidate == needle {
			return true
		}
	}

	return false
}

func (h *hnsw) findAndConnectNeighbors(node *vertex,
	entryPointID uint64, nodeVec []float32, targetLevel, currentMaxLevel int,
	denyList helpers.AllowList) error {
	nfc := newNeighborFinderConnector(h, node, entryPointID, nodeVec, targetLevel,
		currentMaxLevel, denyList)

	return nfc.Do()
}

type neighborFinderConnector struct {
	graph           *hnsw
	node            *vertex
	entryPointID    uint64
	nodeVec         []float32
	targetLevel     int
	currentMaxLevel int
	denyList        helpers.AllowList
	bufLinksLog     BufferedLinksLogger
	results         *binarySearchTreeGeneric
}

func newNeighborFinderConnector(graph *hnsw, node *vertex, entryPointID uint64,
	nodeVec []float32, targetLevel, currentMaxLevel int,
	denyList helpers.AllowList) *neighborFinderConnector {
	return &neighborFinderConnector{
		graph:           graph,
		node:            node,
		entryPointID:    entryPointID,
		nodeVec:         nodeVec,
		targetLevel:     targetLevel,
		currentMaxLevel: currentMaxLevel,
		denyList:        denyList,
	}
}

func (n *neighborFinderConnector) Do() error {
	n.results = &binarySearchTreeGeneric{}
	n.bufLinksLog = n.graph.commitLog.NewBufferedLinksLogger()

	dist, ok, err := n.graph.distBetweenNodeAndVec(n.entryPointID, n.nodeVec)
	if err != nil {
		return errors.Wrapf(err, "calculate distance between insert node and final entrypoint")
	}
	if !ok {
		return fmt.Errorf("entrypoint was deleted in the object store, " +
			"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
	}

	n.results.insert(n.entryPointID, dist)
	// neighborsAtLevel := make(map[int][]uint32) // for distributed spike

	for level := min(n.targetLevel, n.currentMaxLevel); level >= 0; level-- {
		err := n.doAtLevel(level)
		if err != nil {
			return err
		}
	}

	return n.bufLinksLog.Close()
}

func (n *neighborFinderConnector) doAtLevel(level int) error {
	if err := n.replaceEntrypointsIfUnderMaintenance(); err != nil {
		return err
	}

	results, err := n.graph.searchLayerByVector(n.nodeVec, *n.results, n.graph.efConstruction,
		level, nil)
	if err != nil {
		return errors.Wrapf(err, "find neighbors: search layer at level %d", level)
	}

	n.removeSelfFromResults()

	neighbors := n.graph.selectNeighborsSimple(*results, n.graph.maximumConnections,
		n.denyList)

	// // for distributed spike
	// neighborsAtLevel[level] = neighbors

	for _, neighborID := range neighbors {
		if err := n.connectNeighborAtLevel(neighborID, level); err != nil {
			return err
		}
	}

	return nil
}

func (n *neighborFinderConnector) replaceEntrypointsIfUnderMaintenance() error {
	if n.node.isUnderMaintenance() {
		haveAlternative := false
		for i, ep := range n.results.flattenInOrder() {
			if haveAlternative {
				break
			}
			if i == 0 {
				continue
			}

			if !n.graph.nodeByID(ep.index).isUnderMaintenance() {
				haveAlternative = true
			}
		}

		if !haveAlternative {
			globalEP := n.graph.entryPointID
			dist, ok, err := n.graph.distBetweenNodeAndVec(globalEP, n.nodeVec)
			if err != nil {
				return errors.Wrapf(err, "calculate distance between insert node and final entrypoint")
			}
			if !ok {
				return fmt.Errorf("entrypoint was deleted in the object store, " +
					"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
			}
			n.results.insert(globalEP, dist)
		}
	}

	return nil
}

func (n *neighborFinderConnector) connectNeighborAtLevel(neighborID uint64,
	level int) error {
	neighbor := n.graph.nodeByID(neighborID)
	if skip := n.skipNeighbor(neighbor); skip {
		return nil
	}

	if err := neighbor.linkAtLevel(level, n.node.id, n.graph.commitLog); err != nil {
		return err
	}

	if err := n.node.linkAtLevel(level, neighbor.id, n.graph.commitLog); err != nil {
		return err
	}

	currentConnections := neighbor.connectionsAtLevel(level)
	maximumConnections := n.maximumConnections(level)
	if len(currentConnections) <= maximumConnections {
		// nothing to do, skip
		return nil
	}

	updatedConnections, err := n.graph.selectNeighborsSimpleFromId(n.node.id,
		currentConnections, maximumConnections, n.denyList)
	if err != nil {
		return errors.Wrap(err, "connect neighbors")
	}

	if err := n.bufLinksLog.ReplaceLinksAtLevel(neighbor.id, level,
		updatedConnections); err != nil {
		return err
	}

	neighbor.setConnectionsAtLevel(level, updatedConnections)
	return nil
}

func (n *neighborFinderConnector) skipNeighbor(neighbor *vertex) bool {
	if neighbor == n.node {
		// don't connect to self
		return true
	}

	if neighbor == nil || n.graph.hasTombstone(neighbor.id) {
		// don't connect to tombstoned nodes. This would only increase the
		// cleanup that needs to be done. Even worse: A tombstoned node can be
		// cleaned up at any time, also while we are connecting to it. So,
		// while the node still exists right now, it might already be nil in
		// the next line, which would lead to a nil-pointer panic.
		return true
	}

	return false
}

func (n *neighborFinderConnector) removeSelfFromResults() {
	if n.results.contains(n.node.id, 0) {
		// Make sure we don't get the node we're currently assigning on the
		// result list. This could lead to a self-link, but far worse it could
		// lead to using ourself as an entry point on the next lower level. In
		// the process of (re)-assigning edges it would be fatal to use ourselves
		// as an entrypoint, as there are only two possible scenarios: 1. This is
		// a new insert, so we don't have edges yet. 2. This is a re-assign after
		// a delete, so we did originally have edges, but they were cleared in
		// preparation for the re-assignment.
		//
		// So why is it so bad to have ourselves (without connections) as an
		// entrypoint? Because the exit condition in searchLayerByVector is if
		// the candidates distance is worse than the current worst distance.
		// Naturally, the node itself has the best distance (=0) to itself, so
		// we'd ignore all other elements. However, since the node - as outlined
		// before - has no nodes, the search wouldn't find any results. Thus we
		// also can't add any new connections, leading to an isolated node in the
		// grapn.graph. If that isolated node were to become the graphs entrypoint, the
		// graph is basically unusable.
		n.results.delete(n.node.id, 0)
	}
}

func (n *neighborFinderConnector) maximumConnections(level int) int {
	if level == 0 {
		return n.graph.maximumConnectionsLayerZero
	}

	return n.graph.maximumConnections
}
