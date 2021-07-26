//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

import (
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
)

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
	entryPointDist  float32
	nodeVec         []float32
	targetLevel     int
	currentMaxLevel int
	denyList        helpers.AllowList
	// bufLinksLog     BufferedLinksLogger
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
	dist, ok, err := n.graph.distBetweenNodeAndVec(n.entryPointID, n.nodeVec)
	if err != nil {
		return errors.Wrapf(err, "calculate distance between insert node and final entrypoint")
	}
	if !ok {
		return errors.Errorf("entrypoint was deleted in the object store, " +
			"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
	}

	n.entryPointDist = dist

	for level := min(n.targetLevel, n.currentMaxLevel); level >= 0; level-- {
		err := n.doAtLevel(level)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *neighborFinderConnector) doAtLevel(level int) error {
	if err := n.replaceEntrypointsIfUnderMaintenance(); err != nil {
		return err
	}

	eps := priorityqueue.NewMin(1)
	eps.Insert(n.entryPointID, n.entryPointDist)

	results, err := n.graph.searchLayerByVector(n.nodeVec, eps, n.graph.efConstruction,
		level, nil)
	if err != nil {
		return errors.Wrapf(err, "find neighbors: search layer at level %d", level)
	}

	// max := n.maximumConnections(level)
	max := n.graph.maximumConnections
	if err := n.graph.selectNeighborsHeuristic(results, max, n.denyList); err != nil {
		return err
	}

	// // for distributed spike
	// neighborsAtLevel[level] = neighbors

	neighbors := make([]uint64, 0, results.Len())
	for results.Len() > 0 {
		id := results.Pop().ID
		neighbors = append(neighbors, id)
	}

	n.graph.pools.pqResults.Put(results)

	// set all outoing in one go
	n.node.setConnectionsAtLevel(level, neighbors)
	n.graph.commitLog.ReplaceLinksAtLevel(n.node.id, level, neighbors)

	for _, neighborID := range neighbors {
		if err := n.connectNeighborAtLevel(neighborID, level); err != nil {
			return err
		}
	}

	if len(neighbors) > 0 {
		// there could be no neighbors left, if all are marked deleted, in this
		// case, don't change the entrypoint
		n.entryPointID = neighbors[len(neighbors)-1]
		dist, ok, err := n.graph.distBetweenNodeAndVec(n.entryPointID, n.nodeVec)
		if err != nil {
			return errors.Wrapf(err, "calculate distance between insert node and final entrypoint")
		}
		if !ok {
			return errors.Errorf("entrypoint was deleted in the object store, " +
				"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
		}

		n.entryPointDist = dist
	}

	return nil
}

func (n *neighborFinderConnector) replaceEntrypointsIfUnderMaintenance() error {
	node := n.graph.nodeByID(n.entryPointID)
	if node.isUnderMaintenance() {
		alternativeEP := n.graph.entryPointID
		if alternativeEP == n.node.id || alternativeEP == n.entryPointID {
			tmpDenyList := n.denyList.DeepCopy()
			tmpDenyList.Insert(alternativeEP)

			alternative, _ := n.graph.findNewLocalEntrypoint(tmpDenyList, n.graph.currentMaximumLayer,
				n.entryPointID)
			alternativeEP = alternative
		}
		dist, ok, err := n.graph.distBetweenNodeAndVec(alternativeEP, n.nodeVec)
		if err != nil {
			return errors.Wrapf(err, "calculate distance between insert node and final entrypoint")
		}
		if !ok {
			return errors.Errorf("entrypoint was deleted in the object store, " +
				"it has been flagged for cleanup and should be fixed in the next cleanup cycle")
		}
		n.entryPointID = alternativeEP
		n.entryPointDist = dist
	}

	return nil
}

func (n *neighborFinderConnector) connectNeighborAtLevel(neighborID uint64,
	level int) error {
	neighbor := n.graph.nodeByID(neighborID)
	if skip := n.skipNeighbor(neighbor); skip {
		return nil
	}

	neighbor.Lock()
	defer neighbor.Unlock()
	currentConnections := neighbor.connectionsAtLevelNoLock(level)

	maximumConnections := n.maximumConnections(level)
	if len(currentConnections) < maximumConnections {
		// we can simply append
		// updatedConnections = append(currentConnections, n.node.id)
		neighbor.appendConnectionAtLevelNoLock(level, n.node.id)
		if err := n.graph.commitLog.AddLinkAtLevel(neighbor.id, level, n.node.id); err != nil {
			return err
		}
	} else {
		// we need to run the heurisitc

		dist, ok, err := n.graph.distBetweenNodes(n.node.id, neighborID)
		if err != nil {
			return errors.Wrapf(err, "dist between %d and %d", n.node.id, neighborID)
		}

		if !ok {
			// it seems either the node or the neighbor were deleted in the meantime,
			// there is nothing we can do now
			return nil
		}

		candidates := priorityqueue.NewMax(len(currentConnections) + 1)
		candidates.Insert(n.node.id, dist)

		for _, existingConnection := range currentConnections {
			dist, ok, err := n.graph.distBetweenNodes(existingConnection, neighborID)
			if err != nil {
				return errors.Wrapf(err, "dist between %d and %d", existingConnection, neighborID)
			}

			if !ok {
				// was deleted in the meantime
				continue
			}

			candidates.Insert(existingConnection, dist)
		}

		err = n.graph.selectNeighborsHeuristic(candidates, maximumConnections, n.denyList)
		if err != nil {
			return errors.Wrap(err, "connect neighbors")
		}

		neighbor.resetConnectionsAtLevelNoLock(level)
		if err := n.graph.commitLog.ClearLinks(neighbor.id); err != nil {
			return err
		}

		for candidates.Len() > 0 {
			id := candidates.Pop().ID
			neighbor.appendConnectionAtLevelNoLock(level, id)
			if err := n.graph.commitLog.AddLinkAtLevel(neighbor.id, level, id); err != nil {
				return err
			}
		}
	}

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

func (n *neighborFinderConnector) maximumConnections(level int) int {
	if level == 0 {
		return n.graph.maximumConnectionsLayerZero
	}

	return n.graph.maximumConnections
}
