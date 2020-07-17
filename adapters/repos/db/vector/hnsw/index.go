package hnsw

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/pkg/errors"
)

const importLimit = 10000 // TODO: make variable

type hnsw struct {
	sync.RWMutex

	// Each node should not have more edges than this number
	maximumConnections int

	// Nodes in the lowest level have a separate (usually higher) max connection
	// limit
	maximumConnectionsLayerZero int

	// the current maximum can be smaller than the configured maximum because of
	// the exponentially decaying layer function. The initial entry is started at
	// layer 0, but this has the chance to grow with every subsequent entry
	currentMaximumLayer int

	// this is a point on the highest level, if we insert a new point with a
	// higher level it will become the new entry point. Note tat the level of
	// this point is always currentMaximumLayer
	entryPointID int

	// ef parameter used in construction phases, should be higher than ef during querying
	efConstruction int

	levelNormalizer float64

	nodes []*hnswVertex

	vectorForID vectorForID

	commitLog *hnswCommitLogger

	// // for distributed spike, can be used to call a insertExternal on a different graph
	// insertHook func(node, targetLevel int, neighborsAtLevel map[int][]uint32)

	id string
}

type vectorForID func(ctx context.Context, id int32) ([]float32, error)

func New(rootPath, id string, maximumConnections int, efConstruction int, vectorForID vectorForID) *hnsw {
	return &hnsw{
		maximumConnections:          maximumConnections,
		maximumConnectionsLayerZero: 2 * maximumConnections,                    // inspired by original paper and other implementations
		levelNormalizer:             1 / math.Log(float64(maximumConnections)), // inspired by c++ implementation
		efConstruction:              efConstruction,
		nodes:                       make([]*hnswVertex, 0, importLimit), // TODO: grow variably rather than fixed length
		vectorForID:                 vectorForID,
		commitLog:                   newHnswCommitLogger(rootPath, id),
		id:                          id,
	}

}

// TODO: use this for incoming replication
// func (h *hnsw) insertFromExternal(nodeId, targetLevel int, neighborsAtLevel map[int][]uint32) {
// 	defer m.addBuildingReplication(time.Now())

// 	// randomly introduce up to 50ms delay to account for network slowness
// 	time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

// 	var node *hnswVertex
// 	h.RLock()
// 	total := len(h.nodes)
// 	if total > nodeId {
// 		node = h.nodes[nodeId] // it could be that we implicitly added this node already because it was referenced
// 	}
// 	h.RUnlock()

// 	if node == nil {
// 		node = &hnswVertex{
// 			id:          nodeId,
// 			connections: make(map[int][]uint32),
// 			level:       targetLevel,
// 		}
// 	} else {
// 		node.level = targetLevel
// 	}

// 	if total == 0 {
// 		h.Lock()
// 		h.commitLog.SetEntryPointWithMaxLayer(node.id, 0)
// 		h.entryPointID = node.id
// 		h.currentMaximumLayer = 0
// 		node.connections = map[int][]uint32{}
// 		node.level = 0
// 		// h.nodes = make([]*hnswVertex, 100000)
// 		h.commitLog.AddNode(node)
// 		h.nodes[node.id] = node
// 		h.Unlock()
// 		return
// 	}

// 	currentMaximumLayer := h.currentMaximumLayer
// 	h.Lock()
// 	h.nodes[nodeId] = node
// 	h.commitLog.AddNode(node)
// 	h.Unlock()

// 	for level := min(targetLevel, currentMaximumLayer); level >= 0; level-- {
// 		neighbors := neighborsAtLevel[level]

// 		for _, neighborID := range neighbors {
// 			h.RLock()
// 			neighbor := h.nodes[neighborID]
// 			if neighbor == nil {
// 				// due to everything being parallel it could be that the linked neighbor
// 				// doesn't exist yet
// 				h.nodes[neighborID] = &hnswVertex{
// 					id:          int(neighborID),
// 					connections: make(map[int][]uint32),
// 				}
// 				neighbor = h.nodes[neighborID]
// 			}
// 			h.RUnlock()

// 			neighbor.linkAtLevel(level, uint32(nodeId), h.commitLog)
// 			node.linkAtLevel(level, uint32(neighbor.id), h.commitLog)

// 			neighbor.RLock()
// 			currentConnections := neighbor.connections[level]
// 			neighbor.RUnlock()

// 			maximumConnections := h.maximumConnections
// 			if level == 0 {
// 				maximumConnections = h.maximumConnectionsLayerZero
// 			}

// 			if len(currentConnections) <= maximumConnections {
// 				// nothing to do, skip
// 				continue
// 			}

// 			// TODO: support both neighbor selection algos
// 			updatedConnections := h.selectNeighborsSimpleFromId(nodeId, currentConnections, maximumConnections)

// 			neighbor.Lock()
// 			h.commitLog.ReplaceLinksAtLevel(neighbor.id, level, updatedConnections)
// 			neighbor.connections[level] = updatedConnections
// 			neighbor.Unlock()
// 		}
// 	}

// 	if targetLevel > h.currentMaximumLayer {
// 		h.Lock()
// 		h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel)
// 		h.entryPointID = nodeId
// 		h.currentMaximumLayer = targetLevel
// 		h.Unlock()
// 	}

// }

func (h *hnsw) Add(id int, vector []float32) error {
	// TODO: can we do anything meaningful with the vector to save another
	// lookup?
	node := &hnswVertex{
		id: id,
	}

	return h.insert(node)
}

func (h *hnsw) insert(node *hnswVertex) error {
	// before := time.Now()
	h.RLock()
	// m.addBuildingReadLockingBeginning(before)
	total := len(h.nodes)
	h.RUnlock()

	if total == 0 {
		h.Lock()
		h.commitLog.SetEntryPointWithMaxLayer(node.id, 0)
		h.entryPointID = node.id
		h.currentMaximumLayer = 0
		node.connections = map[int][]uint32{}
		node.level = 0
		h.nodes = make([]*hnswVertex, importLimit)
		h.commitLog.AddNode(node)
		h.nodes[node.id] = node
		h.Unlock()

		// go h.insertHook(node.id, 0, node.connections)
		return nil
	}
	// initially use the "global" entrypoint which is guaranteed to be on the
	// currently highest layer
	entryPointID := h.entryPointID

	// initially use the level of the entrypoint which is the highest level of
	// the h-graph in the first iteration
	currentMaximumLayer := h.currentMaximumLayer

	targetLevel := int(math.Floor(-math.Log(rand.Float64()*h.levelNormalizer))) - 1

	// before = time.Now()
	node.Lock()
	// m.addBuildingItemLocking(before)
	node.level = targetLevel
	node.connections = map[int][]uint32{}
	node.Unlock()

	// before = time.Now()
	h.Lock()
	// m.addBuildingLocking(before)
	nodeId := node.id
	h.nodes[nodeId] = node
	h.commitLog.AddNode(node)
	h.Unlock()

	// in case the new target is lower than the current max, we need to search
	// each layer for a better candidate and update the candidate
	for level := currentMaximumLayer; level > targetLevel; level-- {
		tmpBST := &binarySearchTreeGeneric{}
		dist, err := h.distBetweenNodes(nodeId, entryPointID)
		if err != nil {
			return errors.Wrapf(err, "calculate distance between insert node and entry point at level %d", level)
		}
		tmpBST.insert(entryPointID, dist)
		res, err := h.searchLayer(node, *tmpBST, 1, level)
		if err != nil {
			return errors.Wrapf(err, "update candidate: search layer at level %d", level)
		}
		entryPointID = res.minimum().index
	}

	var results = &binarySearchTreeGeneric{}
	dist, err := h.distBetweenNodes(nodeId, entryPointID)
	if err != nil {
		return errors.Wrapf(err, "calculate distance between insert node and final entrypoint")
	}
	results.insert(entryPointID, dist)

	neighborsAtLevel := make(map[int][]uint32) // for distributed spike

	for level := min(targetLevel, currentMaximumLayer); level >= 0; level-- {
		results, err = h.searchLayer(node, *results, h.efConstruction, level)
		if err != nil {
			return errors.Wrapf(err, "find neighbors: search layer at level %d", level)
		}

		// TODO: support both neighbor selection algos
		neighbors := h.selectNeighborsSimple(nodeId, *results, h.maximumConnections)

		// for distributed spike
		neighborsAtLevel[level] = neighbors

		for _, neighborID := range neighbors {
			// before := time.Now()
			h.RLock()
			// m.addBuildingReadLocking(before)
			neighbor := h.nodes[neighborID]
			h.RUnlock()

			neighbor.linkAtLevel(level, uint32(nodeId), h.commitLog)
			node.linkAtLevel(level, uint32(neighbor.id), h.commitLog)

			// before = time.Now()
			neighbor.RLock()
			// m.addBuildingItemLocking(before)
			currentConnections := neighbor.connections[level]
			neighbor.RUnlock()

			maximumConnections := h.maximumConnections
			if level == 0 {
				maximumConnections = h.maximumConnectionsLayerZero
			}

			if len(currentConnections) <= maximumConnections {
				// nothing to do, skip
				continue
			}

			// TODO: support both neighbor selection algos
			updatedConnections, err := h.selectNeighborsSimpleFromId(nodeId, currentConnections, maximumConnections)
			if err != nil {
				return errors.Wrap(err, "connect neighbors")
			}

			// before = time.Now()
			neighbor.Lock()
			// m.addBuildingItemLocking(before)
			h.commitLog.ReplaceLinksAtLevel(neighbor.id, level, updatedConnections)
			neighbor.connections[level] = updatedConnections
			neighbor.Unlock()
		}
	}

	// go h.insertHook(nodeId, targetLevel, neighborsAtLevel)

	if targetLevel > h.currentMaximumLayer {
		// before = time.Now()
		h.Lock()
		// m.addBuildingLocking(before)
		h.commitLog.SetEntryPointWithMaxLayer(nodeId, targetLevel)
		h.entryPointID = nodeId
		h.currentMaximumLayer = targetLevel
		h.Unlock()
	}

	return nil
}

func (v *hnswVertex) linkAtLevel(level int, target uint32, cl *hnswCommitLogger) {
	v.Lock()
	cl.AddLinkAtLevel(v.id, level, target)
	v.connections[level] = append(v.connections[level], target)
	v.Unlock()
}

type hnswVertex struct {
	id int
	sync.RWMutex
	level       int
	connections map[int][]uint32 // map[level][]connectedId
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (h *hnsw) distBetweenNodes(a, b int) (float32, error) {
	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	vecA, err := h.vectorForID(context.Background(), int32(a))
	if err != nil {
		return 0, errors.Wrapf(err, "could not get vector of object at docID %d", a)
	}

	if vecA == nil || len(vecA) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector at docID %d", a)
	}

	vecB, err := h.vectorForID(context.Background(), int32(b))
	if err != nil {
		return 0, errors.Wrapf(err, "could not get vector of object at docID %d", b)
	}

	if vecB == nil || len(vecB) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector at docID %d", b)
	}

	return cosineDist(vecA, vecB)
}

func (h *hnsw) distBetweenNodeAndVec(node int, vecB []float32) (float32, error) {
	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	vecA, err := h.vectorForID(context.Background(), int32(node))
	if err != nil {
		return 0, errors.Wrapf(err, "could not get vector of object at docID %d", node)
	}

	if vecA == nil || len(vecA) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector at docID %d", node)
	}

	if vecB == nil || len(vecB) == 0 {
		return 0, fmt.Errorf("got a nil or zero-length vector as search vector")
	}

	return cosineDist(vecA, vecB)
}

func (h *hnsw) Stats() {
	fmt.Printf("levels: %d\n", h.currentMaximumLayer)

	perLevelCount := map[int]uint{}

	for _, node := range h.nodes {
		if node == nil {
			continue
		}
		l := node.level
		if l == 0 && len(node.connections) == 0 {
			// filter out allocated space without nodes
			continue
		}
		c, ok := perLevelCount[l]
		if !ok {
			perLevelCount[l] = 0
		}

		perLevelCount[l] = c + 1
	}

	for level, count := range perLevelCount {
		fmt.Printf("unique count on level %d: %d\n", level, count)
	}

}
