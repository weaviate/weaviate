//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"
	ssdhelpers "github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type hnsw struct {
	// global lock to prevent concurrent map read/write, etc.
	sync.RWMutex

	// certain operations related to deleting, such as finding a new entrypoint
	// can only run sequentially, this separate lock helps assuring this without
	// blocking the general usage of the hnsw index
	deleteLock *sync.Mutex

	tombstoneLock *sync.RWMutex

	// prevents tombstones cleanup to be performed in parallel with index reset operation
	resetLock *sync.Mutex
	// indicates whether reset operation occurred or not - if so tombstones cleanup method
	// is aborted as it makes no sense anymore
	resetCtx       context.Context
	resetCtxCancel context.CancelFunc

	// make sure the very first insert happens just once, otherwise we
	// accidentally overwrite previous entrypoints on parallel imports on an
	// empty graph
	initialInsertOnce *sync.Once

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
	entryPointID uint64

	// ef parameter used in construction phases, should be higher than ef during querying
	efConstruction int

	// ef at search time
	ef int64

	// only used if ef=-1
	efMin    int64
	efMax    int64
	efFactor int64

	// on filtered searches with less than n elements, perform flat search
	flatSearchCutoff int64

	levelNormalizer float64

	nodes []*vertex

	vectorForID      VectorForID
	multiVectorForID MultiVectorForID

	cache cache[float32]

	commitLog CommitLogger

	// a lookup of current tombstones (i.e. nodes that have received a tombstone,
	// but have not been cleaned up yet) Cleanup is the process of removal of all
	// outgoing edges to the tombstone as well as deleting the tombstone itself.
	// This process should happen periodically.
	tombstones map[uint64]struct{}

	// used for cancellation of the tombstone cleanup goroutine
	cleanupInterval       time.Duration
	tombstoneCleanupCycle *cyclemanager.CycleManager

	// // for distributed spike, can be used to call a insertExternal on a different graph
	// insertHook func(node, targetLevel int, neighborsAtLevel map[int][]uint32)

	id       string
	rootPath string

	logger            logrus.FieldLogger
	distancerProvider distancer.Provider

	pools *pools

	forbidFlat bool // mostly used in testing scenarios where we want to use the index even in scenarios where we typically wouldn't

	metrics       *Metrics
	insertMetrics *insertMetrics

	randFunc func() float64 // added to temporarily get rid on flakiness in tombstones related tests. to be removed after fixing WEAVIATE-179

	// The deleteVsInsertLock makes sure that there are no concurrent delete and
	// insert operations happening. It uses an RW-Mutex with:
	//
	// RLock -> Insert operations, this means any number of import operations can
	// happen concurrently.
	//
	// Lock -> Delete operation. This means only a single delete operation can
	// occur at a time, no insert operation can occur simultaneously with a
	// delete. Since the delete is cheap (just marking the node as deleted), the
	// single-threadedness of deletes is not a big problem.
	//
	// This lock was introduced as part of
	// https://github.com/weaviate/weaviate/issues/2194
	//
	// See
	// https://github.com/weaviate/weaviate/pull/2191#issuecomment-1242726787
	// where we ran performance tests to make sure introducing this lock has no
	// negative impact on performance.
	deleteVsInsertLock sync.RWMutex

	compressed             atomic.Bool
	pq                     *ssdhelpers.ProductQuantizer
	compressedVectorsCache cache[byte]
	compressedStore        *lsmkv.Store
	compressActionLock     *sync.RWMutex
	className              string
	shardName              string
}

type CommitLogger interface {
	ID() string
	Start()
	AddNode(node *vertex) error
	SetEntryPointWithMaxLayer(id uint64, level int) error
	AddLinkAtLevel(nodeid uint64, level int, target uint64) error
	ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error
	AddTombstone(nodeid uint64) error
	RemoveTombstone(nodeid uint64) error
	DeleteNode(nodeid uint64) error
	ClearLinks(nodeid uint64) error
	ClearLinksAtLevel(nodeid uint64, level uint16) error
	Reset() error
	Drop(ctx context.Context) error
	Flush() error
	Shutdown(ctx context.Context) error
	RootPath() string
	SwitchCommitLogs(bool) error
	MaintenanceInProgress() bool
	AddPQ(ssdhelpers.PQData) error
}

type BufferedLinksLogger interface {
	AddLinkAtLevel(nodeid uint64, level int, target uint64) error
	ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error
	Close() error // Close should Flush and Close
}

type MakeCommitLogger func() (CommitLogger, error)

type (
	VectorForID      func(ctx context.Context, id uint64) ([]float32, error)
	MultiVectorForID func(ctx context.Context, ids []uint64) ([][]float32, []error)
)

// New creates a new HNSW index, the commit logger is provided through a thunk
// (a function which can be deferred). This is because creating a commit logger
// opens files for writing. However, checking whether a file is present, is a
// criterium for the index to see if it has to recover from disk or if its a
// truly new index. So instead the index is initialized, with un-biased disk
// checks first and only then is the commit logger created
func New(cfg Config, uc ent.UserConfig) (*hnsw, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if cfg.Logger == nil {
		logger := logrus.New()
		logger.Out = io.Discard
		cfg.Logger = logger
	}

	normalizeOnRead := false
	if cfg.DistanceProvider.Type() == "cosine-dot" {
		normalizeOnRead = true
	}

	vectorCache := newShardedLockCache(cfg.VectorForIDThunk, uc.VectorCacheMaxObjects,
		cfg.Logger, normalizeOnRead, defaultDeletionInterval)

	compressedVectorsCache := newCompressedShardedLockCache(uc.VectorCacheMaxObjects, cfg.Logger)
	resetCtx, resetCtxCancel := context.WithCancel(context.Background())
	index := &hnsw{
		maximumConnections: uc.MaxConnections,

		// inspired by original paper and other implementations
		maximumConnectionsLayerZero: 2 * uc.MaxConnections,

		// inspired by c++ implementation
		levelNormalizer:        1 / math.Log(float64(uc.MaxConnections)),
		efConstruction:         uc.EFConstruction,
		flatSearchCutoff:       int64(uc.FlatSearchCutoff),
		nodes:                  make([]*vertex, initialSize),
		cache:                  vectorCache,
		vectorForID:            vectorCache.get,
		multiVectorForID:       vectorCache.multiGet,
		compressedVectorsCache: compressedVectorsCache,
		id:                     cfg.ID,
		rootPath:               cfg.RootPath,
		tombstones:             map[uint64]struct{}{},
		logger:                 cfg.Logger,
		distancerProvider:      cfg.DistanceProvider,
		deleteLock:             &sync.Mutex{},
		tombstoneLock:          &sync.RWMutex{},
		resetLock:              &sync.Mutex{},
		resetCtx:               resetCtx,
		resetCtxCancel:         resetCtxCancel,
		initialInsertOnce:      &sync.Once{},
		cleanupInterval:        time.Duration(uc.CleanupIntervalSeconds) * time.Second,

		ef:       int64(uc.EF),
		efMin:    int64(uc.DynamicEFMin),
		efMax:    int64(uc.DynamicEFMax),
		efFactor: int64(uc.DynamicEFFactor),

		metrics:   NewMetrics(cfg.PrometheusMetrics, cfg.ClassName, cfg.ShardName),
		shardName: cfg.ShardName,

		randFunc:           rand.Float64,
		compressActionLock: &sync.RWMutex{},
		className:          cfg.ClassName,
	}

	index.tombstoneCleanupCycle = cyclemanager.New(
		cyclemanager.NewFixedIntervalTicker(index.cleanupInterval),
		index.tombstoneCleanup)
	index.insertMetrics = newInsertMetrics(index.metrics)

	if err := index.init(cfg); err != nil {
		return nil, errors.Wrapf(err, "init index %q", index.id)
	}

	return index, nil
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

func (h *hnsw) findBestEntrypointForNode(currentMaxLevel, targetLevel int,
	entryPointID uint64, nodeVec []float32,
) (uint64, error) {
	// in case the new target is lower than the current max, we need to search
	// each layer for a better candidate and update the candidate
	for level := currentMaxLevel; level > targetLevel; level-- {
		eps := priorityqueue.NewMin(1)
		dist, ok, err := h.distBetweenNodeAndVec(entryPointID, nodeVec)
		if err != nil {
			return 0, errors.Wrapf(err,
				"calculate distance between insert node and entry point at level %d", level)
		}
		if !ok {
			continue
		}

		eps.Insert(entryPointID, dist)
		res, err := h.searchLayerByVector(nodeVec, eps, 1, level, nil)
		if err != nil {
			return 0,
				errors.Wrapf(err, "update candidate: search layer at level %d", level)
		}
		if res.Len() > 0 {
			// if we could find a new entrypoint, use it
			// in case everything was tombstoned, stick with the existing one
			elem := res.Pop()
			n := h.nodeByID(elem.ID)
			if n != nil && !n.isUnderMaintenance() {
				// but not if the entrypoint is under maintenance
				entryPointID = elem.ID
			}
		}

		h.pools.pqResults.Put(res)
	}

	return entryPointID, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (h *hnsw) distBetweenNodes(a, b uint64) (float32, bool, error) {
	if h.compressed.Load() {
		v1, err := h.compressedVectorsCache.get(context.Background(), a)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID)
				return 0, false, nil
			} else {
				// not a typed error, we can recover from, return with err
				return 0, false, errors.Wrapf(err,
					"could not get vector of object at docID %d", a)
			}
		}
		if len(v1) == 0 {
			return 0, false, fmt.Errorf("got a nil or zero-length vector at docID %d", a)
		}

		v2, err := h.compressedVectorsCache.get(context.Background(), b)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID)
				return 0, false, nil
			} else {
				// not a typed error, we can recover from, return with err
				return 0, false, errors.Wrapf(err,
					"could not get vector of object at docID %d", a)
			}
		}
		if len(v2) == 0 {
			return 0, false, fmt.Errorf("got a nil or zero-length vector at docID %d", b)
		}

		return h.pq.DistanceBetweenCompressedVectors(v1, v2), true, nil
	}
	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	vecA, err := h.vectorForID(context.Background(), a)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID)
			return 0, false, nil
		} else {
			// not a typed error, we can recover from, return with err
			return 0, false, errors.Wrapf(err,
				"could not get vector of object at docID %d", a)
		}
	}

	if len(vecA) == 0 {
		return 0, false, fmt.Errorf("got a nil or zero-length vector at docID %d", a)
	}

	vecB, err := h.vectorForID(context.Background(), b)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID)
			return 0, false, nil
		} else {
			// not a typed error, we can recover from, return with err
			return 0, false, errors.Wrapf(err,
				"could not get vector of object at docID %d", b)
		}
	}

	if len(vecB) == 0 {
		return 0, false, fmt.Errorf("got a nil or zero-length vector at docID %d", b)
	}

	return h.distancerProvider.SingleDist(vecA, vecB)
}

func (h *hnsw) distBetweenNodeAndVec(node uint64, vecB []float32) (float32, bool, error) {
	if h.compressed.Load() {
		v1, err := h.compressedVectorsCache.get(context.Background(), node)
		if err != nil {
			var e storobj.ErrNotFound
			if errors.As(err, &e) {
				h.handleDeletedNode(e.DocID)
				return 0, false, nil
			} else {
				// not a typed error, we can recover from, return with err
				return 0, false, errors.Wrapf(err,
					"could not get vector of object at docID %d", node)
			}
		}
		if len(v1) == 0 {
			return 0, false, fmt.Errorf("got a nil or zero-length vector at docID %d", node)
		}

		return h.pq.DistanceBetweenCompressedAndUncompressedVectors(vecB, v1), true, nil
	}
	// TODO: introduce single search/transaction context instead of spawning new
	// ones
	vecA, err := h.vectorForID(context.Background(), node)
	if err != nil {
		var e storobj.ErrNotFound
		if errors.As(err, &e) {
			h.handleDeletedNode(e.DocID)
			return 0, false, nil
		} else {
			// not a typed error, we can recover from, return with err
			return 0, false, errors.Wrapf(err,
				"could not get vector of object at docID %d", node)
		}
	}

	if len(vecA) == 0 {
		return 0, false, fmt.Errorf(
			"got a nil or zero-length vector at docID %d", node)
	}

	if len(vecB) == 0 {
		return 0, false, fmt.Errorf(
			"got a nil or zero-length vector as search vector")
	}

	return h.distancerProvider.SingleDist(vecA, vecB)
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

func (h *hnsw) isEmpty() bool {
	h.RLock()
	defer h.RUnlock()

	return h.isEmptyUnsecured()
}

func (h *hnsw) isEmptyUnsecured() bool {
	for _, node := range h.nodes {
		if node != nil {
			return false
		}
	}
	return true
}

func (h *hnsw) nodeByID(id uint64) *vertex {
	h.RLock()
	defer h.RUnlock()

	if id >= uint64(len(h.nodes)) {
		// See https://github.com/weaviate/weaviate/issues/1838 for details.
		// This could be after a crash recovery when the object store is "further
		// ahead" than the hnsw index and we receive a delete request
		return nil
	}

	return h.nodes[id]
}

func (h *hnsw) Drop(ctx context.Context) error {
	// cancel tombstone cleanup goroutine

	// if the interval is 0 we never started a cleanup cycle, therefore there is
	// no loop running that could receive our cancel and we would be stuck. Thus,
	// only cancel if we know it's been started.
	if h.cleanupInterval != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if err := h.tombstoneCleanupCycle.StopAndWait(ctx); err != nil {
			return errors.Wrap(err, "hnsw drop")
		}
	}

	if h.compressed.Load() {
		h.compressedVectorsCache.drop()
	} else {
		// cancel vector cache goroutine
		h.cache.drop()
	}

	// cancel commit logger last, as the tombstone cleanup cycle might still
	// write while it's still running
	err := h.commitLog.Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "commit log drop")
	}

	return nil
}

func (h *hnsw) Shutdown(ctx context.Context) error {
	if err := h.tombstoneCleanupCycle.StopAndWait(ctx); err != nil {
		return errors.Wrap(err, "hnsw shutdown")
	}

	if err := h.commitLog.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "hnsw shutdown")
	}

	if h.compressed.Load() {
		h.compressedVectorsCache.drop()
		if err := h.compressedStore.Shutdown(ctx); err != nil {
			return errors.Wrap(err, "hnsw shutdown")
		}
	} else {
		h.cache.drop()
	}

	return nil
}

func (h *hnsw) Flush() error {
	return h.commitLog.Flush()
}

func (h *hnsw) Entrypoint() uint64 {
	h.RLock()
	defer h.RUnlock()

	return h.entryPointID
}
