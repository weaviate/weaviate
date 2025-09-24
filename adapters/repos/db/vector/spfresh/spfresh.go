//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package spfresh

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
)

const (
	reassignThreshold = 3        // Fine-tuned threshold to avoid unnecessary splits during reassign operations
	splitReuseEpsilon = 0.000001 // Epsilon to determine if a split can reuse the existing posting
)

var (
	ErrPostingNotFound  = errors.New("posting not found")
	ErrVectorNotFound   = errors.New("vector not found")
	ErrIdenticalVectors = errors.New("posting list contains identical or near-identical vectors")
)

var _ common.VectorIndex = (*SPFresh)(nil)

// SPFresh is an implementation of a vector index using the SPFresh algorithm.
// It spawns background workers to handle split, merge, and reassign operations,
// while exposing a synchronous API for searching and updating vectors.
// Note: this is a work in progress and not all features are implemented yet.
type SPFresh struct {
	Logger logrus.FieldLogger
	Config *Config // Config contains internal configuration settings.

	// some components require knowing the vector size beforehand
	// and can only be initialized once the first vector has been
	// received
	initDimensionsOnce sync.Once
	dims               int32 // Number of dimensions of expected vectors
	vectorSize         int32 // Size of the compressed vectors in bytes

	// Internal components
	SPTAG        SPTAG                   // Provides access to the SPTAG index for centroid operations.
	Store        *LSMStore               // Used for managing persistence of postings.
	IDs          common.MonotonicCounter // Shared monotonic counter for generating unique IDs for new postings.
	VersionMap   *VersionMap             // Stores vector versions in-memory.
	PostingSizes *PostingSizes           // Stores the size of each posting in-memory.

	// ctx and cancel are used to manage the lifecycle of the background operations.
	ctx    context.Context
	cancel context.CancelFunc

	splitCh    *common.UnboundedChannel[uint64]            // Channel for split operations
	mergeCh    *common.UnboundedChannel[uint64]            // Channel for merge operations
	reassignCh *common.UnboundedChannel[reassignOperation] // Channel for reassign operations
	wg         sync.WaitGroup

	splitList *deduplicator // Prevents duplicate split operations
	mergeList *deduplicator // Prevents duplicate merge operations

	visitedPool *visited.Pool
	// TODO: make the distancer configurable
	distancer *Distancer

	postingLocks *common.ShardedRWLocks // Locks to prevent concurrent modifications to the same posting.

	initialPostingLock sync.Mutex
}

func New(cfg *Config, store *lsmkv.Store) (*SPFresh, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	postingStore, err := NewLSMStore(store, bucketName(cfg.ID))
	if err != nil {
		return nil, err
	}

	s := SPFresh{
		Logger: cfg.Logger.WithField("component", "SPFresh"),
		Config: cfg,
		SPTAG:  NewBruteForceSPTAG(),
		Store:  postingStore,
		// Capacity of the version map: 8k pages, 1M vectors each -> 8B vectors
		// - An empty version map consumes 240KB of memory
		// - Each allocated page consumes 1MB of memory
		// - A fully used version map consumes 8GB of memory
		VersionMap: NewVersionMap(8*1024, 1024*1024),
		// Capacity of the posting sizes: 1k pages, 1M postings each -> 1B postings
		// - An empty posting sizes buffer consumes 240KB of memory
		// - Each allocated page consumes 4MB of memory
		// - A fully used posting sizes consumes 4GB of memory
		PostingSizes: NewPostingSizes(1024, 1024*1024),

		postingLocks: common.NewDefaultShardedRWLocks(),
		// TODO: Eventually, we'll create sharded workers between all instances of SPFresh
		// to minimize the number of goroutines while still maximizing CPU usage and I/O throughput.
		splitCh:    common.MakeUnboundedChannel[uint64](),
		mergeCh:    common.MakeUnboundedChannel[uint64](),
		reassignCh: common.MakeUnboundedChannel[reassignOperation](),
		splitList:  newDeduplicator(),
		mergeList:  newDeduplicator(),
		// TODO: choose a better starting size since we can predict the max number of
		// visited vectors based on cfg.InternalPostingCandidates.
		visitedPool: visited.NewPool(1, 512, -1),
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())

	// start N workers to process split operations
	for i := 0; i < s.Config.SplitWorkers; i++ {
		s.wg.Add(1)
		enterrors.GoWrapper(s.splitWorker, s.Logger)
	}

	// start M workers to process reassign operations
	for i := 0; i < s.Config.ReassignWorkers; i++ {
		s.wg.Add(1)
		enterrors.GoWrapper(s.reassignWorker, s.Logger)
	}

	// start a single worker to process merge operations
	s.wg.Add(1)
	enterrors.GoWrapper(s.mergeWorker, s.Logger)

	return &s, nil
}

// Delete marks a vector as deleted in the version map.
func (s *SPFresh) Delete(ids ...uint64) error {
	for _, id := range ids {
		version := s.VersionMap.MarkDeleted(id)
		if version == 0 {
			return ErrVectorNotFound
		}
	}
	return nil
}

func (s *SPFresh) Type() common.IndexType {
	return common.IndexTypeSPFresh
}

func (s *SPFresh) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	return errors.New("UpdateUserConfig is not supported for the spfresh index")
}

func (s *SPFresh) Drop(ctx context.Context) error {
	_ = s.Shutdown(ctx)
	// Shard::drop will take care of handling store buckets
	return nil
}

func (s *SPFresh) Shutdown(ctx context.Context) error {
	if s.ctx == nil {
		return nil // Already closed or not started
	}

	if s.ctx.Err() != nil {
		return s.ctx.Err() // Context already cancelled
	}

	// Cancel the context to prevent new operations from being enqueued
	s.cancel()

	// Close the split channel to signal workers to stop
	s.splitCh.Close(ctx)
	s.reassignCh.Close(ctx)
	s.mergeCh.Close(ctx)

	s.wg.Wait() // Wait for all workers to finish
	return nil
}

func (s *SPFresh) Flush() error {
	return s.Store.Flush()
}

func (s *SPFresh) SwitchCommitLogs(ctx context.Context) error {
	return nil
}

func (s *SPFresh) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return nil, nil
}

func (s *SPFresh) PostStartup() {
	// This method can be used to perform any post-startup initialization
	// For now, it does nothing
}

func (s *SPFresh) Compressed() bool {
	return true
}

func (s *SPFresh) Multivector() bool {
	return false
}

func (s *SPFresh) ContainsDoc(id uint64) bool {
	v := s.VersionMap.Get(id)
	return !v.Deleted() && v.Version() > 0
}

func (s *SPFresh) Iterate(fn func(id uint64) bool) {
	s.Logger.Warn("Iterate is not implemented for SPFresh index")
}

func float32SliceFromByteSlice(vector []byte, slice []float32) []float32 {
	for i := range slice {
		slice[i] = math.Float32frombits(binary.LittleEndian.Uint32(vector[i*4:]))
	}
	return slice
}

func (s *SPFresh) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	var bucketName string
	if s.Config.TargetVector != "" {
		bucketName = fmt.Sprintf("%s_%s", helpers.VectorsBucketLSM, s.Config.TargetVector)
	} else {
		bucketName = helpers.VectorsBucketLSM
	}

	distFunc := func(id uint64) (float32, error) {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], id)
		vec, err := s.Store.store.Bucket(bucketName).Get(buf[:])
		if err != nil {
			return 0, err
		}

		dist, err := s.Config.Distancer.SingleDist(queryVector, float32SliceFromByteSlice(vec, make([]float32, len(vec)/4)))
		if err != nil {
			return 0, err
		}
		return dist, nil
	}

	return common.QueryVectorDistancer{DistanceFunc: distFunc}
}

func (s *SPFresh) CompressionStats() compressionhelpers.CompressionStats {
	return s.SPTAG.Quantizer().Stats()
}

// deduplicator is a simple thread-safe structure to prevent duplicate values.
type deduplicator struct {
	mu sync.RWMutex
	m  map[uint64]struct{}
}

func newDeduplicator() *deduplicator {
	return &deduplicator{
		m: make(map[uint64]struct{}),
	}
}

// tryAdd attempts to add an ID to the deduplicator.
// Returns true if the ID was added, false if it already exists.
func (d *deduplicator) tryAdd(id uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, exists := d.m[id]
	if !exists {
		d.m[id] = struct{}{}
	}
	return !exists
}

// done marks an ID as processed, removing it from the deduplicator.
func (d *deduplicator) done(id uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.m, id)
}

// contains checks if an ID is already in the deduplicator.
func (d *deduplicator) contains(id uint64) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	_, exists := d.m[id]
	return exists
}
