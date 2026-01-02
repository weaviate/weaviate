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

package hfresh

import (
	"context"
	"encoding/binary"
	stderrors "errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
)

const (
	reassignThreshold = 3 // Fine-tuned threshold to avoid unnecessary splits during reassign operations
)

var (
	ErrPostingNotFound = errors.New("posting not found")
	ErrVectorNotFound  = errors.New("vector not found")
)

var _ common.VectorIndex = (*HFresh)(nil)

// HFresh is an implementation of a vector index using the SPFresh algorithm.
// It spawns background workers to handle split, merge, and reassign operations,
// while exposing a synchronous API for searching and updating vectors.
// Note: this is a work in progress and not all features are implemented yet.
type HFresh struct {
	id             string
	logger         logrus.FieldLogger
	config         *Config // Config contains internal configuration settings.
	metrics        *Metrics
	scheduler      *queue.Scheduler
	maxPostingSize uint32
	minPostingSize uint32
	replicas       uint32
	rngFactor      float32
	searchProbe    uint32
	store          *lsmkv.Store

	// some components require knowing the vector size beforehand
	// and can only be initialized once the first vector has been
	// received
	initDimensionsOnce sync.Once
	dims               uint32 // Number of dimensions of expected vectors
	distancer          *Distancer
	quantizer          *compressionhelpers.RotationalQuantizer

	// Internal components
	Centroids    *HNSWIndex       // Provides access to the centroids.
	PostingStore *PostingStore    // Used for managing persistence of postings.
	IDs          *common.Sequence // Shared monotonic counter for generating unique IDs for new postings.
	VersionMap   *VersionMap      // Stores vector versions in-memory.
	PostingSizes *PostingSizes    // Stores the size of each posting in-memory.
	Metadata     *MetadataStore   // Stores metadata about the index.

	// ctx and cancel are used to manage the lifecycle of the background operations.
	ctx    context.Context
	cancel context.CancelFunc

	taskQueue TaskQueue

	visitedPool *visited.Pool

	postingLocks       *common.ShardedRWLocks // Locks to prevent concurrent modifications to the same posting.
	initialPostingLock sync.Mutex

	vectorForId common.VectorForID[float32]
}

func New(cfg *Config, uc ent.UserConfig, store *lsmkv.Store) (*HFresh, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	metrics := NewMetrics(cfg.PrometheusMetrics, cfg.ClassName, cfg.ShardName)

	bucket, err := NewSharedBucket(store, cfg.ID, cfg.Store)
	if err != nil {
		return nil, err
	}

	postingStore, err := NewPostingStore(store, bucket, metrics, cfg.ID, cfg.Store)
	if err != nil {
		return nil, err
	}

	postingSizes, err := NewPostingSizes(bucket, metrics)
	if err != nil {
		return nil, err
	}

	versionMap, err := NewVersionMap(bucket)
	if err != nil {
		return nil, err
	}

	metadata := NewMetadataStore(bucket)

	h := HFresh{
		id:           cfg.ID,
		logger:       cfg.Logger.WithField("component", "HFresh"),
		config:       cfg,
		scheduler:    cfg.Scheduler,
		store:        store,
		metrics:      metrics,
		PostingStore: postingStore,
		vectorForId:  cfg.VectorForIDThunk,
		VersionMap:   versionMap,
		PostingSizes: postingSizes,
		Metadata:     metadata,
		postingLocks: common.NewDefaultShardedRWLocks(),
		// TODO: choose a better starting size since we can predict the max number of
		// visited vectors based on cfg.InternalPostingCandidates.
		visitedPool:    visited.NewPool(1, 512, -1),
		maxPostingSize: uc.MaxPostingSize,
		minPostingSize: uc.MinPostingSize,
		replicas:       uc.Replicas,
		rngFactor:      uc.RNGFactor,
		searchProbe:    uc.SearchProbe,
	}

	h.Centroids, err = NewHNSWIndex(metrics, store, cfg, 1024*1024, 1024)
	if err != nil {
		return nil, err
	}
	h.IDs, err = common.NewSequence(NewBucketStore(bucket), 1000)
	if err != nil {
		return nil, err
	}

	h.ctx, h.cancel = context.WithCancel(context.Background())

	taskQueue, err := NewTaskQueue(&h, bucket)
	if err != nil {
		return nil, err
	}
	h.taskQueue = *taskQueue

	if err = h.restoreMetadata(); err != nil {
		h.logger.Warnf("unable to restore metadata from previous run with error: %v", err)
	}

	return &h, nil
}

// Delete marks a vector as deleted in the version map.
func (h *HFresh) Delete(ids ...uint64) error {
	for _, id := range ids {
		start := time.Now()
		version, err := h.VersionMap.MarkDeleted(context.Background(), id)
		if err != nil {
			return errors.Wrapf(err, "failed to mark vector %d as deleted", id)
		}
		if version == 0 {
			return ErrVectorNotFound
		}
		h.metrics.DeleteVector(start)
	}

	return nil
}

func (h *HFresh) Type() common.IndexType {
	return common.IndexTypeHFresh
}

func (h *HFresh) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	parsed, ok := updated.(ent.UserConfig)
	if !ok {
		callback()
		return errors.Errorf("config is not UserConfig, but %T", updated)
	}

	atomic.StoreUint32(&h.searchProbe, parsed.SearchProbe)

	callback()
	return nil
}

func (h *HFresh) Drop(ctx context.Context, keepFiles bool) error {
	_ = h.Shutdown(ctx)
	// Shard::drop will take care of handling store buckets
	return nil
}

func (h *HFresh) Shutdown(ctx context.Context) error {
	if h.ctx == nil {
		return nil // Already closed or not started
	}

	if h.ctx.Err() != nil {
		return h.ctx.Err() // Context already cancelled
	}

	// Cancel the context to prevent new operations from being enqueued
	h.cancel()

	var errs []error

	err := h.taskQueue.Close()
	if err != nil {
		errs = append(errs, err)
	}

	err = h.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	err = h.IDs.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	err = h.Centroids.hnsw.Shutdown(ctx)
	if err != nil {
		errs = append(errs, err)
	}

	return stderrors.Join(errs...)
}

func (h *HFresh) Flush() error {
	var errs []error

	// flush the HNSW commit log
	err := h.Centroids.hnsw.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	// flush the task queues
	err = h.taskQueue.Flush()
	if err != nil {
		errs = append(errs, err)
	}

	return stderrors.Join(errs...)
}

func (h *HFresh) SwitchCommitLogs(ctx context.Context) error {
	return h.Centroids.hnsw.SwitchCommitLogs(ctx)
}

func (h *HFresh) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return nil, nil
}

func (h *HFresh) PostStartup(ctx context.Context) {
	h.Centroids.hnsw.PostStartup(ctx)
}

func (h *HFresh) Compressed() bool {
	return true
}

func (h *HFresh) Multivector() bool {
	return false
}

func (h *HFresh) ContainsDoc(id uint64) bool {
	v, err := h.VersionMap.Get(context.Background(), id)
	if err != nil {
		h.logger.WithField("vectorID", id).
			Debug("vector version get failed, returning false")
		return false
	}
	return !v.Deleted() && v.Version() > 0
}

func (h *HFresh) Iterate(fn func(id uint64) bool) {
	h.logger.Warn("Iterate is not implemented for HFresh index")
}

func float32SliceFromByteSlice(vector []byte, slice []float32) []float32 {
	for i := range slice {
		slice[i] = math.Float32frombits(binary.LittleEndian.Uint32(vector[i*4:]))
	}
	return slice
}

func (h *HFresh) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	var bucketName string
	if h.config.TargetVector != "" {
		bucketName = fmt.Sprintf("%s_%s", helpers.VectorsBucketLSM, h.config.TargetVector)
	} else {
		bucketName = helpers.VectorsBucketLSM
	}

	distFunc := func(id uint64) (float32, error) {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], id)
		vec, err := h.store.Bucket(bucketName).Get(buf[:])
		if err != nil {
			return 0, err
		}

		dist, err := h.config.DistanceProvider.SingleDist(queryVector, float32SliceFromByteSlice(vec, make([]float32, len(vec)/4)))
		if err != nil {
			return 0, err
		}
		return dist, nil
	}

	return common.QueryVectorDistancer{DistanceFunc: distFunc}
}

func (h *HFresh) CompressionStats() compressionhelpers.CompressionStats {
	if h.quantizer != nil {
		return h.quantizer.Stats()
	}
	return compressionhelpers.UncompressedStats{}
}

func (h *HFresh) Preload(id uint64, vector []float32) {
	// for now, nothing to do here
}

// deduplicator is a simple thread-safe structure to prevent duplicate values.
type deduplicator struct {
	m *xsync.Map[uint64, struct{}]
}

func newDeduplicator() *deduplicator {
	return &deduplicator{
		m: xsync.NewMap[uint64, struct{}](),
	}
}

// tryAdd attempts to add an ID to the deduplicator.
// Returns true if the ID was added, false if it already exists.
func (d *deduplicator) tryAdd(id uint64) bool {
	_, loaded := d.m.LoadOrStore(id, struct{}{})
	return !loaded
}

// done marks an ID as processed, removing it from the deduplicator.
func (d *deduplicator) done(id uint64) {
	d.m.Delete(id)
}

// contains checks if an ID is already in the deduplicator.
func (d *deduplicator) contains(id uint64) bool {
	_, exists := d.m.Load(id)
	return exists
}
