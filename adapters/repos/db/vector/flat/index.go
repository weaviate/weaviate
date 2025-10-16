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

package flat

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	entcfg "github.com/weaviate/weaviate/entities/config"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entlsmkv "github.com/weaviate/weaviate/entities/lsmkv"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

const (
	defaultCachePageSize = 32
)

type flat struct {
	id                string
	targetVector      string
	rootPath          string
	dims              int32
	metadata          *bolt.DB
	metadataLock      *sync.RWMutex
	store             *lsmkv.Store
	logger            logrus.FieldLogger
	distancerProvider distancer.Provider
	initOnce          sync.Once
	rescore           int64

	compressed      atomic.Bool
	compressionType CompressionType
	quantizer       Quantizer
	cache           *Cache

	pqResults *common.PqMaxPool
	pool      *pools

	count                uint64
	concurrentCacheReads int
}

type distanceCalc func(vecAsBytes []byte) (float32, error)

func New(cfg Config, uc flatent.UserConfig, store *lsmkv.Store) (*flat, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	logger := cfg.Logger
	if logger == nil {
		l := logrus.New()
		l.Out = io.Discard
		logger = l
	}

	index := &flat{
		id:                   cfg.ID,
		targetVector:         cfg.TargetVector,
		rootPath:             cfg.RootPath,
		logger:               logger,
		distancerProvider:    cfg.DistanceProvider,
		metadataLock:         &sync.RWMutex{},
		rescore:              extractCompressionRescore(uc),
		pqResults:            common.NewPqMaxPool(100),
		compressionType:      extractCompression(uc),
		pool:                 newPools(),
		store:                store,
		concurrentCacheReads: runtime.GOMAXPROCS(0) * 2,
	}
	if err := index.initBuckets(context.Background(), cfg); err != nil {
		return nil, fmt.Errorf("init flat index buckets: %w", err)
	}

	if err := index.restoreMetadata(); err != nil {
		return nil, err
	}

	// If compression is BQ we create the quantizer and cache immediately
	// For RQ we create when restoring from disk or on first batch
	if index.compressionType == CompressionBQ {
		index.compressed.Store(true)
		builder := NewQuantizerBuilder(cfg.DistanceProvider)
		index.quantizer = builder.CreateQuantizer(index.compressionType, 0)
	}

	cached, cacheType := extractCache(uc)
	if cached {
		index.cache = NewCache(index.getUint64QuantizedVector, index.getByteQuantizedVector, uc.VectorCacheMaxObjects,
			cfg.Logger, cfg.AllocChecker, cacheType)
	}

	return index, nil
}

func (flat *flat) getUint64QuantizedVector(ctx context.Context, id uint64) ([]uint64, error) {
	key := flat.pool.byteSlicePool.Get(8)
	defer flat.pool.byteSlicePool.Put(key)
	binary.BigEndian.PutUint64(key.slice, id)
	bytes, err := flat.store.Bucket(flat.getCompressedBucketName()).Get(key.slice)
	if err != nil {
		return nil, err
	}
	if len(bytes) == 0 {
		return nil, nil
	}
	return uint64SliceFromByteSlice(bytes, make([]uint64, len(bytes)/8)), nil
}

func (flat *flat) getByteQuantizedVector(ctx context.Context, id uint64) ([]byte, error) {
	key := flat.pool.byteSlicePool.Get(8)
	defer flat.pool.byteSlicePool.Put(key)
	binary.BigEndian.PutUint64(key.slice, id)
	bytes, err := flat.store.Bucket(flat.getCompressedBucketName()).Get(key.slice)
	if err != nil {
		return nil, err
	}
	if len(bytes) == 0 {
		return nil, nil
	}
	return bytes, nil
}

func extractCompression(uc flatent.UserConfig) CompressionType {
	if uc.BQ.Enabled {
		return CompressionBQ
	}

	if uc.RQ.Enabled {
		if uc.RQ.Bits == 8 {
			return CompressionRQ8
		}
		return CompressionRQ1
	}

	return CompressionNone
}

func extractCompressionRescore(uc flatent.UserConfig) int64 {
	compression := extractCompression(uc)
	switch compression {
	case CompressionBQ:
		return int64(uc.BQ.RescoreLimit)
	case CompressionRQ1, CompressionRQ8:
		return int64(uc.RQ.RescoreLimit)
	default:
		return 0
	}
}

func extractCache(uc flatent.UserConfig) (bool, QuantizerType) {
	if uc.BQ.Enabled {
		return uc.BQ.Cache, Uint64Quantizer
	}

	if uc.RQ.Enabled {
		if uc.RQ.Bits == 8 {
			return uc.RQ.Cache, ByteQuantizer
		}
		return uc.RQ.Cache, Uint64Quantizer
	}

	return false, 0
}

func (index *flat) storeCompressedVector(id uint64, vector []byte) {
	index.storeGenericVector(id, vector, index.getCompressedBucketName())
}

func (index *flat) storeVector(id uint64, vector []byte) {
	index.storeGenericVector(id, vector, index.getBucketName())
}

func (index *flat) storeGenericVector(id uint64, vector []byte, bucket string) {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	index.store.Bucket(bucket).Put(idBytes, vector)
}

func (index *flat) Cached() bool {
	return index.compressed.Load() && index.cache != nil
}

func (index *flat) Compressed() bool {
	return index.compressed.Load()
}

func (index *flat) Multivector() bool {
	return false
}

func (index *flat) getBucketName() string {
	if index.targetVector != "" {
		return fmt.Sprintf("%s_%s", helpers.VectorsBucketLSM, index.targetVector)
	}
	return helpers.VectorsBucketLSM
}

func (index *flat) getCompressedBucketName() string {
	return helpers.GetCompressedBucketName(index.targetVector)
}

func (index *flat) initBuckets(ctx context.Context, cfg Config) error {
	// TODO: Forced compaction should not stay an all or nothing option.
	//       This is only a temporary measure until dynamic compaction
	//       behavior is implemented.
	//       See: https://github.com/weaviate/weaviate/issues/5241
	forceCompaction := shouldForceCompaction()
	if err := index.store.CreateOrLoadBucket(ctx, index.getBucketName(),
		lsmkv.WithForceCompaction(forceCompaction),
		lsmkv.WithUseBloomFilter(false),
		lsmkv.WithMinMMapSize(cfg.MinMMapSize),
		lsmkv.WithMinWalThreshold(cfg.MinMMapSize),
		lsmkv.WithAllocChecker(cfg.AllocChecker),
		lsmkv.WithLazySegmentLoading(cfg.LazyLoadSegments),
		lsmkv.WithWriteSegmentInfoIntoFileName(cfg.WriteSegmentInfoIntoFileName),
		lsmkv.WithWriteMetadata(cfg.WriteMetadataFilesEnabled),
		lsmkv.WithStrategy(lsmkv.StrategyReplace),

		// Pread=false flag introduced around ~v1.25.9. Before that, the pread flag
		// was simply missing. Now we want to explicitly set it to false for
		// performance reasons. There are pread performance improvements in the
		// pipeline, but as of now, mmap is much more performant – especially for
		// parallel cache prefilling.
		//
		// In the future when the pure pread performance is on par with mmap, we
		// should update this to pass the global setting.
		lsmkv.WithPread(false),
		lsmkv.WithCalcCountNetAdditions(true),
	); err != nil {
		return fmt.Errorf("create or load flat vectors bucket: %w", err)
	}
	if index.compressionType != CompressionNone {
		if err := index.store.CreateOrLoadBucket(ctx, index.getCompressedBucketName(),
			lsmkv.WithForceCompaction(forceCompaction),
			lsmkv.WithUseBloomFilter(false),
			lsmkv.WithMinMMapSize(cfg.MinMMapSize),
			lsmkv.WithMinWalThreshold(cfg.MinMMapSize),
			lsmkv.WithAllocChecker(cfg.AllocChecker),
			lsmkv.WithLazySegmentLoading(cfg.LazyLoadSegments),
			lsmkv.WithWriteSegmentInfoIntoFileName(cfg.WriteSegmentInfoIntoFileName),
			lsmkv.WithWriteMetadata(cfg.WriteMetadataFilesEnabled),
			lsmkv.WithStrategy(lsmkv.StrategyReplace),

			// Pread=false flag introduced around ~v1.25.9. Before that, the pread flag
			// was simply missing. Now we want to explicitly set it to false for
			// performance reasons. There are pread performance improvements in the
			// pipeline, but as of now, mmap is much more performant – especially for
			// parallel cache prefilling.
			//
			// In the future when the pure pread performance is on par with mmap, we
			// should update this to pass the global setting.
			lsmkv.WithPread(false),
		); err != nil {
			return fmt.Errorf("create or load flat compressed vectors bucket: %w", err)
		}
	}

	count := index.store.Bucket(index.getBucketName()).CountAsync()
	atomic.StoreUint64(&index.count, uint64(count))

	return nil
}

// TODO: Remove this function when gh-5241 is completed. See flat::initBuckets for more details.
func shouldForceCompaction() bool {
	return !entcfg.Enabled(os.Getenv("FLAT_INDEX_DISABLE_FORCED_COMPACTION"))
}

func (index *flat) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ids) != len(vectors) {
		return errors.Errorf("ids and vectors sizes does not match")
	}
	if len(ids) == 0 {
		return errors.Errorf("insertBatch called with empty lists")
	}
	for i := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := index.Add(ctx, ids[i], vectors[i]); err != nil {
			return err
		}
	}
	return nil
}

func byteSliceFromUint64Slice(vector []uint64, slice []byte) []byte {
	for i := range vector {
		binary.LittleEndian.PutUint64(slice[i*8:], vector[i])
	}
	return slice
}

func byteSliceFromFloat32Slice(vector []float32, slice []byte) []byte {
	for i := range vector {
		binary.LittleEndian.PutUint32(slice[i*4:], math.Float32bits(vector[i]))
	}
	return slice
}

func uint64SliceFromByteSlice(vector []byte, slice []uint64) []uint64 {
	for i := range slice {
		slice[i] = binary.LittleEndian.Uint64(vector[i*8:])
	}
	return slice
}

func float32SliceFromByteSlice(vector []byte, slice []float32) []float32 {
	for i := range slice {
		slice[i] = math.Float32frombits(binary.LittleEndian.Uint32(vector[i*4:]))
	}
	return slice
}

func (index *flat) initializeDimensionsAndRQ(vector []float32) {
	dims := atomic.LoadInt32(&index.dims)

	if dims == 0 {
		size := int32(len(vector))
		atomic.StoreInt32(&index.dims, size)
		if err := index.setDimensions(size); err != nil {
			index.logger.WithError(err).Error("could not set dimensions")
			return // Fail the entire initialization
		}
		dims = size
	}

	if !index.Compressed() && (index.compressionType == CompressionRQ1 || index.compressionType == CompressionRQ8) {
		builder := NewQuantizerBuilder(index.distancerProvider)
		index.quantizer = builder.CreateQuantizer(index.compressionType, dims)
		if err := index.persistRQData(); err != nil {
			index.logger.WithError(err).Error("could not persist RQ data")
			return // Fail the entire initialization
		}
		index.compressed.Store(true)
	}
}

func (index *flat) Add(ctx context.Context, id uint64, vector []float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Note usually batches in Weaviate will already call ValidateBeforeInsert
	if err := index.ValidateBeforeInsert(vector); err != nil {
		return err
	}

	index.initOnce.Do(func() {
		index.initializeDimensionsAndRQ(vector)
	})

	vector = index.normalized(vector)
	slice := make([]byte, len(vector)*4)
	index.storeVector(id, byteSliceFromFloat32Slice(vector, slice))

	index.Preload(id, vector)

	for {
		oldCount := atomic.LoadUint64(&index.count)
		if atomic.CompareAndSwapUint64(&index.count, oldCount, oldCount+1) {
			break
		}
	}

	return nil
}

func (index *flat) Delete(ids ...uint64) error {
	for i := range ids {
		if index.Cached() {
			index.cache.Delete(context.Background(), ids[i])
		}
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, ids[i])

		if err := index.store.Bucket(index.getBucketName()).Delete(idBytes); err != nil {
			return err
		}

		if index.Compressed() {
			if err := index.store.Bucket(index.getCompressedBucketName()).Delete(idBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

func (index *flat) searchTimeRescore(k int) int {
	// load atomically, so we can get away with concurrent updates of the
	// userconfig without having to set a lock each time we try to read - which
	// can be so common that it would cause considerable overhead
	if rescore := int(atomic.LoadInt64(&index.rescore)); rescore > k {
		return rescore
	}
	return k
}

func (index *flat) SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	switch index.compressionType {
	case CompressionBQ, CompressionRQ1, CompressionRQ8:
		return index.searchByVectorQuantized(ctx, vector, k, allow)
	default:
		return index.searchByVector(ctx, vector, k, allow)
	}
}

func (index *flat) searchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	// TODO: pass context into inner methods, so it can be checked more granuarly
	heap := index.pqResults.GetMax(k)
	defer index.pqResults.Put(heap)

	vector = index.normalized(vector)

	if err := index.findTopVectors(heap, allow, k,
		index.store.Bucket(index.getBucketName()).Cursor,
		index.createDistanceCalc(vector),
	); err != nil {
		return nil, nil, err
	}

	ids, dists := index.extractHeap(heap)
	return ids, dists, nil
}

func (index *flat) createDistanceCalc(vector []float32) distanceCalc {
	return func(vecAsBytes []byte) (float32, error) {
		vecSlice := index.pool.float32SlicePool.Get(len(vecAsBytes) / 4)
		defer index.pool.float32SlicePool.Put(vecSlice)

		candidate := float32SliceFromByteSlice(vecAsBytes, vecSlice.slice)
		return index.distancerProvider.SingleDist(vector, candidate)
	}
}

func (index *flat) searchByVectorQuantized(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	// Ensure quantizer is initialized
	if index.quantizer == nil {
		return nil, nil, fmt.Errorf("quantizer not initialized")
	}

	// TODO: pass context into inner methods, so it can be checked more granuarly
	rescore := index.searchTimeRescore(k)
	heap := index.pqResults.GetMax(rescore)
	defer index.pqResults.Put(heap)

	vector = index.normalized(vector)

	if index.Compressed() && index.Cached() {
		if err := index.findTopVectorsQuantizedCached(heap, allow, rescore, vector); err != nil {
			return nil, nil, err
		}
	} else {
		if err := index.findTopVectors(heap, allow, rescore,
			index.store.Bucket(index.getCompressedBucketName()).Cursor,
			index.createDistanceCalcQuantized(vector),
		); err != nil {
			return nil, nil, err
		}
	}

	distanceCalc := index.createDistanceCalc(vector)
	idsSlice := index.pool.uint64SlicePool.Get(heap.Len())
	defer index.pool.uint64SlicePool.Put(idsSlice)

	for i := range idsSlice.slice {
		idsSlice.slice[i] = heap.Pop().ID
	}

	// we expect to be mostly IO-bound, so more goroutines than CPUs is fine
	distancesUncompressedVectors := make([]float32, len(idsSlice.slice))

	eg := enterrors.NewErrorGroupWrapper(index.logger)
	for workerID := 0; workerID < index.concurrentCacheReads; workerID++ {
		workerID := workerID
		eg.Go(func() error {
			for idPos := workerID; idPos < len(idsSlice.slice); idPos += index.concurrentCacheReads {
				id := idsSlice.slice[idPos]
				candidateAsBytes, err := index.vectorById(id)
				if err != nil {
					return err
				}
				if len(candidateAsBytes) == 0 {
					continue
				}
				distance, err := distanceCalc(candidateAsBytes)
				if err != nil {
					return err
				}

				distancesUncompressedVectors[idPos] = distance
			}

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	for i, id := range idsSlice.slice {
		index.insertToHeap(heap, k, id, distancesUncompressedVectors[i])
	}

	ids, dists := index.extractHeap(heap)
	return ids, dists, nil
}

func (index *flat) createDistanceCalcQuantized(queryVector []float32) distanceCalc {
	switch index.compressionType {
	case CompressionRQ1:
		// For RQ-1 bit, use NewDistancer to get 5-bit query quantization for better accuracy
		distancer := index.quantizer.(*BinaryRotationalQuantizerWrapper).NewDistancer(queryVector)
		return func(vecAsBytes []byte) (float32, error) {
			vecSliceQuantized := index.pool.uint64SlicePool.Get(len(vecAsBytes) / 8)
			defer index.pool.uint64SlicePool.Put(vecSliceQuantized)

			candidate := uint64SliceFromByteSlice(vecAsBytes, vecSliceQuantized.slice)
			return distancer.Distance(candidate)
		}
	case CompressionBQ:
		queryQuantized := index.quantizer.EncodeUint64(queryVector)
		return func(vecAsBytes []byte) (float32, error) {
			vecSliceQuantized := index.pool.uint64SlicePool.Get(len(vecAsBytes) / 8)
			defer index.pool.uint64SlicePool.Put(vecSliceQuantized)

			candidate := uint64SliceFromByteSlice(vecAsBytes, vecSliceQuantized.slice)
			return index.quantizer.DistanceBetweenUint64Vectors(candidate, queryQuantized)
		}
	case CompressionRQ8:
		queryQuantized := index.quantizer.EncodeBytes(queryVector)
		return func(vecAsBytes []byte) (float32, error) {
			// For byte quantizer, use the bytes directly without conversion
			return index.quantizer.DistanceBetweenByteVectors(vecAsBytes, queryQuantized)
		}
	default:
		return func(vecAsBytes []byte) (float32, error) {
			return 0, fmt.Errorf("unsupported quantizer data type: %v", index.compressionType)
		}
	}
}

func (index *flat) vectorById(id uint64) ([]byte, error) {
	idSlice := index.pool.byteSlicePool.Get(8)
	defer index.pool.byteSlicePool.Put(idSlice)

	binary.BigEndian.PutUint64(idSlice.slice, id)
	return index.store.Bucket(index.getBucketName()).Get(idSlice.slice)
}

// populates given heap with smallest distances and corresponding ids calculated by
// distanceCalc
func (index *flat) findTopVectors(heap *priorityqueue.Queue[any],
	allow helpers.AllowList, limit int, cursorFn func() *lsmkv.CursorReplace,
	distanceCalc distanceCalc,
) error {
	var key []byte
	var v []byte
	var id uint64
	allowMax := uint64(0)

	cursor := cursorFn()
	defer cursor.Close()

	if allow != nil {
		// nothing allowed, skip search
		if allow.IsEmpty() {
			return nil
		}

		allowMax = allow.Max()

		idSlice := index.pool.byteSlicePool.Get(8)
		binary.BigEndian.PutUint64(idSlice.slice, allow.Min())
		key, v = cursor.Seek(idSlice.slice)
		index.pool.byteSlicePool.Put(idSlice)
	} else {
		key, v = cursor.First()
	}

	// since keys are sorted, once key/id get greater than max allowed one
	// further search can be stopped
	for ; key != nil && (allow == nil || id <= allowMax); key, v = cursor.Next() {
		id = binary.BigEndian.Uint64(key)
		if allow == nil || allow.Contains(id) {
			distance, err := distanceCalc(v)
			if err != nil {
				return err
			}
			index.insertToHeap(heap, limit, id, distance)
		}
	}
	return nil
}

// populates given heap with smallest distances and corresponding ids calculated by
// distanceCalc for quantized cached vectors
func (index *flat) findTopVectorsQuantizedCached(heap *priorityqueue.Queue[any],
	allow helpers.AllowList, limit int, queryVector []float32,
) error {
	switch index.compressionType {
	case CompressionRQ1:
		distancer := index.quantizer.(*BinaryRotationalQuantizerWrapper).NewDistancer(queryVector)
		action := func(currentId uint64, vec []uint64) error {
			distance, err := distancer.Distance(vec)
			if err != nil {
				return err
			}
			index.insertToHeap(heap, limit, currentId, distance)
			return nil
		}
		return index.cache.IterateUint64WithAllowlist(allow, action)
	case CompressionBQ:
		queryQuantizedUint64 := index.quantizer.EncodeUint64(queryVector)
		action := func(currentId uint64, vec []uint64) error {
			distance, err := index.quantizer.DistanceBetweenUint64Vectors(vec, queryQuantizedUint64)
			if err != nil {
				return err
			}
			index.insertToHeap(heap, limit, currentId, distance)
			return nil
		}
		return index.cache.IterateUint64WithAllowlist(allow, action)
	case CompressionRQ8:
		queryQuantizedBytes := index.quantizer.EncodeBytes(queryVector)
		action := func(currentId uint64, vec []byte) error {
			distance, err := index.quantizer.DistanceBetweenByteVectors(vec, queryQuantizedBytes)
			if err != nil {
				return err
			}
			index.insertToHeap(heap, limit, currentId, distance)
			return nil
		}
		return index.cache.IterateBytesWithAllowlist(allow, action)
	default:
		return fmt.Errorf("unsupported quantizer data type: %v", index.compressionType)
	}
}

func (index *flat) insertToHeap(heap *priorityqueue.Queue[any],
	limit int, id uint64, distance float32,
) {
	if heap.Len() < limit {
		heap.Insert(id, distance)
	} else if heap.Top().Dist > distance {
		heap.Pop()
		heap.Insert(id, distance)
	}
}

func (index *flat) extractHeap(heap *priorityqueue.Queue[any],
) ([]uint64, []float32) {
	len := heap.Len()

	ids := make([]uint64, len)
	dists := make([]float32, len)
	for i := len - 1; i >= 0; i-- {
		item := heap.Pop()
		ids[i] = item.ID
		dists[i] = item.Dist
	}
	return ids, dists
}

func (index *flat) normalized(vector []float32) []float32 {
	if index.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		return distancer.Normalize(vector)
	}
	return vector
}

func (index *flat) SearchByVectorDistance(ctx context.Context, vector []float32,
	targetDistance float32, maxLimit int64, allow helpers.AllowList,
) ([]uint64, []float32, error) {
	var (
		searchParams = newSearchByDistParams(maxLimit)

		resultIDs  []uint64
		resultDist []float32
	)

	recursiveSearch := func() (bool, error) {
		totalLimit := searchParams.TotalLimit()
		ids, dist, err := index.SearchByVector(ctx, vector, totalLimit, allow)
		if err != nil {
			return false, errors.Wrap(err, "vector search")
		}

		// if there is less results than given limit search can be stopped
		shouldContinue := !(len(ids) < totalLimit)

		// ensures the indexes aren't out of range
		offsetCap := searchParams.OffsetCapacity(ids)
		totalLimitCap := searchParams.TotalLimitCapacity(ids)

		if offsetCap == totalLimitCap {
			return false, nil
		}

		ids, dist = ids[offsetCap:totalLimitCap], dist[offsetCap:totalLimitCap]
		for i := range ids {
			if aboveThresh := dist[i] <= targetDistance; aboveThresh ||
				floatcomp.InDelta(float64(dist[i]), float64(targetDistance), 1e-6) {
				resultIDs = append(resultIDs, ids[i])
				resultDist = append(resultDist, dist[i])
			} else {
				// as soon as we encounter a certainty which
				// is below threshold, we can stop searching
				shouldContinue = false
				break
			}
		}

		return shouldContinue, nil
	}

	var shouldContinue bool
	var err error
	for shouldContinue, err = recursiveSearch(); shouldContinue && err == nil; {
		searchParams.Iterate()
		if searchParams.MaxLimitReached() {
			index.logger.
				WithField("action", "unlimited_vector_search").
				Warnf("maximum search limit of %d results has been reached",
					searchParams.MaximumSearchLimit())
			break
		}
	}
	if err != nil {
		return nil, nil, err
	}

	return resultIDs, resultDist, nil
}

func (index *flat) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	parsed, ok := updated.(flatent.UserConfig)
	if !ok {
		callback()
		return errors.Errorf("config is not UserConfig, but %T", updated)
	}

	// Store atomically as a lock here would be very expensive, this value is
	// read on every single user-facing search, which can be highly concurrent
	atomic.StoreInt64(&index.rescore, extractCompressionRescore(parsed))

	callback()
	return nil
}

func (index *flat) Drop(ctx context.Context) error {
	if err := index.removeMetadataFile(); err != nil {
		return err
	}
	// Shard::drop will take care of handling store's buckets
	return nil
}

func (index *flat) Flush() error {
	// nothing to do here
	// Shard will take care of handling store's buckets
	return nil
}

func (index *flat) Shutdown(ctx context.Context) error {
	// Shard::shutdown will take care of handling store's buckets
	return nil
}

func (index *flat) SwitchCommitLogs(context.Context) error {
	return nil
}

func (index *flat) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	var files []string

	metadataFile := index.getMetadataFile()
	fullPath := filepath.Join(index.rootPath, metadataFile)

	if _, err := os.Stat(fullPath); err == nil {
		relPath, err := filepath.Rel(basePath, fullPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get relative path: %w", err)
		}
		// If the file doesn't exist, we simply don't add it to the list
		files = append(files, relPath)
	}

	return files, nil
}

func (index *flat) GetKeys(id uint64) (uint64, uint64, error) {
	return 0, 0, errors.Errorf("GetKeys is not supported for flat index")
}

func (index *flat) ValidateBeforeInsert(vector []float32) error {
	if len(vector) == 0 {
		return errors.Errorf("cannot insert vector of dimension 0")
	}

	dims := int(atomic.LoadInt32(&index.dims))

	// no vectors exist
	if dims == 0 {
		return nil
	}

	// check if vector length is the same as existing nodes
	if dims != len(vector) {
		return errors.Errorf("insert called with a vector of the wrong size: %d. Saved length: %d, path: %s",
			len(vector), dims, index.rootPath)
	}

	return nil
}

func (index *flat) Preload(id uint64, vector []float32) {
	if index.Compressed() {
		if index.quantizer.Type() == ByteQuantizer {
			// For byte quantizer
			vectorQuantized := index.quantizer.EncodeBytes(vector)
			if index.Cached() {
				index.cache.Grow(id)
				index.cache.PreloadBytes(id, vectorQuantized)
			}
			index.storeCompressedVector(id, vectorQuantized)
		} else if index.quantizer.Type() == Uint64Quantizer {
			// For uint64 quantizer
			vectorQuantized := index.quantizer.EncodeUint64(vector)
			if index.Cached() {
				index.cache.Grow(id)
				index.cache.PreloadUint64(id, vectorQuantized)
			}
			slice := make([]byte, len(vectorQuantized)*8)
			index.storeCompressedVector(id, byteSliceFromUint64Slice(vectorQuantized, slice))
		}
	}
}

func (index *flat) PostStartup() {
	if !index.Cached() || index.quantizer == nil {
		return
	}

	// The idea here is to first read everything from disk in one go, then grow
	// the cache just once before inserting all vectors. A previous iteration
	// would grow the cache as part of the cursor loop and this ended up making
	// up 75% of the CPU time needed. This new implementation with two loops is
	// much more efficient and only ever-so-slightly more memory-consuming (about
	// one additional struct per vector while loading. Should be negligible)

	before := time.Now()
	bucket := index.store.Bucket(index.getCompressedBucketName())
	// we expect to be IO-bound, so more goroutines than CPUs is fine, we do
	// however want some kind of relationship to the machine size, so
	// 2*GOMAXPROCS seems like a good default.

	var maxID uint64
	var count int

	// Use the quantizer's FromCompressedBytes method with generic parallel iterator
	if index.quantizer.Type() == Uint64Quantizer {
		// The initial size of 10k is chosen fairly arbitrarily. The cost of growing
		// this slice dynamically should be quite cheap compared to other operations
		// involved here, e.g. disk reads.
		vecs := make([]compressionhelpers.VecAndID[uint64], 0, 10_000)

		it := compressionhelpers.NewParallelIterator(bucket, 2*runtime.GOMAXPROCS(0),
			binary.BigEndian.Uint64, func(compressed []byte, buf *[]uint64) []uint64 {
				return index.quantizer.FromCompressedBytesToUint64(compressed, buf)
			}, index.logger)
		channel := it.IterateAll()
		if channel == nil {
			return // nothing to do
		}
		for vectors := range channel {
			for _, v := range vectors {
				// if we mix little and big endian IDs by mistake, we might get a very large
				// maxID which would cause us to allocate a huge cache.
				// In that case, we consider that anything larger than a quadrillion is an error
				// and should be skipped.
				if v.Id > 1e15 {
					continue
				}

				vecs = append(vecs, v)
			}
		}

		for i := range vecs {
			count++
			if vecs[i].Id > maxID {
				maxID = vecs[i].Id
			}
		}
	} else if index.quantizer.Type() == ByteQuantizer {
		// The initial size of 10k is chosen fairly arbitrarily. The cost of growing
		// this slice dynamically should be quite cheap compared to other operations
		// involved here, e.g. disk reads.
		vecs := make([]compressionhelpers.VecAndID[byte], 0, 10_000)

		it := compressionhelpers.NewParallelIterator(bucket, 2*runtime.GOMAXPROCS(0),
			binary.BigEndian.Uint64, func(compressed []byte, buf *[]byte) []byte {
				return index.quantizer.FromCompressedBytesToBytes(compressed, buf)
			}, index.logger)
		channel := it.IterateAll()
		if channel == nil {
			return // nothing to do
		}
		for vectors := range channel {
			for _, v := range vectors {
				// if we mix little and big endian IDs by mistake, we might get a very large
				// maxID which would cause us to allocate a huge cache.
				// In that case, we consider that anything larger than a quadrillion is an error
				// and should be skipped.
				if v.Id > 1e15 {
					continue
				}

				vecs = append(vecs, v)
			}
		}

		for i := range vecs {
			count++
			if vecs[i].Id > maxID {
				maxID = vecs[i].Id
			}
		}
	}

	// Grow cache just once
	index.cache.LockAll()
	defer index.cache.UnlockAll()

	if index.quantizer.Type() == Uint64Quantizer {
		index.cache.SetSizeAndGrowNoLockUint64(maxID)
		// Re-iterate to preload cache
		it := compressionhelpers.NewParallelIterator(bucket, 2*runtime.GOMAXPROCS(0),
			binary.BigEndian.Uint64, func(compressed []byte, buf *[]uint64) []uint64 {
				return index.quantizer.FromCompressedBytesToUint64(compressed, buf)
			}, index.logger)
		channel := it.IterateAll()
		if channel != nil {
			for v := range channel {
				for _, vec := range v {
					index.cache.PreloadNoLockUint64(vec.Id, vec.Vec)
				}
			}
		}
	} else if index.quantizer.Type() == ByteQuantizer {
		index.cache.SetSizeAndGrowNoLockBytes(maxID)
		// Re-iterate to preload cache
		it := compressionhelpers.NewParallelIterator(bucket, 2*runtime.GOMAXPROCS(0),
			binary.BigEndian.Uint64, func(compressed []byte, buf *[]byte) []byte {
				return index.quantizer.FromCompressedBytesToBytes(compressed, buf)
			}, index.logger)
		channel := it.IterateAll()
		if channel != nil {
			for v := range channel {
				for _, vec := range v {
					index.cache.PreloadNoLockBytes(vec.Id, vec.Vec)
				}
			}
		}
	}

	took := time.Since(before)
	index.logger.WithFields(logrus.Fields{
		"action":   "preload_cache",
		"type":     index.compressionType.String(),
		"count":    count,
		"took":     took,
		"index_id": index.id,
	}).Debugf("pre-loaded %d vectors in %s", count, took)
}

func (index *flat) ContainsDoc(id uint64) bool {
	var bucketName string

	// logic modeled after SearchByVector which indicates that the PQ bucket is
	// the same as the uncompressed bucket "for now"
	switch index.compressionType {
	case CompressionBQ, CompressionRQ1, CompressionRQ8:
		bucketName = index.getCompressedBucketName()
	default:
		bucketName = index.getBucketName()
	}

	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	v, err := index.store.Bucket(bucketName).Get(idBytes)
	if v == nil || errors.Is(err, entlsmkv.NotFound) {
		return false
	}

	return true
}

func (index *flat) Iterate(fn func(docID uint64) bool) {
	var bucketName string

	// logic modeled after SearchByVector which indicates that the PQ bucket is
	// the same as the uncompressed bucket "for now"
	switch index.compressionType {
	case CompressionBQ, CompressionRQ1, CompressionRQ8:
		bucketName = index.getCompressedBucketName()
	default:
		bucketName = index.getBucketName()
	}

	bucket := index.store.Bucket(bucketName)
	cursor := bucket.Cursor()
	defer cursor.Close()

	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		id := binary.BigEndian.Uint64(key)
		if !fn(id) {
			break
		}
	}
}

func newSearchByDistParams(maxLimit int64) *common.SearchByDistParams {
	initialOffset := 0
	initialLimit := common.DefaultSearchByDistInitialLimit

	return common.NewSearchByDistParams(initialOffset, initialLimit, initialOffset+initialLimit, maxLimit)
}

type immutableParameter struct {
	accessor func(c flatent.UserConfig) interface{}
	name     string
}

func validateImmutableField(u immutableParameter,
	previous, next flatent.UserConfig,
) error {
	oldField := u.accessor(previous)
	newField := u.accessor(next)
	if oldField != newField {
		return errors.Errorf("%s is immutable: attempted change from \"%v\" to \"%v\"",
			u.name, oldField, newField)
	}

	return nil
}

func ValidateUserConfigUpdate(initial, updated schemaConfig.VectorIndexConfig) error {
	initialParsed, ok := initial.(flatent.UserConfig)
	if !ok {
		return errors.Errorf("initial is not UserConfig, but %T", initial)
	}

	updatedParsed, ok := updated.(flatent.UserConfig)
	if !ok {
		return errors.Errorf("updated is not UserConfig, but %T", updated)
	}

	immutableFields := []immutableParameter{
		{
			name:     "distance",
			accessor: func(c flatent.UserConfig) interface{} { return c.Distance },
		},
		{
			name:     "pq.cache",
			accessor: func(c flatent.UserConfig) interface{} { return c.PQ.Cache },
		},
		{
			name:     "pq",
			accessor: func(c flatent.UserConfig) interface{} { return c.PQ.Enabled },
		},
		{
			name:     "bq",
			accessor: func(c flatent.UserConfig) interface{} { return c.BQ.Enabled },
		},
		{
			name:     "rq",
			accessor: func(c flatent.UserConfig) interface{} { return c.RQ.Enabled },
		},
		{
			name:     "rq.bits",
			accessor: func(c flatent.UserConfig) interface{} { return c.RQ.Bits },
		},
		// as of v1.25.2, updating the BQ cache setting is now possible.
		// Note that the change does not take effect until the tenant is
		// reloaded, either from a complete restart or from
		// activating/deactivating it.
	}

	for _, u := range immutableFields {
		if err := validateImmutableField(u, initialParsed, updatedParsed); err != nil {
			return err
		}
	}
	return nil
}

func (index *flat) AlreadyIndexed() uint64 {
	return atomic.LoadUint64(&index.count)
}

func (index *flat) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	var distFunc func(nodeID uint64) (float32, error)
	queryVector = index.normalized(queryVector)
	defaultDistFunc := func(nodeID uint64) (float32, error) {
		vec, err := index.vectorById(nodeID)
		if err != nil {
			return 0, err
		}
		dist, err := index.distancerProvider.SingleDist(queryVector, float32SliceFromByteSlice(vec, make([]float32, len(vec)/4)))
		if err != nil {
			return 0, err
		}
		return dist, nil
	}
	switch index.compressionType {
	case CompressionBQ, CompressionRQ1, CompressionRQ8:
		if index.cache == nil {
			distFunc = defaultDistFunc
		} else {
			// For RQ-1 bit, use NewDistancer to get 5-bit query quantization for better accuracy
			// This matches HNSW behavior where queries use higher precision than data vectors
			if index.compressionType == CompressionRQ1 && index.quantizer.Type() == Uint64Quantizer {
				// Create a distancer that uses 5-bit query quantization
				distancer := index.quantizer.(*BinaryRotationalQuantizerWrapper).NewDistancer(queryVector)
				distFunc = func(nodeID uint64) (float32, error) {
					if int32(nodeID) > index.cache.Len() {
						return -1, fmt.Errorf("node %v is larger than the cache size %v", nodeID, index.cache.Len())
					}
					vec, err := index.cache.uint64Cache.Get(context.Background(), nodeID)
					if err != nil {
						return 0, err
					}
					return distancer.Distance(vec)
				}
			} else {
				// Pre-encode query vector once for performance (for other quantizers)
				var queryVecEncodeUint64 []uint64
				var queryVecEncodeBytes []byte

				if index.quantizer.Type() == Uint64Quantizer {
					queryVecEncodeUint64 = index.quantizer.EncodeUint64(queryVector)
				} else if index.quantizer.Type() == ByteQuantizer {
					queryVecEncodeBytes = index.quantizer.EncodeBytes(queryVector)
				}

				distFunc = func(nodeID uint64) (float32, error) {
					if int32(nodeID) > index.cache.Len() {
						return -1, fmt.Errorf("node %v is larger than the cache size %v", nodeID, index.cache.Len())
					}
					if index.quantizer.Type() == Uint64Quantizer {
						vec, err := index.cache.uint64Cache.Get(context.Background(), nodeID)
						if err != nil {
							return 0, err
						}
						return index.quantizer.DistanceBetweenUint64Vectors(vec, queryVecEncodeUint64)
					} else if index.quantizer.Type() == ByteQuantizer {
						vec, err := index.cache.byteCache.Get(context.Background(), nodeID)
						if err != nil {
							return 0, err
						}
						return index.quantizer.DistanceBetweenByteVectors(vec, queryVecEncodeBytes)
					}
					return 0, fmt.Errorf("unsupported quantizer data type: %v", index.quantizer.Type())
				}
			}
		}
	default:
		distFunc = func(nodeID uint64) (float32, error) {
			vec, err := index.vectorById(nodeID)
			if err != nil {
				return 0, err
			}
			dist, err := index.distancerProvider.SingleDist(queryVector, float32SliceFromByteSlice(vec, make([]float32, len(vec)/4)))
			if err != nil {
				return 0, err
			}
			return dist, nil
		}
	}
	return common.QueryVectorDistancer{DistanceFunc: distFunc}
}

func (index *flat) Type() common.IndexType {
	return common.IndexTypeFlat
}

func (index *flat) CompressionStats() compressionhelpers.CompressionStats {
	// Flat index doesn't have detailed compression stats, return uncompressed stats
	return compressionhelpers.UncompressedStats{}
}

func (h *flat) ShouldUpgrade() (bool, int) {
	return false, 0
}

func (h *flat) Upgrade(callback func()) error {
	return nil
}

func (h *flat) Upgraded() bool {
	return false
}
