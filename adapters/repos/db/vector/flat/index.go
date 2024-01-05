//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/schema"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

const (
	compressionBQ   = "bq"
	compressionPQ   = "pq"
	compressionNone = "none"
)

type flat struct {
	sync.Mutex
	id                  string
	dims                int32
	store               *lsmkv.Store
	logger              logrus.FieldLogger
	distancerProvider   distancer.Provider
	trackDimensionsOnce sync.Once
	rescore             int64
	bq                  compressionhelpers.BinaryQuantizer

	pqResults *common.PqMaxPool
	pool      *pools

	compression string
	bqCache     cache.Cache[uint64]
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
		id:                cfg.ID,
		logger:            logger,
		distancerProvider: cfg.DistanceProvider,
		rescore:           extractCompressionRescore(uc),
		pqResults:         common.NewPqMaxPool(100),
		compression:       extractCompression(uc),
		pool:              newPools(),
		store:             store,
	}
	index.initBuckets(context.Background())
	if uc.BQ.Enabled && uc.BQ.Cache {
		index.bqCache = cache.NewShardedUInt64LockCache(index.getBQVector, uc.VectorCacheMaxObjects, cfg.Logger, 0)
	}

	return index, nil
}

func (flat *flat) getBQVector(ctx context.Context, id uint64) ([]uint64, error) {
	key := flat.pool.byteSlicePool.Get(8)
	defer flat.pool.byteSlicePool.Put(key)
	binary.BigEndian.PutUint64(key.slice, id)
	bytes, err := flat.store.Bucket(helpers.VectorsCompressedBucketLSM).Get(key.slice)
	if err != nil {
		return nil, err
	}
	return uint64SliceFromByteSlice(bytes, make([]uint64, len(bytes)/8)), nil
}

func extractCompression(uc flatent.UserConfig) string {
	if uc.BQ.Enabled && uc.PQ.Enabled {
		return compressionNone
	}

	if uc.BQ.Enabled {
		return compressionBQ
	}

	if uc.PQ.Enabled {
		return compressionPQ
	}

	return compressionNone
}

func extractCompressionRescore(uc flatent.UserConfig) int64 {
	compression := extractCompression(uc)
	switch compression {
	case compressionPQ:
		return int64(uc.PQ.RescoreLimit)
	case compressionBQ:
		return int64(uc.BQ.RescoreLimit)
	default:
		return 0
	}
}

func (index *flat) storeCompressedVector(id uint64, vector []byte) {
	index.storeGenericVector(id, vector, helpers.VectorsCompressedBucketLSM)
}

func (index *flat) storeVector(id uint64, vector []byte) {
	index.storeGenericVector(id, vector, helpers.VectorsBucketLSM)
}

func (index *flat) storeGenericVector(id uint64, vector []byte, bucket string) {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	index.store.Bucket(bucket).Put(idBytes, vector)
}

func (index *flat) isBQ() bool {
	return index.compression == compressionBQ
}

func (index *flat) isBQCached() bool {
	return index.bqCache != nil
}

func (index *flat) Compressed() bool {
	return index.compression != compressionNone
}

func (index *flat) initBuckets(ctx context.Context) error {
	if err := index.store.CreateOrLoadBucket(ctx, helpers.VectorsBucketLSM,
		lsmkv.WithForceCompation(true),
		lsmkv.WithUseBloomFilter(false),
		lsmkv.WithCalcCountNetAdditions(false),
	); err != nil {
		return fmt.Errorf("Create or load flat vectors bucket: %w", err)
	}
	if index.isBQ() {
		if err := index.store.CreateOrLoadBucket(ctx, helpers.VectorsCompressedBucketLSM,
			lsmkv.WithForceCompation(true),
			lsmkv.WithUseBloomFilter(false),
			lsmkv.WithCalcCountNetAdditions(false),
		); err != nil {
			return fmt.Errorf("Create or load flat compressed vectors bucket: %w", err)
		}
	}
	return nil
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
		if err := index.Add(ids[i], vectors[i]); err != nil {
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

func (index *flat) Add(id uint64, vector []float32) error {
	index.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&index.dims, int32(len(vector)))

		if index.isBQ() {
			index.bq = compressionhelpers.NewBinaryQuantizer(nil)
		}
	})
	if len(vector) != int(index.dims) {
		return errors.Errorf("insert called with a vector of the wrong size")
	}
	vector = index.normalized(vector)
	slice := make([]byte, len(vector)*4)
	index.storeVector(id, byteSliceFromFloat32Slice(vector, slice))

	if index.isBQ() {
		vectorBQ := index.bq.Encode(vector)
		if index.isBQCached() {
			index.bqCache.Grow(id)
			index.bqCache.Preload(id, vectorBQ)
		}
		slice = make([]byte, len(vectorBQ)*8)
		index.storeCompressedVector(id, byteSliceFromUint64Slice(vectorBQ, slice))
	}
	return nil
}

func (index *flat) Delete(ids ...uint64) error {
	for i := range ids {
		if index.isBQCached() {
			index.bqCache.Delete(context.Background(), ids[i])
		}
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, ids[i])

		if err := index.store.Bucket(helpers.VectorsBucketLSM).Delete(idBytes); err != nil {
			return err
		}

		if index.isBQ() {
			if err := index.store.Bucket(helpers.VectorsCompressedBucketLSM).Delete(idBytes); err != nil {
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

func (index *flat) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	switch index.compression {
	case compressionBQ:
		return index.searchByVectorBQ(vector, k, allow)
	case compressionPQ:
		// use uncompressed for now
		fallthrough
	default:
		return index.searchByVector(vector, k, allow)
	}
}

func (index *flat) searchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	heap := index.pqResults.GetMax(k)
	defer index.pqResults.Put(heap)

	vector = index.normalized(vector)

	if err := index.findTopVectors(heap, allow, k,
		index.store.Bucket(helpers.VectorsBucketLSM).Cursor,
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
		distance, _, err := index.distancerProvider.SingleDist(vector, candidate)
		return distance, err
	}
}

func (index *flat) searchByVectorBQ(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	rescore := index.searchTimeRescore(k)
	heap := index.pqResults.GetMax(rescore)
	defer index.pqResults.Put(heap)

	vector = index.normalized(vector)
	vectorBQ := index.bq.Encode(vector)

	if index.isBQCached() {
		if err := index.findTopVectorsCached(heap, allow, rescore, vectorBQ); err != nil {
			return nil, nil, err
		}
	} else {
		if err := index.findTopVectors(heap, allow, rescore,
			index.store.Bucket(helpers.VectorsCompressedBucketLSM).Cursor,
			index.createDistanceCalcBQ(vectorBQ),
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
	for _, id := range idsSlice.slice {
		candidateAsBytes, err := index.vectorById(id)
		if err != nil {
			return nil, nil, err
		}
		distance, err := distanceCalc(candidateAsBytes)
		if err != nil {
			return nil, nil, err
		}
		index.insertToHeap(heap, k, id, distance)
	}

	ids, dists := index.extractHeap(heap)
	return ids, dists, nil
}

func (index *flat) createDistanceCalcBQ(vectorBQ []uint64) distanceCalc {
	return func(vecAsBytes []byte) (float32, error) {
		vecSliceBQ := index.pool.uint64SlicePool.Get(len(vecAsBytes) / 8)
		defer index.pool.uint64SlicePool.Put(vecSliceBQ)

		candidate := uint64SliceFromByteSlice(vecAsBytes, vecSliceBQ.slice)
		return index.bq.DistanceBetweenCompressedVectors(candidate, vectorBQ)
	}
}

func (index *flat) vectorById(id uint64) ([]byte, error) {
	idSlice := index.pool.byteSlicePool.Get(8)
	defer index.pool.byteSlicePool.Put(idSlice)

	binary.BigEndian.PutUint64(idSlice.slice, id)
	return index.store.Bucket(helpers.VectorsBucketLSM).Get(idSlice.slice)
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
// distanceCalc
func (index *flat) findTopVectorsCached(heap *priorityqueue.Queue[any],
	allow helpers.AllowList, limit int, vectorBQ []uint64,
) error {
	var id uint64
	allowMax := uint64(0)

	if allow != nil {
		// nothing allowed, skip search
		if allow.IsEmpty() {
			return nil
		}

		allowMax = allow.Max()

		id = allow.Min()
	} else {
		id = 0
	}
	all := index.bqCache.Len()

	// since keys are sorted, once key/id get greater than max allowed one
	// further search can be stopped
	for ; id < uint64(all) && (allow == nil || id <= allowMax); id++ {
		if allow == nil || allow.Contains(id) {
			vec, err := index.bqCache.Get(context.Background(), id)
			if err != nil {
				return err
			}
			if len(vec) == 0 {
				continue
			}
			distance, err := index.bq.DistanceBetweenCompressedVectors(vec, vectorBQ)
			if err != nil {
				return err
			}
			index.insertToHeap(heap, limit, id, distance)
		}
	}
	return nil
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

func (index *flat) SearchByVectorDistance(vector []float32, targetDistance float32, maxLimit int64, allow helpers.AllowList) ([]uint64, []float32, error) {
	var (
		searchParams = newSearchByDistParams(maxLimit)

		resultIDs  []uint64
		resultDist []float32
	)

	recursiveSearch := func() (bool, error) {
		totalLimit := searchParams.TotalLimit()
		ids, dist, err := index.SearchByVector(vector, totalLimit, allow)
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

func (index *flat) UpdateUserConfig(updated schema.VectorIndexConfig, callback func()) error {
	parsed, ok := updated.(flatent.UserConfig)
	if !ok {
		callback()
		return errors.Errorf("config is not UserConfig, but %T", updated)
	}

	// Store automatically as a lock here would be very expensive, this value is
	// read on every single user-facing search, which can be highly concurrent
	atomic.StoreInt64(&index.rescore, extractCompressionRescore(parsed))

	callback()
	return nil
}

func (index *flat) Drop(ctx context.Context) error {
	// nothing to do here
	// Shard::drop will take care of handling store's buckets
	return nil
}

func (index *flat) Flush() error {
	// nothing to do here
	// Shard will take care of handling store's buckets
	return nil
}

func (index *flat) Shutdown(ctx context.Context) error {
	// nothing to do here
	// Shard::shutdown will take care of handling store's buckets
	return nil
}

func (index *flat) SwitchCommitLogs(context.Context) error {
	return nil
}

func (index *flat) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	// nothing to do here
	// Shard::ListBackupFiles will take care of handling store's buckets
	return []string{}, nil
}

func (i *flat) ValidateBeforeInsert(vector []float32) error {
	return nil
}

func (index *flat) PostStartup() {
	if !index.isBQCached() {
		return
	}
	cursor := index.store.Bucket(helpers.VectorsCompressedBucketLSM).Cursor()
	defer cursor.Close()

	for key, v := cursor.First(); key != nil; key, v = cursor.Next() {
		id := binary.BigEndian.Uint64(key)
		index.bqCache.Preload(id, uint64SliceFromByteSlice(v, make([]uint64, len(v)/8)))
	}
}

func (index *flat) Dump(labels ...string) {
	if len(labels) > 0 {
		fmt.Printf("--------------------------------------------------\n")
		fmt.Printf("--  %s\n", strings.Join(labels, ", "))
	}
	fmt.Printf("--------------------------------------------------\n")
	fmt.Printf("ID: %s\n", index.id)
	fmt.Printf("--------------------------------------------------\n")
}

func (index *flat) DistanceBetweenVectors(x, y []float32) (float32, bool, error) {
	return index.distancerProvider.SingleDist(x, y)
}

func (index *flat) ContainsNode(id uint64) bool {
	return true
}

func (index *flat) DistancerProvider() distancer.Provider {
	return index.distancerProvider
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

func ValidateUserConfigUpdate(initial, updated schema.VectorIndexConfig) error {
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
			name:     "bq.cache",
			accessor: func(c flatent.UserConfig) interface{} { return c.BQ.Cache },
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
	}

	for _, u := range immutableFields {
		if err := validateImmutableField(u, initialParsed, updatedParsed); err != nil {
			return err
		}
	}
	return nil
}
