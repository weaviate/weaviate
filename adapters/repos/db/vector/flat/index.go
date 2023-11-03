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

package flat

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

type flat struct {
	id                  string
	dims                int32
	store               *lsmkv.Store
	logger              logrus.FieldLogger
	distancerProvider   distancer.Provider
	shardName           string
	trackDimensionsOnce sync.Once
	ef                  int64
	bq                  ssdhelpers.BinaryQuantizer

	tempVectors *common.TempVectorsPool
	pqResults   *common.PqMaxPool
	pool        *pools

	shardCompactionCallbacks cyclemanager.CycleCallbackGroup
	shardFlushCallbacks      cyclemanager.CycleCallbackGroup

	compression string
}

func New(
	cfg hnsw.Config, uc flatent.UserConfig,
	shardCompactionCallbacks, shardFlushCallbacks cyclemanager.CycleCallbackGroup,
) (*flat, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	if cfg.Logger == nil {
		logger := logrus.New()
		logger.Out = io.Discard
		cfg.Logger = logger
	}

	index := &flat{
		id:                       cfg.ID,
		logger:                   cfg.Logger,
		distancerProvider:        cfg.DistanceProvider,
		ef:                       int64(uc.EF),
		shardName:                cfg.ShardName,
		tempVectors:              common.NewTempVectorsPool(),
		pqResults:                common.NewPqMaxPool(100),
		compression:              uc.Compression,
		shardCompactionCallbacks: shardCompactionCallbacks,
		shardFlushCallbacks:      shardFlushCallbacks,
		pool:                     newPools(),
	}
	index.initStore(cfg.RootPath, cfg.ClassName)

	return index, nil
}

func (h *flat) storeCompressedVector(index uint64, vector []byte) {
	h.storeGenericVector(index, vector, helpers.CompressedObjectsBucketLSM)
}

func (h *flat) storeVector(index uint64, vector []byte) {
	h.storeGenericVector(index, vector, helpers.ObjectsBucketLSM)
}

func (h *flat) storeGenericVector(index uint64, vector []byte, bucket string) {
	Id := make([]byte, 8)
	binary.BigEndian.PutUint64(Id, index)
	h.store.Bucket(bucket).Put(Id, vector)
}

func (index *flat) initStore(rootPath, className string) error {
	ctx := context.Background()

	storeDir := path.Join(rootPath, "flat")
	store, err := lsmkv.New(storeDir, rootPath, index.logger, nil,
		index.shardCompactionCallbacks, index.shardFlushCallbacks)
	if err != nil {
		return errors.Wrap(err, "Init lsmkv (compressed vectors store)")
	}
	err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM, lsmkv.WithForceCompation(true))
	if err != nil {
		return errors.Wrapf(err, "Create or load bucket (vectors store)")
	}
	if index.compression != flatent.CompressionNone {
		err = store.CreateOrLoadBucket(ctx, helpers.CompressedObjectsBucketLSM, lsmkv.WithForceCompation(true))
		if err != nil {
			return errors.Wrapf(err, "Create or load bucket (compressed vectors store)")
		}
	}
	index.store = store
	return nil
}

func (index *flat) AddBatch(ids []uint64, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return errors.Errorf("ids and vectors sizes does not match")
	}
	if len(ids) == 0 {
		return errors.Errorf("insertBatch called with empty lists")
	}
	for idx := range ids {
		if err := index.Add(ids[idx], vectors[idx]); err != nil {
			return err
		}
	}
	return nil
}

func byteSliceFromUint64Slice(x []uint64, slice []byte) []byte {
	for i := range x {
		binary.LittleEndian.PutUint64(slice[i*8:], x[i])
	}
	return slice
}

func byteSliceFromFloat32Slice(x []float32, slice []byte) []byte {
	for i := range x {
		binary.LittleEndian.PutUint32(slice[i*4:], math.Float32bits(x[i]))
	}
	return slice
}

func uint64SliceFromByteSlice(x []byte, slice []uint64) []uint64 {
	len := len(x) / 8
	for i := 0; i < len; i++ {
		slice[i] = binary.LittleEndian.Uint64(x[i*8:])
	}
	return slice
}

func float32SliceFromByteSlice(x []byte, slice []float32) []float32 {
	len := len(x) / 4
	for i := 0; i < len; i++ {
		slice[i] = math.Float32frombits(binary.LittleEndian.Uint32(x[i*4:]))
	}
	return slice
}

func (index *flat) Add(id uint64, vector []float32) error {
	index.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&index.dims, int32(len(vector)))
		if index.compression == flatent.CompressionNone {
			return
		}
		index.bq = ssdhelpers.NewBinaryQuantizer()
	})
	if len(vector) != int(index.dims) {
		return errors.Errorf("insert called with a vector of the wrong size")
	}
	vector = index.normalized(vector)
	slice := make([]byte, len(vector)*4)
	index.storeVector(id, byteSliceFromFloat32Slice(vector, slice))
	if index.compression == flatent.CompressionNone {
		return nil
	}
	vec, err := index.bq.Encode(vector)
	if err != nil {
		return err
	}
	slice = make([]byte, len(vec)*8)
	index.storeCompressedVector(id, byteSliceFromUint64Slice(vec, slice))

	return nil
}

func (index *flat) Delete(ids ...uint64) error {
	for _, i := range ids {
		id := make([]byte, 8)
		binary.BigEndian.PutUint64(id, uint64(i))
		err := index.store.Bucket(helpers.ObjectsBucketLSM).Delete(id)
		if err != nil {
			return err
		}
		if index.compression == flatent.CompressionNone {
			continue
		}
		err = index.store.Bucket(helpers.CompressedObjectsBucketLSM).Delete(id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (index *flat) searchTimeEF(k int) int {
	// load atomically, so we can get away with concurrent updates of the
	// userconfig without having to set a lock each time we try to read - which
	// can be so common that it would cause considerable overhead
	ef := int(atomic.LoadInt64(&index.ef))
	if ef < k {
		ef = k
	}
	return ef
}

func (index *flat) SearchByVector(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	switch index.compression {
	case flatent.CompressionBQ:
		return index.searchByVectorBQ(vector, k, allow)
	case flatent.CompressionPQ:
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
		index.store.Bucket(helpers.ObjectsBucketLSM).Cursor,
		index.distanceFn(vector),
	); err != nil {
		return nil, nil, err
	}

	ids, dists := index.extractHeap(heap)
	return ids, dists, nil
}

func (index *flat) distanceFn(vector []float32) func(vecAsBytes []byte) (float32, error) {
	return func(vecAsBytes []byte) (float32, error) {
		vecSlice := index.pool.float32SlicePool.Get(len(vecAsBytes) / 4)
		defer index.pool.float32SlicePool.Put(vecSlice)

		candidate := float32SliceFromByteSlice(vecAsBytes, vecSlice.slice)
		distance, _, err := index.distancerProvider.SingleDist(vector, candidate)
		return distance, err
	}
}

func (index *flat) searchByVectorBQ(vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	ef := index.searchTimeEF(k)
	heap := index.pqResults.GetMax(ef)
	defer index.pqResults.Put(heap)

	vector = index.normalized(vector)
	vectorBQ, err := index.bq.Encode(vector)
	if err != nil {
		return nil, nil, err
	}

	if err := index.findTopVectors(heap, allow, ef,
		index.store.Bucket(helpers.CompressedObjectsBucketLSM).Cursor,
		index.distanceFnBQ(vectorBQ),
	); err != nil {
		return nil, nil, err
	}

	distanceFn := index.distanceFn(vector)
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
		distance, err := distanceFn(candidateAsBytes)
		if err != nil {
			return nil, nil, err
		}
		index.insertToHeap(heap, k, id, distance)
	}

	ids, dists := index.extractHeap(heap)
	return ids, dists, nil
}

func (index *flat) distanceFnBQ(vectorBQ []uint64) func(vecAsBytes []byte) (float32, error) {
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
	return index.store.Bucket(helpers.ObjectsBucketLSM).Get(idSlice.slice)
}

// populates given heap with smallest distances and corresponding ids calculated by
// distanceFn
func (index *flat) findTopVectors(heap *priorityqueue.Queue, allow helpers.AllowList, limit int,
	cursorFn func() *lsmkv.CursorReplace, distanceFn func(vecAsBytes []byte) (float32, error),
) error {
	var key []byte
	var v []byte
	var id uint64
	found := 0
	allowLen := 0

	cursor := cursorFn()
	defer cursor.Close()

	if allow != nil {
		allowLen = allow.Len()
		firstId, _ := allow.Iterator().Next()

		idSlice := index.pool.byteSlicePool.Get(8)
		binary.BigEndian.PutUint64(idSlice.slice, firstId)
		key, v = cursor.Seek(idSlice.slice)
		index.pool.byteSlicePool.Put(idSlice)
	} else {
		key, v = cursor.First()
	}

	for key != nil && (allow == nil || found < allowLen) {
		id = binary.BigEndian.Uint64(key)
		if allow == nil || allow.Contains(id) {
			found++
			distance, err := distanceFn(v)
			if err != nil {
				return err
			}
			index.insertToHeap(heap, limit, id, distance)
		}
		key, v = cursor.Next()
	}
	return nil
}

func (index *flat) insertToHeap(heap *priorityqueue.Queue, limit int, id uint64, distance float32) {
	if heap.Len() < limit {
		heap.Insert(id, distance)
	} else if heap.Top().Dist > distance {
		heap.Pop()
		heap.Insert(id, distance)
	}
}

func (index *flat) extractHeap(heap *priorityqueue.Queue) ([]uint64, []float32) {
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
		shouldContinue := false

		ids, dist, err := index.SearchByVector(vector, searchParams.TotalLimit(), allow)
		if err != nil {
			return false, errors.Wrap(err, "vector search")
		}

		// ensures the indexers aren't out of range
		offsetCap := searchParams.OffsetCapacity(ids)
		totalLimitCap := searchParams.TotalLimitCapacity(ids)

		ids, dist = ids[offsetCap:totalLimitCap], dist[offsetCap:totalLimitCap]

		if len(ids) == 0 {
			return false, nil
		}

		lastFound := dist[len(dist)-1]
		shouldContinue = lastFound <= targetDistance

		for i := range ids {
			if aboveThresh := dist[i] <= targetDistance; aboveThresh ||
				floatcomp.InDelta(float64(dist[i]), float64(targetDistance), 1e-6) {
				resultIDs = append(resultIDs, ids[i])
				resultDist = append(resultDist, dist[i])
			} else {
				// as soon as we encounter a certainty which
				// is below threshold, we can stop searching
				break
			}
		}

		return shouldContinue, nil
	}

	shouldContinue, err := recursiveSearch()
	if err != nil {
		return nil, nil, err
	}

	for shouldContinue {
		searchParams.Iterate()
		if searchParams.MaxLimitReached() {
			index.logger.
				WithField("action", "unlimited_vector_search").
				Warnf("maximum search limit of %d results has been reached",
					searchParams.MaximumSearchLimit())
			break
		}

		shouldContinue, err = recursiveSearch()
		if err != nil {
			return nil, nil, err
		}
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
	atomic.StoreInt64(&index.ef, int64(parsed.EF))

	callback()
	return nil
}

func (index *flat) Drop(ctx context.Context) error {
	return nil
}

func (index *flat) Flush() error {
	return nil
}

func (index *flat) Shutdown(ctx context.Context) error {
	if err := index.store.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "flat shutdown")
	}
	return index.Drop(ctx)
}

func (index *flat) SwitchCommitLogs(context.Context) error {
	return nil
}

func (index *flat) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return nil, nil
}

func (i *flat) ValidateBeforeInsert(vector []float32) error {
	return nil
}

func (index *flat) PostStartup() {
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

func ValidateUserConfigUpdate(initial, updated schema.VectorIndexConfig) error {
	/*
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
		}

		for _, u := range immutableFields {
			if err := validateImmutableField(u, initialParsed, updatedParsed); err != nil {
				return err
			}
		}
	*/
	return nil
}
