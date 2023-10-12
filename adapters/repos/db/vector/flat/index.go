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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
	vectorindexcommon "github.com/weaviate/weaviate/entities/vectorindex/common"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/usecases/floatcomp"
)

type flat struct {
	sync.RWMutex
	id                  string
	dims                int32
	compressedCache     cache.Cache[uint64]
	store               *lsmkv.Store
	logger              logrus.FieldLogger
	distancerProvider   distancer.Provider
	shardName           string
	trackDimensionsOnce sync.Once
	nodes               map[uint64]interface{}
	ef                  int64

	TempVectorForIDThunk common.TempVectorForID
	vectorForID          common.VectorForID[float32]
	bq                   ssdhelpers.BinaryQuantizer
	maxId                uint64

	tempVectors *common.TempVectorsPool
	pqResults   *common.PqMaxPool
	fullyOnDisk bool

	shardCompactionCallbacks cyclemanager.CycleCallbackGroup
	shardFlushCallbacks      cyclemanager.CycleCallbackGroup
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
		nodes:                    make(map[uint64]interface{}),
		id:                       cfg.ID,
		logger:                   cfg.Logger,
		distancerProvider:        cfg.DistanceProvider,
		ef:                       int64(uc.EF),
		shardName:                cfg.ShardName,
		TempVectorForIDThunk:     cfg.TempVectorForIDThunk,
		vectorForID:              cfg.VectorForIDThunk,
		tempVectors:              common.NewTempVectorsPool(),
		pqResults:                common.NewPqMaxPool(100),
		fullyOnDisk:              uc.FullyOnDisk,
		shardCompactionCallbacks: shardCompactionCallbacks,
		shardFlushCallbacks:      shardFlushCallbacks,
	}
	index.compressedCache = cache.NewShardedUInt64LockCache(index.getCompressedVectorForID, vectorindexcommon.DefaultVectorCacheMaxObjects, cfg.Logger, 0)
	index.initStore(cfg.RootPath, cfg.ClassName)

	return index, nil
}

func (h *flat) storeCompressedVector(index uint64, vector []byte) {
	Id := make([]byte, 8)
	binary.LittleEndian.PutUint64(Id, index)
	h.store.Bucket(helpers.CompressedObjectsBucketLSM).Put(Id, vector)
}

func (index *flat) initStore(rootPath, className string) error {
	if !index.fullyOnDisk {
		return nil
	}
	store, err := lsmkv.New(fmt.Sprintf("%s/%s/%s", rootPath, className, index.shardName), "", index.logger, nil,
		index.shardCompactionCallbacks, index.shardFlushCallbacks)
	if err != nil {
		return errors.Wrap(err, "Init lsmkv (compressed vectors store)")
	}
	err = store.CreateOrLoadBucket(context.Background(), helpers.CompressedObjectsBucketLSM)
	if err != nil {
		return errors.Wrapf(err, "Create or load bucket (compressed vectors store)")
	}
	index.store = store
	return nil
}

func (index *flat) AddBatch(ids []uint64, vectors [][]float32) error {
	/*if len(ids) != len(vectors) {
		return errors.Errorf("ids and vectors sizes does not match")
	}
	if len(ids) == 0 {
		return errors.Errorf("insertBatch called with empty lists")
	}
	index.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&index.dims, int32(len(vectors[0])))
		fmt.Println("dimensions: " + string(index.dims))
		index.bq = *ssdhelpers.NewBinaryQuantizer()
		index.bq.Fit(vectors)
	})
	for idx := range ids {
		if err := index.Add(ids[idx], vectors[idx]); err != nil {
			return err
		}
	}*/
	return nil
}

func byteSliceFromUint64Slice(x []uint64) []byte {
	y := make([]byte, len(x)*8)
	for i := range x {
		binary.LittleEndian.PutUint64(y[i*8:], x[i])
	}
	return y
}

func uint64SliceFromByteSlice(x []byte) []uint64 {
	len := len(x) / 8
	y := make([]uint64, len)
	for i := 0; i < len; i++ {
		y[i] = binary.LittleEndian.Uint64(x[i*8:])
	}
	return y
}

func (index *flat) Add(id uint64, vector []float32) error {
	index.trackDimensionsOnce.Do(func() {
		atomic.StoreInt32(&index.dims, int32(len(vector)))
		fmt.Println("dimensions: " + string(index.dims))
		index.bq = *ssdhelpers.NewBinaryQuantizer()
	})
	if len(vector) != int(index.dims) {
		return errors.Errorf("insert called with a vector of the wrong size")
	}
	if index.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		vector = distancer.Normalize(vector)
	}
	vec, err := index.bq.Encode(vector)
	if err != nil {
		return err
	}
	if index.fullyOnDisk {
		index.storeCompressedVector(id, byteSliceFromUint64Slice(vec))
		return nil
	}

	if index.compressedCache == nil {
		return errors.New("No cache...")
	}
	index.compressedCache.Grow(id)
	index.compressedCache.Preload(id, vec)
	index.Lock()
	defer index.Unlock()
	index.nodes[id] = struct{}{}
	if index.maxId < id {
		index.maxId = id
	}
	return nil
}

func (index *flat) Delete(ids ...uint64) error {
	for _, id := range ids {
		index.compressedCache.Delete(context.Background(), id)
		index.Lock()
		delete(index.nodes, id)
		index.Unlock()
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
	/*if len(vector) != int(index.dims) {
		return nil, nil, errors.Errorf("search called with a vector of the wrong size, %d, %d", len(vector), index.dims)
	}
	*/
	if index.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		vector = distancer.Normalize(vector)
	}
	query, err := index.bq.Encode(vector)
	if err != nil {
		return nil, nil, err
	}
	ef := index.searchTimeEF(k)
	heap := index.pqResults.GetMax(ef)
	defer index.pqResults.Put(heap)
	index.RLock()

	if index.fullyOnDisk {
		cursor := index.store.Bucket(helpers.CompressedObjectsBucketLSM).Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			id := binary.LittleEndian.Uint64(k)
			// Make sure to copy the vector. The cursor only guarantees that
			// the underlying memory won't change until we hit .Next(). Since
			// we want to keep this around in the cache "forever", we need to
			// alloc some new memory and copy the vector.
			//
			// https://github.com/weaviate/weaviate/issues/3049
			candidate := uint64SliceFromByteSlice(v)
			d, _ := index.bq.DistanceBetweenCompressedVectors(candidate, query)
			if heap.Len() < ef || heap.Top().Dist > d {
				if heap.Len() == ef {
					heap.Pop()
				}
				heap.Insert(id, d)
			}

		}
		cursor.Close()
	} else {
		maxId := index.maxId
		index.RUnlock()
		for j := uint64(0); j < maxId; j++ {
			candidate, err := index.compressedCache.Get(context.Background(), j)
			if err != nil {
				continue
			}
			d, _ := index.bq.DistanceBetweenCompressedVectors(candidate, query)
			if heap.Len() < ef || heap.Top().Dist > d {
				if heap.Len() == ef {
					heap.Pop()
				}
				heap.Insert(uint64(j), d)
			}
		}
	}

	ids := make([]uint64, ef)
	for j := range ids {
		ids[j] = heap.Pop().ID
	}

	for _, id := range ids {
		candidate, err := index.vectorForID(context.Background(), id)
		if err != nil {
			return nil, nil, err
		}
		d, _, _ := index.distancerProvider.SingleDist(candidate, vector)
		if heap.Len() < ef || heap.Top().Dist > d {
			if heap.Len() == ef {
				heap.Pop()
			}
			heap.Insert(uint64(id), d)
		}
	}
	for heap.Len() > k {
		heap.Pop()
	}

	ids = make([]uint64, k)
	dists := make([]float32, k)
	for j := k - 1; j >= 0; j-- {
		elem := heap.Pop()
		ids[j] = elem.ID
		dists[j] = elem.Dist
	}

	return ids, dists, nil
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
	index.compressedCache.Drop()
	return nil
}

func (index *flat) Flush() error {
	return nil
}

func (index *flat) Shutdown(ctx context.Context) error {
	if index.fullyOnDisk {
		if err := index.store.Shutdown(ctx); err != nil {
			return errors.Wrap(err, "flat shutdown")
		}
	}
	return index.Drop(ctx)
}

func (index *flat) SwitchCommitLogs(context.Context) error {
	return nil
}

func (index *flat) ListFiles(context.Context) ([]string, error) {
	return nil, nil
}

func (i *flat) ValidateBeforeInsert(vector []float32) error {
	return nil
}

func (index *flat) PostStartup() {
	index.prefillCache()
}

func (h *flat) prefillCache() {
	/*limit := 0
	limit = int(h.cache.CopyMaxSize())

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
		defer cancel()

		var err error
		err = newVectorCachePrefiller(h.cache, h, h.logger).Prefill(ctx, limit)

		if err != nil {
			h.logger.WithError(err).Error("prefill vector cache")
		}
	}()*/
}

func (index *flat) Dump(labels ...string) {
	if len(labels) > 0 {
		fmt.Printf("--------------------------------------------------\n")
		fmt.Printf("--  %s\n", strings.Join(labels, ", "))
	}
	fmt.Printf("--------------------------------------------------\n")
	fmt.Printf("ID: %s\n", index.id)
	fmt.Printf("Vectors: %d\n", len(index.nodes))
	fmt.Printf("--------------------------------------------------\n")
}

func (index *flat) DistanceBetweenVectors(x, y []float32) (float32, bool, error) {
	return index.distancerProvider.SingleDist(x, y)
}

func (index *flat) ContainsNode(id uint64) bool {
	_, found := index.nodes[id]
	return found
}

func (index *flat) getCompressedVectorForID(ctx context.Context, id uint64) ([]uint64, error) {
	slice := index.tempVectors.Get(int(index.dims))
	vec, err := index.TempVectorForIDThunk(context.Background(), id, slice)
	index.tempVectors.Put(slice)
	if err != nil {
		return nil, errors.Wrap(err, "Getting vector for id")
	}
	if index.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		vec = distancer.Normalize(vec)
	}

	return index.bq.Encode(vec)
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

/*
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
*/
