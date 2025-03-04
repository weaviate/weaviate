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

package ivfpq

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	entcfg "github.com/weaviate/weaviate/entities/config"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	ent "github.com/weaviate/weaviate/entities/vectorindex/ivfpq"
)

const (
	trainingLimit = 100_000
)

type ivfpq struct {
	sync.RWMutex
	id                  string
	targetVector        string
	rootPath            string
	dims                int32
	store               *lsmkv.Store
	logger              logrus.FieldLogger
	distancerProvider   distancer.Provider
	trackDimensionsOnce sync.Once
	rescore             int64
	sq                  *compressionhelpers.ScalarQuantizer
	pq                  *compressionhelpers.ProductQuantizer
	invertedIndex       invertedIndexNode

	pqResults *common.PqMaxPool
	pool      *pools
	count     uint64

	probingSize    int32
	distanceCutOff float32
}

func New(cfg Config, uc ent.UserConfig, store *lsmkv.Store) (*ivfpq, error) {
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	logger := cfg.Logger
	if logger == nil {
		l := logrus.New()
		l.Out = io.Discard
		logger = l
	}

	index := &ivfpq{
		id:                cfg.ID,
		targetVector:      cfg.TargetVector,
		rootPath:          cfg.RootPath,
		logger:            logger,
		distancerProvider: cfg.DistanceProvider,
		pqResults:         common.NewPqMaxPool(100),
		pool:              newPools(),
		store:             store,
		probingSize:       int32(uc.ProbingSize),
		distanceCutOff:    uc.DistanceCutOff,
	}

	if err := index.initBuckets(context.Background()); err != nil {
		return nil, fmt.Errorf("init flat index buckets: %w", err)
	}

	return index, nil
}

func (index *ivfpq) Compressed() bool {
	index.RLock()
	defer index.RUnlock()
	return index.pq != nil
}

func (index *ivfpq) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
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

func (index *ivfpq) Add(ctx context.Context, id uint64, vector []float32) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	index.trackDimensionsOnce.Do(func() {
		size := int32(len(vector))
		atomic.StoreInt32(&index.dims, size)
	})
	if len(vector) != int(index.dims) {
		return errors.Errorf("insert called with a vector of the wrong size")
	}
	vector = index.normalized(vector)
	slice := make([]byte, len(vector)*4)
	index.storeVector(id, byteSliceFromFloat32Slice(vector, slice), index.getBucketName())

	if index.Compressed() {
		vectorSQ := index.sq.Encode(vector)
		index.storeVector(id, vectorSQ, index.getCompressedBucketName())
		code := index.pq.Encode(vector)
		index.invertedIndex.add(code, id)
	}
	newCount := atomic.LoadUint64(&index.count)
	atomic.StoreUint64(&index.count, newCount+1)
	return nil
}

func (index *ivfpq) Delete(ids ...uint64) error {
	return nil
}

func (index *ivfpq) SearchByVector(ctx context.Context, vector []float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	if index.Compressed() {
		codes, dists := index.pq.SortCodes(vector)
		probingSize := int(atomic.LoadInt32(&index.probingSize))
		heap := priorityqueue.NewMax[byte](probingSize)
		index.invertedIndex.search(codes, dists, probingSize, heap, 0)

		compressedK := 15

		cHeap := priorityqueue.NewMax[byte](compressedK)
		vector = index.normalized(vector)
		cdistancer := index.sq.NewDistancer(vector)

		ids := make([]uint64, probingSize)
		dist := make([]float32, probingSize)

		for heap.Len() > probingSize {
			heap.Pop()
		}
		j := heap.Len()
		for heap.Len() > 0 {
			j--
			elem := heap.Pop()
			ids[j] = elem.ID
			dist[j] = elem.Dist
		}

		pos := 0
		for cHeap.Len() < compressedK {
			element := ids[pos]
			pos++
			vec, err := index.getSQVector(ctx, element)
			if err != nil {
				return nil, nil, err
			}
			d, _ := cdistancer.Distance(vec)
			cHeap.Insert(element, d)
		}
		limit := dist[pos]
		for pos < probingSize {
			if dist[pos]-limit > 0.1 {
				ids = ids[:pos]
				break
			}
			element := ids[pos]
			pos++
			vec, err := index.getSQVector(ctx, element)
			if err != nil {
				return nil, nil, err
			}
			d, _ := cdistancer.Distance(vec)
			if d < cHeap.Top().Dist {
				cHeap.Pop()
				cHeap.Insert(element, d)
			}
		}

		heap.Reset()
		for cHeap.Len() > 0 {
			element := cHeap.Pop()
			vec, err := index.vectorById(element.ID)
			if err != nil {
				return nil, nil, err
			}
			distance, err := index.distancerProvider.SingleDist(vec, vector)
			if err != nil {
				return nil, nil, err
			}
			heap.Insert(element.ID, distance)
		}

		for heap.Len() > k {
			heap.Pop()
		}
		ids = make([]uint64, k)
		dist = make([]float32, k)
		j = k
		for heap.Len() > 0 {
			j--
			elem := heap.Pop()
			ids[j] = elem.ID
			dist[j] = elem.Dist
		}

		return ids, dist, nil
	} else {
		return nil, nil, nil
	}
}

func (index *ivfpq) SearchByVectorDistance(ctx context.Context, vector []float32,
	targetDistance float32, maxLimit int64, allow helpers.AllowList,
) ([]uint64, []float32, error) {
	return nil, nil, nil
}

func (index *ivfpq) UpdateUserConfig(updated schemaConfig.VectorIndexConfig, callback func()) error {
	parsed, ok := updated.(ent.UserConfig)
	if !ok {
		callback()
		return errors.Errorf("config is not UserConfig, but %T", updated)
	}

	// Store atomically as a lock here would be very expensive, this value is
	// read on every single user-facing search, which can be highly concurrent
	atomic.StoreInt32(&index.probingSize, int32(parsed.ProbingSize))
	index.distanceCutOff = parsed.DistanceCutOff

	callback()
	return nil
}

func (index *ivfpq) Drop(ctx context.Context) error {
	return nil
}

func (index *ivfpq) Flush() error {
	return nil
}

func (index *ivfpq) Shutdown(ctx context.Context) error {
	return nil
}

func (index *ivfpq) SwitchCommitLogs(context.Context) error {
	return nil
}

func (index *ivfpq) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return nil, nil
}

func (i *ivfpq) ValidateBeforeInsert(vector []float32) error {
	return nil
}

func (index *ivfpq) PostStartup() {

}

func (index *ivfpq) Dump(labels ...string) {

}

func (index *ivfpq) DistanceBetweenVectors(x, y []float32) (float32, error) {
	return 0, nil
}

func (index *ivfpq) ContainsNode(id uint64) bool {
	return true
}

func (index *ivfpq) Iterate(fn func(id uint64) bool) {

}

func (index *ivfpq) DistancerProvider() distancer.Provider {
	return index.distancerProvider
}

func (index *ivfpq) AlreadyIndexed() uint64 {
	return atomic.LoadUint64(&index.count)
}

func (index *ivfpq) QueryVectorDistancer(queryVector []float32) common.QueryVectorDistancer {
	return common.QueryVectorDistancer{}
}

func (index *ivfpq) Stats() (common.IndexStats, error) {
	return nil, nil
}

func (index *ivfpq) AddMulti(ctx context.Context, docID uint64, vectors [][]float32) error {
	return errors.Errorf("AddMulti is not supported for ivfpq index")
}

func (index *ivfpq) AddMultiBatch(ctx context.Context, docIDs []uint64, vectors [][][]float32) error {
	return errors.Errorf("AddMultiBatch is not supported for ivfpq index")
}

func (index *ivfpq) DeleteMulti(ids ...uint64) error {
	return errors.Errorf("DeleteMulti is not supported for ivfpq index")
}

func (index *ivfpq) GetKeys(id uint64) (uint64, uint64, error) {
	return 0, 0, errors.Errorf("GetKeys is not supported for ivfpq index")
}

func (i *ivfpq) ValidateMultiBeforeInsert(vector [][]float32) error {
	return nil
}

func (index *ivfpq) Multivector() bool {
	return false
}

func (index *ivfpq) QueryMultiVectorDistancer(queryVector [][]float32) common.QueryVectorDistancer {
	return common.QueryVectorDistancer{}
}

func (index *ivfpq) SearchByMultiVector(ctx context.Context, vectors [][]float32, k int, allow helpers.AllowList) ([]uint64, []float32, error) {
	return nil, nil, errors.Errorf("SearchByMultiVector is not supported for ivfpq index")
}

func (index *ivfpq) SearchByMultiVectorDistance(ctx context.Context, vector [][]float32,
	targetDistance float32, maxLimit int64, allow helpers.AllowList,
) ([]uint64, []float32, error) {
	return nil, nil, errors.Errorf("SearchByMultiVectorDistance is not supported for ivfpq index")
}

type immutableParameter struct {
	accessor func(c ent.UserConfig) interface{}
	name     string
}

func ValidateUserConfigUpdate(initial, updated schemaConfig.VectorIndexConfig) error {
	initialParsed, ok := initial.(ent.UserConfig)
	if !ok {
		return errors.Errorf("initial is not UserConfig, but %T", initial)
	}

	updatedParsed, ok := updated.(ent.UserConfig)
	if !ok {
		return errors.Errorf("updated is not UserConfig, but %T", updated)
	}

	immutableFields := []immutableParameter{
		{
			name:     "distance",
			accessor: func(c ent.UserConfig) interface{} { return c.Distance },
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

func validateImmutableField(u immutableParameter,
	previous, next ent.UserConfig,
) error {
	oldField := u.accessor(previous)
	newField := u.accessor(next)
	if oldField != newField {
		return errors.Errorf("%s is immutable: attempted change from \"%v\" to \"%v\"",
			u.name, oldField, newField)
	}

	return nil
}

// TODO: Remove this function when gh-5241 is completed. See flat::initBuckets for more details.
func shouldForceCompaction() bool {
	return !entcfg.Enabled(os.Getenv("IVFPQ_INDEX_DISABLE_FORCED_COMPACTION"))
}

func (index *ivfpq) initBuckets(ctx context.Context) error {
	// TODO: Forced compaction should not stay an all or nothing option.
	//       This is only a temporary measure until dynamic compaction
	//       behavior is implemented.
	//       See: https://github.com/weaviate/weaviate/issues/5241
	forceCompaction := shouldForceCompaction()
	if err := index.store.CreateOrLoadBucket(ctx, index.getBucketName(),
		lsmkv.WithForceCompaction(forceCompaction),
		lsmkv.WithUseBloomFilter(false),
		lsmkv.WithCalcCountNetAdditions(false),

		// Pread=false flag introduced around ~v1.25.9. Before that, the pread flag
		// was simply missing. Now we want to explicitly set it to false for
		// performance reasons. There are pread performance improvements in the
		// pipeline, but as of now, mmap is much more performant – especially for
		// parallel cache prefilling.
		//
		// In the future when the pure pread performance is on par with mmap, we
		// should update this to pass the global setting.
		lsmkv.WithPread(false),
	); err != nil {
		return fmt.Errorf("Create or load flat vectors bucket: %w", err)
	}
	if err := index.store.CreateOrLoadBucket(ctx, index.getCompressedBucketName(),
		lsmkv.WithForceCompaction(forceCompaction),
		lsmkv.WithUseBloomFilter(false),
		lsmkv.WithCalcCountNetAdditions(false),

		// Pread=false flag introduced around ~v1.25.9. Before that, the pread flag
		// was simply missing. Now we want to explicitly set it to false for
		// performance reasons. There are pread performance improvements in the
		// pipeline, but as of now, mmap is much more performant – especially for
		// parallel cache prefilling.
		//
		// In the future when the pure pread performance is on par with mmap, we
		// should update this to pass the global setting.
		lsmkv.WithPread(false),
	); err != nil {
		return fmt.Errorf("Create or load ivfpq compressed vectors bucket: %w", err)
	}
	return nil
}

func (index *ivfpq) getBucketName() string {
	if index.targetVector != "" {
		return fmt.Sprintf("%s_%s", helpers.VectorsBucketLSM, index.targetVector)
	}
	return helpers.VectorsBucketLSM
}

func (index *ivfpq) getCompressedBucketName() string {
	if index.targetVector != "" {
		return fmt.Sprintf("%s_%s", helpers.VectorsCompressedBucketLSM, index.targetVector)
	}
	return helpers.VectorsCompressedBucketLSM
}

func (index *ivfpq) normalized(vector []float32) []float32 {
	if index.distancerProvider.Type() == "cosine-dot" {
		// cosine-dot requires normalized vectors, as the dot product and cosine
		// similarity are only identical if the vector is normalized
		return distancer.Normalize(vector)
	}
	return vector
}

func float32SliceFromByteSlice(vector []byte, slice []float32) []float32 {
	for i := range slice {
		slice[i] = math.Float32frombits(binary.LittleEndian.Uint32(vector[i*4:]))
	}
	return slice
}

func byteSliceFromFloat32Slice(vector []float32, slice []byte) []byte {
	for i := range vector {
		binary.LittleEndian.PutUint32(slice[i*4:], math.Float32bits(vector[i]))
	}
	return slice
}

func (index *ivfpq) storeVector(id uint64, vector []byte, bucket string) {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	index.store.Bucket(bucket).Put(idBytes, vector)
}

func (index *ivfpq) getSQVector(ctx context.Context, id uint64) ([]byte, error) {
	key := index.pool.byteSlicePool.Get(8)
	defer index.pool.byteSlicePool.Put(key)
	binary.BigEndian.PutUint64(key.slice, id)
	bytes, err := index.store.Bucket(index.getCompressedBucketName()).Get(key.slice)
	if err != nil {
		return nil, err
	}
	if len(bytes) == 0 {
		return nil, nil
	}
	return bytes, nil
}

func (index *ivfpq) vectorById(id uint64) ([]float32, error) {
	idSlice := index.pool.byteSlicePool.Get(8)
	defer index.pool.byteSlicePool.Put(idSlice)

	binary.BigEndian.PutUint64(idSlice.slice, id)
	vecBytes, err := index.store.Bucket(index.getBucketName()).Get(idSlice.slice)
	if err != nil {
		return nil, err
	}
	vecSlice := index.pool.float32SlicePool.Get(len(vecBytes) / 4)
	defer index.pool.float32SlicePool.Put(vecSlice)

	return float32SliceFromByteSlice(vecBytes, vecSlice.slice), nil
}

func (index *ivfpq) ShouldUpgrade() (bool, int) {
	return !index.Compressed(), trainingLimit
}

func (index *ivfpq) Upgraded() bool {
	return index.Compressed()
}

func (index *ivfpq) Upgrade(callback func()) error {
	index.Lock()
	defer index.Unlock()

	cfg := hnswent.PQConfig{
		Enabled:       true,
		Segments:      3,
		Centroids:     128,
		TrainingLimit: trainingLimit,
		Encoder: hnswent.PQEncoder{
			Type:         hnswent.PQEncoderTypeKMeans,
			Distribution: hnswent.PQEncoderDistributionNormal,
		},
	}
	pq, err := compressionhelpers.NewProductQuantizer(cfg, index.distancerProvider, int(index.dims), index.logger)
	if err != nil {
		panic(err)
	}
	bucket := index.store.Bucket(index.getBucketName())
	cursor := bucket.Cursor()

	data := make([][]float32, 0, trainingLimit)
	ids := make([]uint64, 0, trainingLimit)
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		id := binary.BigEndian.Uint64(k)
		vc := make([]float32, len(v)/4)
		float32SliceFromByteSlice(v, vc)

		if len(vc) != int(index.dims) {
			continue
		}
		ids = append(ids, id)
		data = append(data, vc)
	}
	cursor.Close()
	pq.Fit(data)
	index.invertedIndex = newInvertedIndex(0, 3, common.NewDefaultShardedLocks())
	index.sq = compressionhelpers.NewScalarQuantizer(data, index.distancerProvider)
	index.pq = pq

	compressionhelpers.Concurrently(index.logger, uint64(len(data)), func(i uint64) {
		code := pq.Encode(data[i])
		index.invertedIndex.add(code, ids[i])
		vectorSQ := index.sq.Encode(data[i])
		index.storeVector(ids[i], vectorSQ, index.getCompressedBucketName())
	})
	callback()
	return nil
}
