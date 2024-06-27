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

package compressionhelpers

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

type CompressorDistancer interface {
	DistanceToNode(id uint64) (float32, bool, error)
	DistanceToFloat(vec []float32) (float32, bool, error)
}

type ReturnDistancerFn func()

type VectorCompressor interface {
	Drop() error
	GrowCache(size uint64)
	SetCacheMaxSize(size int64)
	GetCacheMaxSize() int64
	Delete(ctx context.Context, id uint64)
	Preload(id uint64, vector []float32)
	Prefetch(id uint64)
	PrefillCache()

	DistanceBetweenCompressedVectorsFromIDs(ctx context.Context, x, y uint64) (float32, error)
	DistanceBetweenCompressedAndUncompressedVectorsFromID(ctx context.Context, x uint64, y []float32) (float32, error)
	NewDistancer(vector []float32) (CompressorDistancer, ReturnDistancerFn)
	NewDistancerFromID(id uint64) (CompressorDistancer, error)
	NewBag() CompressionDistanceBag

	ExposeFields() PQData
}

type quantizedVectorsCompressor[T byte | uint64] struct {
	cache           cache.Cache[T]
	compressedStore *lsmkv.Store
	quantizer       quantizer[T]
	storeId         func([]byte, uint64)
	loadId          func([]byte) uint64
	logger          logrus.FieldLogger
}

func (compressor *quantizedVectorsCompressor[T]) Drop() error {
	compressor.cache.Drop()
	return nil
}

func (compressor *quantizedVectorsCompressor[T]) GrowCache(size uint64) {
	compressor.cache.Grow(size)
}

func (compressor *quantizedVectorsCompressor[T]) SetCacheMaxSize(size int64) {
	compressor.cache.UpdateMaxSize(size)
}

func (compressor *quantizedVectorsCompressor[T]) GetCacheMaxSize() int64 {
	return compressor.cache.CopyMaxSize()
}

func (compressor *quantizedVectorsCompressor[T]) Delete(ctx context.Context, id uint64) {
	compressor.cache.Delete(ctx, id)
	idBytes := make([]byte, 8)
	compressor.storeId(idBytes, id)
	compressor.compressedStore.Bucket(helpers.VectorsCompressedBucketLSM).Delete(idBytes)
}

func (compressor *quantizedVectorsCompressor[T]) Preload(id uint64, vector []float32) {
	compressedVector := compressor.quantizer.Encode(vector)
	idBytes := make([]byte, 8)
	compressor.storeId(idBytes, id)
	compressor.compressedStore.Bucket(helpers.VectorsCompressedBucketLSM).Put(idBytes, compressor.quantizer.CompressedBytes(compressedVector))
	compressor.cache.Grow(id)
	compressor.cache.Preload(id, compressedVector)
}

func (compressor *quantizedVectorsCompressor[T]) Prefetch(id uint64) {
	compressor.cache.Prefetch(id)
}

func (compressor *quantizedVectorsCompressor[T]) DistanceBetweenCompressedVectors(x, y []T) (float32, error) {
	return compressor.quantizer.DistanceBetweenCompressedVectors(x, y)
}

func (compressor *quantizedVectorsCompressor[T]) DistanceBetweenCompressedAndUncompressedVectors(x []T, y []float32) (float32, error) {
	return compressor.quantizer.DistanceBetweenCompressedAndUncompressedVectors(y, x)
}

func (compressor *quantizedVectorsCompressor[T]) compressedVectorFromID(ctx context.Context, id uint64) ([]T, error) {
	compressedVector, err := compressor.cache.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(compressedVector) == 0 {
		return nil, fmt.Errorf("got a nil or zero-length vector at docID %d", id)
	}
	return compressedVector, nil
}

func (compressor *quantizedVectorsCompressor[T]) DistanceBetweenCompressedVectorsFromIDs(ctx context.Context, id1, id2 uint64) (float32, error) {
	compressedVector1, err := compressor.compressedVectorFromID(ctx, id1)
	if err != nil {
		return 0, err
	}

	compressedVector2, err := compressor.compressedVectorFromID(ctx, id2)
	if err != nil {
		return 0, err
	}

	dist, err := compressor.DistanceBetweenCompressedVectors(compressedVector1, compressedVector2)
	return dist, err
}

func (compressor *quantizedVectorsCompressor[T]) DistanceBetweenCompressedAndUncompressedVectorsFromID(ctx context.Context, id uint64, vector []float32) (float32, error) {
	compressedVector, err := compressor.compressedVectorFromID(ctx, id)
	if err != nil {
		return 0, err
	}

	dist, err := compressor.DistanceBetweenCompressedAndUncompressedVectors(compressedVector, vector)
	return dist, err
}

func (compressor *quantizedVectorsCompressor[T]) getCompressedVectorForID(ctx context.Context, id uint64) ([]T, error) {
	idBytes := make([]byte, 8)
	compressor.storeId(idBytes, id)
	compressedVector, err := compressor.compressedStore.Bucket(helpers.VectorsCompressedBucketLSM).Get(idBytes)
	if err != nil {
		return nil, errors.Wrap(err, "Getting vector for id")
	}
	if len(compressedVector) == 0 {
		return nil, storobj.NewErrNotFoundf(id, "getCompressedVectorForID")
	}

	return compressor.quantizer.FromCompressedBytes(compressedVector), nil
}

func (compressor *quantizedVectorsCompressor[T]) NewDistancer(vector []float32) (CompressorDistancer, ReturnDistancerFn) {
	d := &quantizedCompressorDistancer[T]{
		compressor: compressor,
		distancer:  compressor.quantizer.NewQuantizerDistancer(vector),
	}
	return d, func() {
		compressor.returnDistancer(d)
	}
}

func (compressor *quantizedVectorsCompressor[T]) NewDistancerFromID(id uint64) (CompressorDistancer, error) {
	compressedVector, err := compressor.compressedVectorFromID(context.Background(), id)
	if err != nil {
		return nil, err
	}
	if compressedVector == nil {
		return nil, storobj.ErrNotFound{
			DocID: id,
		}
	}
	d := &quantizedCompressorDistancer[T]{
		compressor: compressor,
		distancer:  compressor.quantizer.NewCompressedQuantizerDistancer(compressedVector),
	}
	return d, nil
}

func (compressor *quantizedVectorsCompressor[T]) returnDistancer(distancer CompressorDistancer) {
	dst := distancer.(*quantizedCompressorDistancer[T]).distancer
	if dst == nil {
		return
	}
	compressor.quantizer.ReturnQuantizerDistancer(dst)
}

func (compressor *quantizedVectorsCompressor[T]) NewBag() CompressionDistanceBag {
	return &quantizedDistanceBag[T]{
		compressor: compressor,
		elements:   make(map[uint64][]T),
	}
}

func (compressor *quantizedVectorsCompressor[T]) initCompressedStore() error {
	err := compressor.compressedStore.CreateOrLoadBucket(context.Background(), helpers.VectorsCompressedBucketLSM)
	if err != nil {
		return errors.Wrapf(err, "Create or load bucket (compressed vectors store)")
	}
	return nil
}

func (compressor *quantizedVectorsCompressor[T]) PrefillCache() {
	before := time.Now()
	count := 0

	// The idea here is to first read everything from disk in one go, then grow
	// the cache just once before inserting all vectors. A previous iteration
	// would grow the cache as part of the cursor loop and this ended up making
	// up 75% of the CPU time needed. This new implementation with two loops is
	// much more efficient and only ever-so-slightly more memory-consuming (about
	// one additional struct per vector while loading. Should be negligible)
	type vec struct {
		id  uint64
		vec []T
	}

	// The initial size of 10k is chosen fairly arbitrarily. The cost of growing
	// this slice dynamically should be quite cheap compared to other operations
	// involved here, e.g. disk reads.
	vecs := make([]vec, 0, 10_000)
	maxID := uint64(0)

	cursor := compressor.compressedStore.Bucket(helpers.VectorsCompressedBucketLSM).Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		count++

		id := compressor.loadId(k)
		vc := make([]byte, len(v))
		copy(vc, v)

		vecs = append(vecs, vec{id: id, vec: compressor.quantizer.FromCompressedBytes(vc)})
		if id > maxID {
			maxID = id
		}

	}
	cursor.Close()

	compressor.cache.Grow(maxID)

	for _, vec := range vecs {
		compressor.cache.Preload(vec.id, vec.vec)
	}

	took := time.Since(before)
	compressor.logger.WithFields(logrus.Fields{
		"action": "hnsw_compressed_vector_cache_prefill",
		"count":  count,
		"took":   took,
	}).Info("prefilled compressed vector cache")
}

func (compressor *quantizedVectorsCompressor[T]) ExposeFields() PQData {
	return compressor.quantizer.ExposeFields()
}

func NewHNSWPQCompressor(
	cfg hnsw.PQConfig,
	distance distancer.Provider,
	dimensions int,
	vectorCacheMaxObjects int,
	logger logrus.FieldLogger,
	data [][]float32,
	store *lsmkv.Store,
	allocChecker memwatch.AllocChecker,
) (VectorCompressor, error) {
	quantizer, err := NewProductQuantizer(cfg, distance, dimensions, logger)
	if err != nil {
		return nil, err
	}
	pqVectorsCompressor := &quantizedVectorsCompressor[byte]{
		quantizer:       quantizer,
		compressedStore: store,
		storeId:         binary.LittleEndian.PutUint64,
		loadId:          binary.LittleEndian.Uint64,
		logger:          logger,
	}
	pqVectorsCompressor.initCompressedStore()
	pqVectorsCompressor.cache = cache.NewShardedByteLockCache(
		pqVectorsCompressor.getCompressedVectorForID, vectorCacheMaxObjects, logger,
		0, allocChecker)
	pqVectorsCompressor.cache.Grow(uint64(len(data)))
	err = quantizer.Fit(data)
	if err != nil {
		return nil, err
	}
	return pqVectorsCompressor, nil
}

func RestoreHNSWPQCompressor(
	cfg hnsw.PQConfig,
	distance distancer.Provider,
	dimensions int,
	vectorCacheMaxObjects int,
	logger logrus.FieldLogger,
	encoders []PQEncoder,
	store *lsmkv.Store,
	allocChecker memwatch.AllocChecker,
) (VectorCompressor, error) {
	quantizer, err := NewProductQuantizerWithEncoders(cfg, distance, dimensions, encoders, logger)
	if err != nil {
		return nil, err
	}
	pqVectorsCompressor := &quantizedVectorsCompressor[byte]{
		quantizer:       quantizer,
		compressedStore: store,
		storeId:         binary.LittleEndian.PutUint64,
		loadId:          binary.LittleEndian.Uint64,
		logger:          logger,
	}
	pqVectorsCompressor.initCompressedStore()
	pqVectorsCompressor.cache = cache.NewShardedByteLockCache(
		pqVectorsCompressor.getCompressedVectorForID, vectorCacheMaxObjects, logger, 0,
		allocChecker)
	return pqVectorsCompressor, nil
}

func NewBQCompressor(
	distance distancer.Provider,
	vectorCacheMaxObjects int,
	logger logrus.FieldLogger,
	store *lsmkv.Store,
	allocChecker memwatch.AllocChecker,
) (VectorCompressor, error) {
	quantizer := NewBinaryQuantizer(distance)
	bqVectorsCompressor := &quantizedVectorsCompressor[uint64]{
		quantizer:       &quantizer,
		compressedStore: store,
		storeId:         binary.BigEndian.PutUint64,
		loadId:          binary.BigEndian.Uint64,
		logger:          logger,
	}
	bqVectorsCompressor.initCompressedStore()
	bqVectorsCompressor.cache = cache.NewShardedUInt64LockCache(
		bqVectorsCompressor.getCompressedVectorForID, vectorCacheMaxObjects, logger, 0,
		allocChecker)
	return bqVectorsCompressor, nil
}

type quantizedCompressorDistancer[T byte | uint64] struct {
	compressor *quantizedVectorsCompressor[T]
	distancer  quantizerDistancer[T]
}

func (distancer *quantizedCompressorDistancer[T]) DistanceToNode(id uint64) (float32, bool, error) {
	compressedVector, err := distancer.compressor.cache.Get(context.Background(), id)
	if err != nil {
		return 0, false, err
	}
	if len(compressedVector) == 0 {
		return 0, false, fmt.Errorf(
			"got a nil or zero-length vector at docID %d", id)
	}
	return distancer.distancer.Distance(compressedVector)
}

func (distancer *quantizedCompressorDistancer[T]) DistanceToFloat(vector []float32) (float32, bool, error) {
	return distancer.distancer.DistanceToFloat(vector)
}
