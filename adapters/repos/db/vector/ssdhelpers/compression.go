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

package ssdhelpers

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type CompressorDistancer interface {
	DistanceToNode(id uint64) (float32, bool, error)
	DistanceToFloat(vec []float32) (float32, bool, error)
}

type QuantizerDistancer[T byte | uint64] interface {
	Distance(x []T) (float32, bool, error)
	DistanceToFloat(x []float32) (float32, bool, error)
}

type CompressionDistanceBag interface {
	Load(ctx context.Context, id uint64) error
	Distance(x, y uint64) (float32, error)
}

type QuantizedDistanceBag[T byte | uint64] struct {
	elements   map[uint64][]T
	compressor *QuantizedVectorsCompressor[T]
}

func (bag *QuantizedDistanceBag[T]) Load(ctx context.Context, id uint64) error {
	var err error
	bag.elements[id], err = bag.compressor.cache.Get(ctx, id)
	return err
}

func (bag *QuantizedDistanceBag[T]) Distance(x, y uint64) (float32, error) {
	v1, found := bag.elements[x]
	if !found {
		return 0, fmt.Errorf("missing id in bag: %d", x)
	}
	v2, found := bag.elements[y]
	if !found {
		return 0, fmt.Errorf("missing id in bag: %d", y)
	}
	return bag.compressor.DistanceBetweenCompressedVectors(v1, v2)
}

type VectorCompressor interface {
	Drop() error
	GrowCache(size uint64)
	SetCacheMaxSize(size int64)
	GetCacheMaxSize() int64
	Delete(ctx context.Context, id uint64)
	Preload(id uint64, vector []float32)
	Prefetch(id uint64)
	PrefillCache()

	PauseCompaction(ctx context.Context) error
	FlushMemtables(ctx context.Context) error
	ResumeCompaction(ctx context.Context) error
	ListFiles(ctx context.Context, basePath string) ([]string, error)

	DistanceBetweenCompressedVectorsFromIDs(ctx context.Context, x, y uint64) (float32, error)
	DistanceBetweenCompressedAndUncompressedVectorsFromID(ctx context.Context, x uint64, y []float32) (float32, error)
	NewDistancer(vector []float32) CompressorDistancer
	NewDistancerFromID(id uint64) CompressorDistancer
	ReturnDistancer(distancer CompressorDistancer)
	NewBag() CompressionDistanceBag

	ExposeFields() PQData
}

type quantizer[T byte | uint64] interface {
	DistanceBetweenCompressedVectors(x, y []T) (float32, error)
	DistanceBetweenCompressedAndUncompressedVectors(x []float32, encoded []T) (float32, error)
	Encode(vec []float32) []T
	NewQuantizerDistancer(a []float32) QuantizerDistancer[T]
	NewCompressedQuantizerDistancer(a []T) QuantizerDistancer[T]
	ReturnQuantizerDistancer(distancer QuantizerDistancer[T])
	CompressedBytes(compressed []T) []byte
	FromCompressedBytes(compressed []byte) []T
	ExposeFields() PQData
}

type QuantizedVectorsCompressor[T byte | uint64] struct {
	cache           cache.Cache[T]
	compressedStore *lsmkv.Store
	quantizer       quantizer[T]
}

func (compressor *QuantizedVectorsCompressor[T]) Drop() error {
	compressor.cache.Drop()

	if err := compressor.compressedStore.ShutdownBucket(context.Background(), helpers.VectorsHNSWPQBucketLSM); err != nil {
		return errors.Wrap(err, "compressed store shutdown")
	}
	return nil
}

func (compressor *QuantizedVectorsCompressor[T]) PauseCompaction(ctx context.Context) error {
	return compressor.compressedStore.PauseCompaction(ctx)
}

func (compressor *QuantizedVectorsCompressor[T]) FlushMemtables(ctx context.Context) error {
	return compressor.compressedStore.FlushMemtables(ctx)
}

func (compressor *QuantizedVectorsCompressor[T]) ResumeCompaction(ctx context.Context) error {
	return compressor.compressedStore.ResumeCompaction(ctx)
}

func (compressor *QuantizedVectorsCompressor[T]) ListFiles(ctx context.Context, basePath string) ([]string, error) {
	return compressor.compressedStore.ListFiles(ctx, basePath)
}

func (compressor *QuantizedVectorsCompressor[T]) GrowCache(size uint64) {
	compressor.cache.Grow(size)
}

func (compressor *QuantizedVectorsCompressor[T]) SetCacheMaxSize(size int64) {
	compressor.cache.UpdateMaxSize(size)
}

func (compressor *QuantizedVectorsCompressor[T]) GetCacheMaxSize() int64 {
	return compressor.cache.CopyMaxSize()
}

func (compressor *QuantizedVectorsCompressor[T]) Delete(ctx context.Context, id uint64) {
	compressor.cache.Delete(ctx, id)
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	compressor.compressedStore.Bucket(helpers.VectorsHNSWPQBucketLSM).Delete(idBytes)
}

func (compressor *QuantizedVectorsCompressor[T]) Preload(id uint64, vector []float32) {
	compressed := compressor.quantizer.Encode(vector)
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	compressor.compressedStore.Bucket(helpers.VectorsHNSWPQBucketLSM).Put(idBytes, compressor.quantizer.CompressedBytes(compressed))
	compressor.cache.Grow(id)
	compressor.cache.Preload(id, compressed)
}

func (compressor *QuantizedVectorsCompressor[T]) Prefetch(id uint64) {
	compressor.cache.Prefetch(id)
}

func (compressor *QuantizedVectorsCompressor[T]) DistanceBetweenCompressedVectors(x, y []T) (float32, error) {
	return compressor.quantizer.DistanceBetweenCompressedVectors(x, y)
}

func (compressor *QuantizedVectorsCompressor[T]) DistanceBetweenCompressedAndUncompressedVectors(x []T, y []float32) (float32, error) {
	return compressor.quantizer.DistanceBetweenCompressedAndUncompressedVectors(y, x)
}

func (compressor *QuantizedVectorsCompressor[T]) compressedVectorFromID(ctx context.Context, id uint64) ([]T, error) {
	v1, err := compressor.cache.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(v1) == 0 {
		return nil, fmt.Errorf("got a nil or zero-length vector at docID %d", id)
	}
	return v1, nil
}

func (compressor *QuantizedVectorsCompressor[T]) DistanceBetweenCompressedVectorsFromIDs(ctx context.Context, x, y uint64) (float32, error) {
	v1, err := compressor.compressedVectorFromID(ctx, x)
	if err != nil {
		return 0, err
	}

	v2, err := compressor.compressedVectorFromID(ctx, y)
	if err != nil {
		return 0, err
	}

	dist, err := compressor.DistanceBetweenCompressedVectors(v1, v2)
	return dist, err
}

func (compressor *QuantizedVectorsCompressor[T]) DistanceBetweenCompressedAndUncompressedVectorsFromID(ctx context.Context, x uint64, y []float32) (float32, error) {
	v1, err := compressor.compressedVectorFromID(ctx, x)
	if err != nil {
		return 0, err
	}

	dist, err := compressor.DistanceBetweenCompressedAndUncompressedVectors(v1, y)
	return dist, err
}

func (compressor *QuantizedVectorsCompressor[T]) getCompressedVectorForID(ctx context.Context, id uint64) ([]T, error) {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	vec, err := compressor.compressedStore.Bucket(helpers.VectorsHNSWPQBucketLSM).Get(idBytes)
	if err != nil {
		return nil, errors.Wrap(err, "Getting vector for id")
	}
	if len(vec) == 0 {
		return nil, storobj.NewErrNotFoundf(id, "getCompressedVectorForID")
	}

	return compressor.quantizer.FromCompressedBytes(vec), nil
}

func (compressor *QuantizedVectorsCompressor[T]) NewDistancer(vector []float32) CompressorDistancer {
	return &QuantizedCompressorDistancer[T]{
		compressor: compressor,
		distancer:  compressor.quantizer.NewQuantizerDistancer(vector),
	}
}

func (compressor *QuantizedVectorsCompressor[T]) NewDistancerFromID(id uint64) CompressorDistancer {
	vector, _ := compressor.getCompressedVectorForID(context.Background(), id)
	return &QuantizedCompressorDistancer[T]{
		compressor: compressor,
		distancer:  compressor.quantizer.NewCompressedQuantizerDistancer(vector),
	}
}

func (compressor *QuantizedVectorsCompressor[T]) ReturnDistancer(distancer CompressorDistancer) {
	dst := distancer.(*QuantizedCompressorDistancer[T]).distancer
	if dst == nil {
		return
	}
	compressor.quantizer.ReturnQuantizerDistancer(dst)
}

func (compressor *QuantizedVectorsCompressor[T]) NewBag() CompressionDistanceBag {
	return &QuantizedDistanceBag[T]{
		compressor: compressor,
		elements:   make(map[uint64][]T),
	}
}

func (compressor *QuantizedVectorsCompressor[T]) initCompressedStore() error {
	err := compressor.compressedStore.CreateOrLoadBucket(context.Background(), helpers.VectorsHNSWPQBucketLSM)
	if err != nil {
		return errors.Wrapf(err, "Create or load bucket (compressed vectors store)")
	}
	return nil
}

func (compressor *QuantizedVectorsCompressor[T]) PrefillCache() {
	cursor := compressor.compressedStore.Bucket(helpers.VectorsHNSWPQBucketLSM).Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		id := binary.BigEndian.Uint64(k)
		compressor.cache.Grow(id)

		vc := make([]byte, len(v))
		copy(vc, v)
		compressor.cache.Preload(id, compressor.quantizer.FromCompressedBytes(vc))
	}
	cursor.Close()
}

func NewPQCompressor(
	cfg hnsw.PQConfig,
	distance distancer.Provider,
	dimensions int,
	vectorCacheMaxObjects int,
	logger logrus.FieldLogger,
	data [][]float32,
	store *lsmkv.Store,
) (VectorCompressor, error) {
	quantizer, err := NewProductQuantizer(cfg, distance, dimensions)
	if err != nil {
		return nil, err
	}
	pqVectorsCompressor := &QuantizedVectorsCompressor[byte]{
		quantizer:       quantizer,
		compressedStore: store,
	}
	pqVectorsCompressor.initCompressedStore()
	pqVectorsCompressor.cache = cache.NewShardedByteLockCache(pqVectorsCompressor.getCompressedVectorForID, vectorCacheMaxObjects, logger, 0)
	pqVectorsCompressor.cache.Grow(uint64(len(data)))
	quantizer.Fit(data)
	return pqVectorsCompressor, nil
}

func RestorePQCompressor(
	cfg hnsw.PQConfig,
	distance distancer.Provider,
	dimensions int,
	vectorCacheMaxObjects int,
	logger logrus.FieldLogger,
	encoders []PQEncoder,
	store *lsmkv.Store,
) (VectorCompressor, error) {
	quantizer, err := NewProductQuantizerWithEncoders(cfg, distance, dimensions, encoders)
	if err != nil {
		return nil, err
	}
	pqVectorsCompressor := &QuantizedVectorsCompressor[byte]{
		quantizer:       quantizer,
		compressedStore: store,
	}
	pqVectorsCompressor.initCompressedStore()
	pqVectorsCompressor.cache = cache.NewShardedByteLockCache(pqVectorsCompressor.getCompressedVectorForID, vectorCacheMaxObjects, logger, 0)
	return pqVectorsCompressor, nil
}

func (compressor *QuantizedVectorsCompressor[T]) ExposeFields() PQData {
	return compressor.quantizer.ExposeFields()
}

func NewBQCompressor(
	distance distancer.Provider,
	vectorCacheMaxObjects int,
	logger logrus.FieldLogger,
	store *lsmkv.Store,
) (VectorCompressor, error) {
	quantizer := NewBinaryQuantizer(distance)
	bqVectorsCompressor := &QuantizedVectorsCompressor[uint64]{
		quantizer:       &quantizer,
		compressedStore: store,
	}
	bqVectorsCompressor.initCompressedStore()
	bqVectorsCompressor.cache = cache.NewShardedUInt64LockCache(bqVectorsCompressor.getCompressedVectorForID, vectorCacheMaxObjects, logger, 0)
	return bqVectorsCompressor, nil
}

func (bq *BinaryQuantizer) ExposeFields() PQData {
	return PQData{}
}

func (bq *BinaryQuantizer) DistanceBetweenCompressedAndUncompressedVectors(x []float32, y []uint64) (float32, error) {
	encoded := bq.Encode(x)
	return bq.DistanceBetweenCompressedVectors(encoded, y)
}

func (pq *ProductQuantizer) NewQuantizerDistancer(vec []float32) QuantizerDistancer[byte] {
	return pq.NewDistancer(vec)
}

func (pq *ProductQuantizer) ReturnQuantizerDistancer(distancer QuantizerDistancer[byte]) {
	if distancer == nil {
		return
	}
	pq.ReturnDistancer(distancer.(*PQDistancer))
}

func (bq *BinaryQuantizer) CompressedBytes(compressed []uint64) []byte {
	slice := make([]byte, len(compressed)*8)
	for i := range compressed {
		binary.LittleEndian.PutUint64(slice[i*8:], compressed[i])
	}
	return slice
}

func (bq *BinaryQuantizer) FromCompressedBytes(compressed []byte) []uint64 {
	l := len(compressed) / 8
	if len(compressed)%8 != 0 {
		l++
	}
	slice := make([]uint64, l)

	for i := range slice {
		slice[i] = binary.LittleEndian.Uint64(compressed[i*8:])
	}
	return slice
}

func (pq *ProductQuantizer) CompressedBytes(compressed []byte) []byte {
	return compressed
}

func (pq *ProductQuantizer) FromCompressedBytes(compressed []byte) []byte {
	return compressed
}

type BQDistancer struct {
	x          []float32
	bq         *BinaryQuantizer
	compressed []uint64
}

func (bq *BinaryQuantizer) NewDistancer(a []float32) *BQDistancer {
	return &BQDistancer{
		x:          a,
		bq:         bq,
		compressed: bq.Encode(a),
	}
}

func (bq *BinaryQuantizer) NewCompressedQuantizerDistancer(a []uint64) QuantizerDistancer[uint64] {
	return &BQDistancer{
		x:          nil,
		bq:         bq,
		compressed: a,
	}
}

func (d *BQDistancer) Distance(x []uint64) (float32, bool, error) {
	dist, err := d.bq.DistanceBetweenCompressedVectors(d.compressed, x)
	return dist, err == nil, err
}

func (d *BQDistancer) DistanceToFloat(x []float32) (float32, bool, error) {
	if len(d.x) > 0 {
		return d.bq.distancer.SingleDist(d.x, x)
	}
	xComp := d.bq.Encode(x)
	dist, err := d.bq.DistanceBetweenCompressedVectors(d.compressed, xComp)
	return dist, err == nil, err
}

func (bq *BinaryQuantizer) NewQuantizerDistancer(vec []float32) QuantizerDistancer[uint64] {
	return bq.NewDistancer(vec)
}

func (bq *BinaryQuantizer) ReturnQuantizerDistancer(distancer QuantizerDistancer[uint64]) {}

type QuantizedCompressorDistancer[T byte | uint64] struct {
	compressor *QuantizedVectorsCompressor[T]
	distancer  QuantizerDistancer[T]
}

func (distancer *QuantizedCompressorDistancer[T]) DistanceToNode(id uint64) (float32, bool, error) {
	vec, err := distancer.compressor.cache.Get(context.Background(), id)
	if err != nil {
		return 0, false, err
	}
	return distancer.distancer.Distance(vec)
}

func (distancer *QuantizedCompressorDistancer[T]) DistanceToFloat(vec []float32) (float32, bool, error) {
	return distancer.distancer.DistanceToFloat(vec)
}
