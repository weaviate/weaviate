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

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// CompressionType represents the type of compression used
type CompressionType string

const (
	CompressionNone CompressionType = "none"
	CompressionBQ   CompressionType = "bq"
	CompressionPQ   CompressionType = "pq"
	CompressionSQ   CompressionType = "sq"
	CompressionRQ1  CompressionType = "rq-1"
	CompressionRQ8  CompressionType = "rq-8"
)

// String returns the string representation of the compression type
func (ct CompressionType) String() string {
	return string(ct)
}

// IsQuantized returns true if this compression type uses quantization
func (ct CompressionType) IsQuantized() bool {
	return ct == CompressionBQ || ct == CompressionRQ1 || ct == CompressionRQ8
}

// Quantizer represents a generic quantizer that can encode vectors and compute distances
type Quantizer[T byte | uint64 | float32] interface {
	// Encode converts a float32 vector to compressed T representation
	Encode(vector []float32) []T

	// DistanceBetweenCompressedVectors computes distance between two compressed vectors
	DistanceBetweenCompressedVectors(x, y []T) (float32, error)

	// PersistCompression persists quantizer data for restoration
	PersistCompression(logger compressionhelpers.CommitLogger)

	// Stats returns compression statistics
	Stats() compressionhelpers.CompressionStats
}

// QuantizerFactory creates quantizers based on configuration
type QuantizerFactory struct {
	distancerProvider distancer.Provider
}

// NewQuantizerFactory creates a new quantizer factory
func NewQuantizerFactory(distancerProvider distancer.Provider) *QuantizerFactory {
	return &QuantizerFactory{
		distancerProvider: distancerProvider,
	}
}

// CreateQuantizer creates a quantizer based on the compression type and dimensions
func (qf *QuantizerFactory) CreateQuantizer(compression CompressionType, dimensions int32) Quantizer[uint64] {
	switch compression {
	case CompressionBQ:
		bq := compressionhelpers.NewBinaryQuantizer(nil)
		return &BinaryQuantizerWrapper{BinaryQuantizer: bq}
	case CompressionRQ1:
		rq := compressionhelpers.NewBinaryRotationalQuantizer(int(dimensions), 42, qf.distancerProvider)
		return &RotationalQuantizerWrapper{BinaryRotationalQuantizer: rq}
	default:
		return nil
	}
}

// CreateByteQuantizer creates a byte quantizer based on the compression type and dimensions
func (qf *QuantizerFactory) CreateByteQuantizer(compression CompressionType, dimensions int32) Quantizer[byte] {
	switch compression {
	case CompressionRQ8:
		rq := compressionhelpers.NewRotationalQuantizer(int(dimensions), 42, 8, qf.distancerProvider)
		return &ByteQuantizerWrapper{RotationalQuantizer: rq}
	default:
		return nil
	}
}

// BinaryQuantizerWrapper wraps BinaryQuantizer to implement Quantizer interface
type BinaryQuantizerWrapper struct {
	compressionhelpers.BinaryQuantizer
}

// FromCompressedBytesWithSubsliceBuffer implements the method needed for parallel iteration
func (bq *BinaryQuantizerWrapper) FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]uint64) []uint64 {
	return bq.BinaryQuantizer.FromCompressedBytesWithSubsliceBuffer(compressed, buffer)
}

// RotationalQuantizerWrapper wraps BinaryRotationalQuantizer to implement Quantizer interface
type RotationalQuantizerWrapper struct {
	*compressionhelpers.BinaryRotationalQuantizer
}

// FromCompressedBytesWithSubsliceBuffer implements the method needed for parallel iteration
func (rq *RotationalQuantizerWrapper) FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]uint64) []uint64 {
	return rq.BinaryRotationalQuantizer.FromCompressedBytesWithSubsliceBuffer(compressed, buffer)
}

// ByteQuantizerWrapper wraps RotationalQuantizer to implement Quantizer interface for byte arrays
type ByteQuantizerWrapper struct {
	*compressionhelpers.RotationalQuantizer
}

// FromCompressedBytesWithSubsliceBuffer implements the method needed for parallel iteration
func (bq *ByteQuantizerWrapper) FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]byte) []byte {
	return bq.RotationalQuantizer.FromCompressedBytesWithSubsliceBuffer(compressed, buffer)
}

// QuantizedCache represents a cache for quantized uint64 vectors
type QuantizedCache struct {
	cache cache.Cache[uint64]
}

// NewQuantizedCache creates a new quantized cache
func NewQuantizedCache(getVector func(ctx context.Context, id uint64) ([]uint64, error), maxObjects int, logger logrus.FieldLogger, allocChecker memwatch.AllocChecker) *QuantizedCache {
	c := cache.NewShardedUInt64LockCache(getVector, maxObjects, defaultCachePageSize, logger, 0, allocChecker)
	return &QuantizedCache{cache: c}
}

// ByteQuantizedCache represents a cache for quantized byte vectors
type ByteQuantizedCache struct {
	cache cache.Cache[byte]
}

// NewByteQuantizedCache creates a new byte quantized cache
func NewByteQuantizedCache(getVector func(ctx context.Context, id uint64) ([]byte, error), maxObjects int, logger logrus.FieldLogger, allocChecker memwatch.AllocChecker) *ByteQuantizedCache {
	c := cache.NewShardedByteLockCache(getVector, maxObjects, defaultCachePageSize, logger, 0, allocChecker)
	return &ByteQuantizedCache{cache: c}
}

// Grow grows the cache to accommodate the given ID
func (qc *QuantizedCache) Grow(id uint64) {
	qc.cache.Grow(id)
}

// Preload preloads a vector into the cache
func (qc *QuantizedCache) Preload(id uint64, vector []uint64) {
	qc.cache.Preload(id, vector)
}

// Delete deletes vectors from the cache
func (qc *QuantizedCache) Delete(ctx context.Context, ids ...uint64) {
	for _, id := range ids {
		qc.cache.Delete(ctx, id)
	}
}

// Len returns the current length of the cache
func (qc *QuantizedCache) Len() int32 {
	return qc.cache.Len()
}

// PageSize returns the page size of the cache
func (qc *QuantizedCache) PageSize() uint64 {
	return qc.cache.PageSize()
}

// GetAllInCurrentLock gets all vectors in the current lock
func (qc *QuantizedCache) GetAllInCurrentLock(ctx context.Context, start uint64, out [][]uint64, errs []error) ([][]uint64, []error, uint64, uint64) {
	return qc.cache.GetAllInCurrentLock(ctx, start, out, errs)
}

// Get gets a vector from the cache
func (qc *QuantizedCache) Get(ctx context.Context, id uint64) ([]uint64, error) {
	return qc.cache.Get(ctx, id)
}

// LockAll locks all cache operations
func (qc *QuantizedCache) LockAll() {
	qc.cache.LockAll()
}

// UnlockAll unlocks all cache operations
func (qc *QuantizedCache) UnlockAll() {
	qc.cache.UnlockAll()
}

// SetSizeAndGrowNoLock sets the cache size and grows without locking
func (qc *QuantizedCache) SetSizeAndGrowNoLock(maxID uint64) {
	qc.cache.SetSizeAndGrowNoLock(maxID)
}

// PreloadNoLock preloads a vector without locking
func (qc *QuantizedCache) PreloadNoLock(id uint64, vector []uint64) {
	qc.cache.PreloadNoLock(id, vector)
}

// IsCached returns true if the cache is available
func (qc *QuantizedCache) IsCached() bool {
	return qc.cache != nil
}

// Grow grows the cache to accommodate the given ID
func (bqc *ByteQuantizedCache) Grow(id uint64) {
	bqc.cache.Grow(id)
}

// Preload preloads a vector into the cache
func (bqc *ByteQuantizedCache) Preload(id uint64, vector []byte) {
	bqc.cache.Preload(id, vector)
}

// Delete deletes vectors from the cache
func (bqc *ByteQuantizedCache) Delete(ctx context.Context, ids ...uint64) {
	for _, id := range ids {
		bqc.cache.Delete(ctx, id)
	}
}

// Len returns the current length of the cache
func (bqc *ByteQuantizedCache) Len() int32 {
	return bqc.cache.Len()
}

// PageSize returns the page size of the cache
func (bqc *ByteQuantizedCache) PageSize() uint64 {
	return bqc.cache.PageSize()
}

// GetAllInCurrentLock gets all vectors in the current lock
func (bqc *ByteQuantizedCache) GetAllInCurrentLock(ctx context.Context, start uint64, out [][]byte, errs []error) ([][]byte, []error, uint64, uint64) {
	return bqc.cache.GetAllInCurrentLock(ctx, start, out, errs)
}

// Get gets a vector from the cache
func (bqc *ByteQuantizedCache) Get(ctx context.Context, id uint64) ([]byte, error) {
	return bqc.cache.Get(ctx, id)
}

// LockAll locks all cache operations
func (bqc *ByteQuantizedCache) LockAll() {
	bqc.cache.LockAll()
}

// UnlockAll unlocks all cache operations
func (bqc *ByteQuantizedCache) UnlockAll() {
	bqc.cache.UnlockAll()
}

// SetSizeAndGrowNoLock sets the cache size and grows without locking
func (bqc *ByteQuantizedCache) SetSizeAndGrowNoLock(maxID uint64) {
	bqc.cache.SetSizeAndGrowNoLock(maxID)
}

// PreloadNoLock preloads a vector without locking
func (bqc *ByteQuantizedCache) PreloadNoLock(id uint64, vector []byte) {
	bqc.cache.PreloadNoLock(id, vector)
}

// IsCached returns true if the cache is available
func (bqc *ByteQuantizedCache) IsCached() bool {
	return bqc.cache != nil
}
