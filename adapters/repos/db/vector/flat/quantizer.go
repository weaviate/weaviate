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
)

// String returns the string representation of the compression type
func (ct CompressionType) String() string {
	return string(ct)
}

// IsQuantized returns true if this compression type uses quantization
func (ct CompressionType) IsQuantized() bool {
	return ct == CompressionBQ || ct == CompressionRQ1
}

// Quantizer represents a generic quantizer that can encode vectors and compute distances
type Quantizer interface {
	// Encode converts a float32 vector to compressed uint64 representation
	Encode(vector []float32) []uint64

	// DistanceBetweenCompressedVectors computes distance between two compressed vectors
	DistanceBetweenCompressedVectors(x, y []uint64) (float32, error)

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
func (qf *QuantizerFactory) CreateQuantizer(compression CompressionType, dimensions int32) Quantizer {
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

// QuantizedCache represents a generic cache for quantized vectors
type QuantizedCache struct {
	cache cache.Cache[uint64]
}

// NewQuantizedCache creates a new quantized cache
func NewQuantizedCache(getVector func(ctx context.Context, id uint64) ([]uint64, error), maxObjects int, logger logrus.FieldLogger, allocChecker memwatch.AllocChecker) *QuantizedCache {
	c := cache.NewShardedUInt64LockCache(getVector, maxObjects, defaultCachePageSize, logger, 0, allocChecker)
	return &QuantizedCache{cache: c}
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
