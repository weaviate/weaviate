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
	"fmt"

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

// QuantizerType represents the type of quantizer
type QuantizerType int

const (
	Uint64Quantizer QuantizerType = iota
	ByteQuantizer
)

// String returns the string representation of the compression type
func (ct CompressionType) String() string {
	return string(ct)
}

// IsQuantized returns true if this compression type uses quantization
func (ct CompressionType) IsQuantized() bool {
	return ct == CompressionBQ || ct == CompressionRQ1 || ct == CompressionRQ8
}

// Quantizer represents a quantizer that can work with different data types
type Quantizer interface {
	EncodeUint64(vector []float32) []uint64
	EncodeBytes(vector []float32) []byte

	DistanceBetweenUint64Vectors(x, y []uint64) (float32, error)
	DistanceBetweenByteVectors(x, y []byte) (float32, error)

	PersistCompression(logger compressionhelpers.CommitLogger)
	Stats() compressionhelpers.CompressionStats
	Type() QuantizerType

	FromCompressedBytesToUint64(compressed []byte, buffer *[]uint64) []uint64
	FromCompressedBytesToBytes(compressed []byte, buffer *[]byte) []byte
}

// QuantizerBuilder creates quantizers based on configuration
type QuantizerBuilder struct {
	distancerProvider distancer.Provider
}

// NewQuantizerBuilder creates a new quantizer builder
func NewQuantizerBuilder(distancerProvider distancer.Provider) *QuantizerBuilder {
	return &QuantizerBuilder{
		distancerProvider: distancerProvider,
	}
}

// CreateQuantizer creates a quantizer based on the compression type and dimensions
func (qb *QuantizerBuilder) CreateQuantizer(compression CompressionType, dimensions int32) Quantizer {
	switch compression {
	case CompressionBQ:
		bq := compressionhelpers.NewBinaryQuantizer(nil)
		return &BinaryQuantizerWrapper{BinaryQuantizer: bq}
	case CompressionRQ1:
		rq := compressionhelpers.NewBinaryRotationalQuantizer(int(dimensions), 42, qb.distancerProvider)
		return &BinaryRotationalQuantizerWrapper{BinaryRotationalQuantizer: rq}
	case CompressionRQ8:
		rq := compressionhelpers.NewRotationalQuantizer(int(dimensions), 42, 8, qb.distancerProvider)
		return &RotationalQuantizerWrapper{RotationalQuantizer: rq}
	default:
		return nil
	}
}

// Wrapper implementations for the quantizer interface

// BinaryQuantizerWrapper wraps BinaryQuantizer to implement Quantizer interface
type BinaryQuantizerWrapper struct {
	compressionhelpers.BinaryQuantizer
}

func (b *BinaryQuantizerWrapper) EncodeUint64(vector []float32) []uint64 {
	return b.BinaryQuantizer.Encode(vector)
}

func (b *BinaryQuantizerWrapper) EncodeBytes(vector []float32) []byte {
	// Binary quantizer doesn't support byte encoding
	return nil
}

func (b *BinaryQuantizerWrapper) DistanceBetweenUint64Vectors(x, y []uint64) (float32, error) {
	return b.BinaryQuantizer.DistanceBetweenCompressedVectors(x, y)
}

func (b *BinaryQuantizerWrapper) DistanceBetweenByteVectors(x, y []byte) (float32, error) {
	return 0, fmt.Errorf("binary quantizer does not support byte vectors")
}

func (b *BinaryQuantizerWrapper) Type() QuantizerType {
	return Uint64Quantizer
}

func (b *BinaryQuantizerWrapper) FromCompressedBytesToUint64(compressed []byte, buffer *[]uint64) []uint64 {
	return b.BinaryQuantizer.FromCompressedBytesWithSubsliceBuffer(compressed, buffer)
}

func (b *BinaryQuantizerWrapper) FromCompressedBytesToBytes(compressed []byte, buffer *[]byte) []byte {
	return nil
}

// BinaryRotationalQuantizerWrapper wraps BinaryRotationalQuantizer to implement Quantizer interface
type BinaryRotationalQuantizerWrapper struct {
	*compressionhelpers.BinaryRotationalQuantizer
}

func (r *BinaryRotationalQuantizerWrapper) EncodeUint64(vector []float32) []uint64 {
	return r.BinaryRotationalQuantizer.Encode(vector)
}

func (r *BinaryRotationalQuantizerWrapper) EncodeBytes(vector []float32) []byte {
	// Rotational quantizer doesn't support byte encoding
	return nil
}

func (r *BinaryRotationalQuantizerWrapper) DistanceBetweenUint64Vectors(x, y []uint64) (float32, error) {
	return r.BinaryRotationalQuantizer.DistanceBetweenCompressedVectors(x, y)
}

func (r *BinaryRotationalQuantizerWrapper) DistanceBetweenByteVectors(x, y []byte) (float32, error) {
	return 0, fmt.Errorf("rotational quantizer does not support byte vectors")
}

func (r *BinaryRotationalQuantizerWrapper) Type() QuantizerType {
	return Uint64Quantizer
}

func (r *BinaryRotationalQuantizerWrapper) FromCompressedBytesToUint64(compressed []byte, buffer *[]uint64) []uint64 {
	return r.BinaryRotationalQuantizer.FromCompressedBytesWithSubsliceBuffer(compressed, buffer)
}

func (r *BinaryRotationalQuantizerWrapper) FromCompressedBytesToBytes(compressed []byte, buffer *[]byte) []byte {
	return nil
}

// RotationalQuantizerWrapper wraps RotationalQuantizer to implement Quantizer interface
type RotationalQuantizerWrapper struct {
	*compressionhelpers.RotationalQuantizer
}

func (b *RotationalQuantizerWrapper) EncodeUint64(vector []float32) []uint64 {
	// Byte quantizer doesn't support uint64 encoding
	return nil
}

func (b *RotationalQuantizerWrapper) EncodeBytes(vector []float32) []byte {
	return b.RotationalQuantizer.Encode(vector)
}

func (b *RotationalQuantizerWrapper) DistanceBetweenUint64Vectors(x, y []uint64) (float32, error) {
	return 0, fmt.Errorf("byte quantizer does not support uint64 vectors")
}

func (b *RotationalQuantizerWrapper) DistanceBetweenByteVectors(x, y []byte) (float32, error) {
	return b.RotationalQuantizer.DistanceBetweenCompressedVectors(x, y)
}

func (b *RotationalQuantizerWrapper) Type() QuantizerType {
	return ByteQuantizer
}

func (b *RotationalQuantizerWrapper) FromCompressedBytesToUint64(compressed []byte, buffer *[]uint64) []uint64 {
	return nil
}

func (b *RotationalQuantizerWrapper) FromCompressedBytesToBytes(compressed []byte, buffer *[]byte) []byte {
	return b.RotationalQuantizer.FromCompressedBytesWithSubsliceBuffer(compressed, buffer)
}

// Cache represents a cache that can work with different data types
type Cache struct {
	uint64Cache cache.Cache[uint64]
	byteCache   cache.Cache[byte]
	dataType    QuantizerType
}

// NewCache creates a new cache
func NewCache(getUint64Vector func(ctx context.Context, id uint64) ([]uint64, error), getByteVector func(ctx context.Context, id uint64) ([]byte, error), maxObjects int, logger logrus.FieldLogger, allocChecker memwatch.AllocChecker, quantizerType QuantizerType) *Cache {
	c := &Cache{dataType: quantizerType}

	switch quantizerType {
	case Uint64Quantizer:
		c.uint64Cache = cache.NewShardedUInt64LockCache(getUint64Vector, maxObjects, defaultCachePageSize, logger, 0, allocChecker)
	case ByteQuantizer:
		c.byteCache = cache.NewShardedByteLockCache(getByteVector, maxObjects, defaultCachePageSize, logger, 0, allocChecker)
	}

	return c
}

// Grow grows the cache to accommodate the given ID
func (c *Cache) Grow(id uint64) {
	switch c.dataType {
	case Uint64Quantizer:
		c.uint64Cache.Grow(id)
	case ByteQuantizer:
		c.byteCache.Grow(id)
	}
}

// PreloadUint64 preloads a uint64 vector into the cache
func (c *Cache) PreloadUint64(id uint64, vector []uint64) {
	if c.dataType == Uint64Quantizer {
		c.uint64Cache.Preload(id, vector)
	}
}

// PreloadBytes preloads a byte vector into the cache
func (c *Cache) PreloadBytes(id uint64, vector []byte) {
	if c.dataType == ByteQuantizer {
		c.byteCache.Preload(id, vector)
	}
}

// Delete deletes vectors from the cache
func (c *Cache) Delete(ctx context.Context, ids ...uint64) {
	if c.dataType == Uint64Quantizer {
		for _, id := range ids {
			c.uint64Cache.Delete(ctx, id)
		}
	}
	if c.dataType == ByteQuantizer {
		for _, id := range ids {
			c.byteCache.Delete(ctx, id)
		}
	}
}

// Len returns the current length of the cache
func (c *Cache) Len() int32 {
	if c.dataType == Uint64Quantizer {
		return c.uint64Cache.Len()
	}
	if c.dataType == ByteQuantizer {
		return c.byteCache.Len()
	}
	return 0
}

// PageSize returns the page size of the cache
func (c *Cache) PageSize() uint64 {
	if c.dataType == Uint64Quantizer {
		return c.uint64Cache.PageSize()
	}
	if c.dataType == ByteQuantizer {
		return c.byteCache.PageSize()
	}
	return 0
}

// GetAllUint64InCurrentLock gets all uint64 vectors in the current lock
func (c *Cache) GetAllUint64InCurrentLock(ctx context.Context, start uint64, out [][]uint64, errs []error) ([][]uint64, []error, uint64, uint64) {
	if c.dataType == Uint64Quantizer {
		return c.uint64Cache.GetAllInCurrentLock(ctx, start, out, errs)
	}
	return nil, errs, 0, 0
}

// GetAllBytesInCurrentLock gets all byte vectors in the current lock
func (c *Cache) GetAllBytesInCurrentLock(ctx context.Context, start uint64, out [][]byte, errs []error) ([][]byte, []error, uint64, uint64) {
	if c.dataType == ByteQuantizer {
		return c.byteCache.GetAllInCurrentLock(ctx, start, out, errs)
	}
	return nil, errs, 0, 0
}

// GetUint64 gets a uint64 vector from the cache
func (c *Cache) GetUint64(ctx context.Context, id uint64) ([]uint64, error) {
	if c.dataType == Uint64Quantizer {
		return c.uint64Cache.Get(ctx, id)
	}
	return nil, fmt.Errorf("uint64 cache not available")
}

// GetBytes gets a byte vector from the cache
func (c *Cache) GetBytes(ctx context.Context, id uint64) ([]byte, error) {
	if c.dataType == ByteQuantizer {
		return c.byteCache.Get(ctx, id)
	}
	return nil, fmt.Errorf("byte cache not available")
}

// LockAll locks all cache operations
func (c *Cache) LockAll() {
	if c.dataType == Uint64Quantizer {
		c.uint64Cache.LockAll()
	}
	if c.dataType == ByteQuantizer {
		c.byteCache.LockAll()
	}
}

// UnlockAll unlocks all cache operations
func (c *Cache) UnlockAll() {
	if c.dataType == Uint64Quantizer {
		c.uint64Cache.UnlockAll()
	}
	if c.dataType == ByteQuantizer {
		c.byteCache.UnlockAll()
	}
}

// SetSizeAndGrowNoLockUint64 sets the uint64 cache size and grows without locking
func (c *Cache) SetSizeAndGrowNoLockUint64(maxID uint64) {
	if c.dataType == Uint64Quantizer {
		c.uint64Cache.SetSizeAndGrowNoLock(maxID)
	}
}

// SetSizeAndGrowNoLockBytes sets the byte cache size and grows without locking
func (c *Cache) SetSizeAndGrowNoLockBytes(maxID uint64) {
	if c.dataType == ByteQuantizer {
		c.byteCache.SetSizeAndGrowNoLock(maxID)
	}
}

// PreloadNoLockUint64 preloads a uint64 vector without locking
func (c *Cache) PreloadNoLockUint64(id uint64, vector []uint64) {
	if c.dataType == Uint64Quantizer {
		c.uint64Cache.PreloadNoLock(id, vector)
	}
}

// PreloadNoLockBytes preloads a byte vector without locking
func (c *Cache) PreloadNoLockBytes(id uint64, vector []byte) {
	if c.dataType == ByteQuantizer {
		c.byteCache.PreloadNoLock(id, vector)
	}
}

// IsCached returns true if the cache is available
func (c *Cache) IsCached() bool {
	return c.dataType == Uint64Quantizer || c.dataType == ByteQuantizer
}
