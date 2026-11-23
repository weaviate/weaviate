//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package shardmemory estimates the Go heap a shard holds once loaded. It is
// pure arithmetic over plain inputs and does no I/O.
package shardmemory

import "github.com/weaviate/weaviate/usecases/replica/hashtree"

// Index type names as they appear in a usage row. Callers pass a dynamic
// index as the type it currently runs as. A literal "dynamic" counts as
// unknown.
const (
	indexTypeHNSW = "hnsw"
	indexTypeFlat = "flat"
)

const (
	// TODO: uncalibrated placeholder; the calibration run replaces it.
	perShardFloorBytes int64 = 200_000
	// TODO: uncalibrated placeholder; the calibration run replaces it.
	perBucketFloorBytes int64 = 30_000
	// TODO: uncalibrated placeholder; the calibration run replaces it.
	perVectorIndexFloorBytes int64 = 500_000
	// TODO: uncalibrated placeholder; the calibration run replaces it.
	hnswNodeCostBytes int64 = 120
	// TODO: uncalibrated placeholder; the calibration run replaces it.
	hnswLinkCostBytes int64 = 7
)

const (
	float32Bytes        int64 = 4
	hashtreeDigestBytes int64 = 16
	bitsetWordBytes     int64 = 8
)

// VectorIndex describes one named vector of a shard.
type VectorIndex struct {
	IndexType         string
	Compressed        bool
	CompressionRatio  float64
	Dimensions        int64
	VectorCount       int64
	DimensionsTracked bool
	MaxConnections    int64
	CacheLimit        int64
	// FlatQuantizerCache is true when the enabled flat quantizer has Cache set.
	FlatQuantizerCache bool
	Multivector        bool
	PQ                 bool
}

type Inputs struct {
	ObjectCount       int64
	BucketCount       int64
	BloomBytes        int64
	SmallSegmentBytes int64
	AsyncReplication  bool
	HashtreeHeight    int
	Vectors           []VectorIndex
}

type Result struct {
	BaselineBytes int64
	DataBytes     int64
	// Unmodelled is true when some input is outside the model. The byte
	// figures then cover only the modelled parts.
	Unmodelled bool
}

func Estimate(in Inputs) Result {
	res := Result{
		BaselineBytes: perShardFloorBytes +
			perBucketFloorBytes*in.BucketCount +
			perVectorIndexFloorBytes*int64(len(in.Vectors)) +
			hashtreeBytes(in.AsyncReplication, in.HashtreeHeight),
		DataBytes: in.BloomBytes + in.SmallSegmentBytes,
	}

	for _, v := range in.Vectors {
		if v.Multivector || v.PQ || (!v.DimensionsTracked && in.ObjectCount > 0) {
			res.Unmodelled = true
		}

		switch v.IndexType {
		case indexTypeHNSW:
			cached := v.VectorCount
			if !v.Compressed {
				cached = min(cached, v.CacheLimit)
			}
			res.DataBytes += vectorCacheBytes(cached, v.Dimensions, v.CompressionRatio) +
				v.VectorCount*(hnswNodeCostBytes+hnswLinkCostBytes*v.MaxConnections)
		case indexTypeFlat:
			if v.FlatQuantizerCache {
				res.DataBytes += vectorCacheBytes(min(v.VectorCount, v.CacheLimit), v.Dimensions, v.CompressionRatio)
			}
		default:
			// hfresh and every unknown type, including "dynamic".
			res.Unmodelled = true
		}
	}

	return res
}

// hashtreeBytes is zero for a height NewHashTree rejects, because no tree is
// allocated for it.
func hashtreeBytes(on bool, height int) int64 {
	if !on || height < 0 || height > hashtree.MaxHeight {
		return 0
	}
	nodes := int64(hashtree.NodesCount(height))
	return hashtreeDigestBytes*nodes + bitsetWordBytes*((nodes+63)/64)
}

func vectorCacheBytes(count, dims int64, ratio float64) int64 {
	// NaN and ratios below 1 count as 1.
	if !(ratio >= 1) {
		ratio = 1
	}
	return int64(float64(count*dims*float32Bytes) / ratio)
}
