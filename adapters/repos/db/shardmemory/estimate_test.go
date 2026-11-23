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

package shardmemory

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEstimate(t *testing.T) {
	hnsw := VectorIndex{
		IndexType:         "hnsw",
		CompressionRatio:  1,
		Dimensions:        128,
		VectorCount:       50_000,
		DimensionsTracked: true,
		MaxConnections:    32,
		CacheLimit:        10_000,
	}
	flat := VectorIndex{
		IndexType:         "flat",
		CompressionRatio:  1,
		Dimensions:        128,
		VectorCount:       50_000,
		DimensionsTracked: true,
		CacheLimit:        10_000,
	}
	with := func(v VectorIndex, edit func(*VectorIndex)) VectorIndex {
		edit(&v)
		return v
	}

	tests := []struct {
		name string
		in   Inputs
		want Result
	}{
		{
			name: "no vectors gives baseline only",
			in:   Inputs{ObjectCount: 1_000, BucketCount: 5},
			want: Result{BaselineBytes: perShardFloorBytes + 5*perBucketFloorBytes},
		},
		{
			name: "bloom and small-segment bytes add to data",
			in:   Inputs{BucketCount: 2, BloomBytes: 1_000, SmallSegmentBytes: 20_000},
			want: Result{
				BaselineBytes: perShardFloorBytes + 2*perBucketFloorBytes,
				DataBytes:     1_000 + 20_000,
			},
		},
		{
			name: "hashtree on at height 10 adds its node and bitset bytes",
			in:   Inputs{AsyncReplication: true, HashtreeHeight: 10},
			want: Result{BaselineBytes: perShardFloorBytes + 16*2047 + 8*32},
		},
		{
			name: "hashtree on at height 16 adds its node and bitset bytes",
			in:   Inputs{AsyncReplication: true, HashtreeHeight: 16},
			want: Result{BaselineBytes: perShardFloorBytes + 16*131_071 + 8*2_048},
		},
		{
			name: "hashtree off adds nothing",
			in:   Inputs{AsyncReplication: false, HashtreeHeight: 16},
			want: Result{BaselineBytes: perShardFloorBytes},
		},
		{
			name: "negative hashtree height adds nothing",
			in:   Inputs{AsyncReplication: true, HashtreeHeight: -5},
			want: Result{BaselineBytes: perShardFloorBytes},
		},
		{
			name: "hashtree height above the max adds nothing",
			in:   Inputs{AsyncReplication: true, HashtreeHeight: 64},
			want: Result{BaselineBytes: perShardFloorBytes},
		},
		{
			name: "uncompressed HNSW is capped by the cache limit",
			in:   Inputs{ObjectCount: 50_000, Vectors: []VectorIndex{hnsw}},
			want: Result{
				BaselineBytes: perShardFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     10_000*128*4 + 50_000*(hnswNodeCostBytes+32*hnswLinkCostBytes),
			},
		},
		{
			name: "compressed HNSW ignores the limit",
			in: Inputs{ObjectCount: 50_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) { v.Compressed = true; v.CompressionRatio = 32 }),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     50_000*128*4/32 + 50_000*(hnswNodeCostBytes+32*hnswLinkCostBytes),
			},
		},
		{
			name: "zero ratio is treated as 1",
			in: Inputs{ObjectCount: 1_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) { v.CompressionRatio = 0; v.VectorCount = 1_000 }),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     1_000*128*4 + 1_000*(hnswNodeCostBytes+32*hnswLinkCostBytes),
			},
		},
		{
			name: "ratio below 1 is treated as 1",
			in: Inputs{ObjectCount: 1_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) { v.CompressionRatio = 0.5; v.VectorCount = 1_000 }),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     1_000*128*4 + 1_000*(hnswNodeCostBytes+32*hnswLinkCostBytes),
			},
		},
		{
			name: "NaN ratio is treated as 1",
			in: Inputs{ObjectCount: 1_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) { v.CompressionRatio = math.NaN(); v.VectorCount = 1_000 }),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     1_000*128*4 + 1_000*(hnswNodeCostBytes+32*hnswLinkCostBytes),
			},
		},
		{
			name: "millions of high-dimensional vectors do not overflow",
			in: Inputs{ObjectCount: 10_000_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) {
					v.VectorCount = 10_000_000
					v.Dimensions = 4_096
					v.CacheLimit = 1_000_000_000_000
					v.MaxConnections = 64
				}),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     10_000_000*4_096*4 + 10_000_000*(hnswNodeCostBytes+64*hnswLinkCostBytes),
			},
		},
		{
			name: "flat without its quantizer cache has no data term",
			in:   Inputs{ObjectCount: 50_000, Vectors: []VectorIndex{flat}},
			want: Result{BaselineBytes: perShardFloorBytes + perVectorIndexFloorBytes},
		},
		{
			name: "flat with its quantizer cache is capped by the cache limit",
			in: Inputs{ObjectCount: 50_000, Vectors: []VectorIndex{
				with(flat, func(v *VectorIndex) {
					v.FlatQuantizerCache = true
					v.Compressed = true
					v.CompressionRatio = 32
				}),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     10_000 * 128 * 4 / 32,
			},
		},
		{
			name: "dynamic resolved to flat costs as flat",
			in: Inputs{ObjectCount: 2_000, Vectors: []VectorIndex{
				with(flat, func(v *VectorIndex) {
					v.FlatQuantizerCache = true
					v.Compressed = true
					v.CompressionRatio = 32
					v.VectorCount = 2_000
				}),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     2_000 * 128 * 4 / 32,
			},
		},
		{
			name: "dynamic resolved to HNSW costs as HNSW",
			in: Inputs{ObjectCount: 50_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) { v.Compressed = true; v.CompressionRatio = 4 }),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     50_000*128*4/4 + 50_000*(hnswNodeCostBytes+32*hnswLinkCostBytes),
			},
		},
		{
			name: "unresolved dynamic type is unknown",
			in: Inputs{ObjectCount: 50_000, BucketCount: 3, BloomBytes: 1_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) { v.IndexType = "dynamic" }),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + 3*perBucketFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     1_000,
				Unmodelled:    true,
			},
		},
		{
			name: "hfresh sets Unmodelled and adds only its floor",
			in: Inputs{ObjectCount: 50_000, BucketCount: 3, BloomBytes: 1_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) { v.IndexType = "hfresh" }),
				flat,
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + 3*perBucketFloorBytes + 2*perVectorIndexFloorBytes,
				DataBytes:     1_000,
				Unmodelled:    true,
			},
		},
		{
			name: "unknown type sets Unmodelled and adds only its floor",
			in: Inputs{ObjectCount: 50_000, BucketCount: 3, BloomBytes: 1_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) { v.IndexType = "noop" }),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + 3*perBucketFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     1_000,
				Unmodelled:    true,
			},
		},
		{
			name: "multivector sets Unmodelled and keeps the HNSW terms",
			in: Inputs{ObjectCount: 50_000, BucketCount: 3, BloomBytes: 1_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) { v.Multivector = true }),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + 3*perBucketFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     1_000 + 10_000*128*4 + 50_000*(hnswNodeCostBytes+32*hnswLinkCostBytes),
				Unmodelled:    true,
			},
		},
		{
			name: "PQ sets Unmodelled and keeps the HNSW terms",
			in: Inputs{ObjectCount: 50_000, BucketCount: 3, BloomBytes: 1_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) { v.PQ = true; v.Compressed = true; v.CompressionRatio = 16 }),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + 3*perBucketFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     1_000 + 50_000*128*4/16 + 50_000*(hnswNodeCostBytes+32*hnswLinkCostBytes),
				Unmodelled:    true,
			},
		},
		{
			name: "untracked dimensions on a shard with objects set Unmodelled",
			in: Inputs{ObjectCount: 50_000, BucketCount: 3, BloomBytes: 1_000, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) { v.DimensionsTracked = false; v.Dimensions = 0 }),
			}},
			want: Result{
				BaselineBytes: perShardFloorBytes + 3*perBucketFloorBytes + perVectorIndexFloorBytes,
				DataBytes:     1_000 + 50_000*(hnswNodeCostBytes+32*hnswLinkCostBytes),
				Unmodelled:    true,
			},
		},
		{
			name: "untracked dimensions on an empty shard stay modelled",
			in: Inputs{ObjectCount: 0, Vectors: []VectorIndex{
				with(hnsw, func(v *VectorIndex) {
					v.DimensionsTracked = false
					v.Dimensions = 0
					v.VectorCount = 0
				}),
			}},
			want: Result{BaselineBytes: perShardFloorBytes + perVectorIndexFloorBytes},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Estimate(tt.in))
		})
	}
}
