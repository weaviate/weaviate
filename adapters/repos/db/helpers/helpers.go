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

package helpers

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/filters"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

var (
	ObjectsBucket              = []byte("objects")
	ObjectsBucketLSM           = "objects"
	VectorsCompressedBucketLSM = "vectors_compressed"
	VectorsBucketLSM           = "vectors"
	DimensionsBucketLSM        = "dimensions"
)

const ObjectsBucketLSMDocIDSecondaryIndex int = 0

// MetaCountProp helps create an internally used propName for meta props that
// don't explicitly exist in the user schema, but are required for proper
// indexing, such as the count of arrays.
func MetaCountProp(propName string) string {
	return fmt.Sprintf("%s__meta_count", propName)
}

func PropLength(propName string) string {
	return propName + filters.InternalPropertyLength
}

func PropNull(propName string) string {
	return propName + filters.InternalNullIndex
}

// BucketFromPropNameLSM creates string used as the bucket name
// for a particular prop in the inverted index
func BucketFromPropNameLSM(propName string) string {
	return fmt.Sprintf("property_%s", propName)
}

func BucketFromPropNameLengthLSM(propName string) string {
	return BucketFromPropNameLSM(PropLength(propName))
}

func BucketFromPropNameNullLSM(propName string) string {
	return BucketFromPropNameLSM(PropNull(propName))
}

func BucketFromPropNameMetaCountLSM(propName string) string {
	return BucketFromPropNameLSM(MetaCountProp(propName))
}

func TempBucketFromBucketName(bucketName string) string {
	return bucketName + "_temp"
}

func BucketSearchableFromPropNameLSM(propName string) string {
	return BucketFromPropNameLSM(propName + "_searchable")
}

func BucketRangeableFromPropNameLSM(propName string) string {
	return BucketFromPropNameLSM(propName + "_rangeable")
}

// CompressionRatioFromConfig calculates the compression ratio from vector index config
// This is used for inactive tenants where we don't have access to the actual vector index
func CompressionRatioFromConfig(config schemaConfig.VectorIndexConfig, dimensions int) float64 {
	// Check for different compression types in config by type asserting
	if hnswConfig, ok := config.(hnsw.UserConfig); ok {
		// Check for different compression types in HNSW config
		if hnswConfig.PQ.Enabled {
			// PQ compression ratio depends on segments
			segments := hnswConfig.PQ.Segments
			if segments > 0 {
				return float64(dimensions*4) / float64(segments)
			}
		} else if hnswConfig.BQ.Enabled {
			// BQ compression ratio is approximately 32x
			return 1.0 / 32.0
		} else if hnswConfig.SQ.Enabled {
			// SQ compression ratio is approximately 4x
			return 0.25
		}
	} else if flatConfig, ok := config.(flat.UserConfig); ok {
		// Check for different compression types in Flat config
		if flatConfig.BQ.Enabled {
			// BQ compression ratio is approximately 32x
			return 1.0 / 32.0
		} else if flatConfig.PQ.Enabled {
			// PQ compression ratio depends on segments (not supported in flat but handle gracefully)
			return 0.25
		} else if flatConfig.SQ.Enabled {
			// SQ compression ratio is approximately 4x
			return 0.25
		}
	}

	// Default to no compression
	return 1.0
}
