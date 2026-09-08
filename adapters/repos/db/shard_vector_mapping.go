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

package db

import (
	"errors"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
)

// The mapping's on-disk layout inside index.db. Every key and field name
// below is read by later versions; changing one is a format change.
const (
	vectorIndexMappingNamespace        = "vector_index_mapping"
	vectorIndexMappingFormatVersionKey = "format_version"
	vectorIndexMappingFormatVersion    = "1"
	vectorIndexMappingLegacyKey        = "legacy"
	vectorIndexMappingNamedPrefix      = "named/"
)

// Record states. There is no dropped state: the schema carries deletion
// intent, and a record outlives it until cleanup completes.
const (
	vectorIndexStateCreating = "creating"
	vectorIndexStateReady    = "ready"
)

var (
	errVectorIndexMappingUninitialized = errors.New("vector index mapping is not initialized")
	errVectorIndexMappingInitialized   = errors.New("vector index mapping is already initialized")
)

// vectorIndexRecord is what the shard persists about one logical vector:
// where its index lives on disk, which implementation opens it, and whether
// creation completed.
type vectorIndexRecord struct {
	PhysicalID string `json:"physical_id"`
	IndexType  string `json:"index_type"`
	State      string `json:"state"`
}

// vectorIndexMapping is a shard's persisted view of which physical vector
// indexes it has: one record per logical vector, in index.db. The legacy
// vector is the empty name.
type vectorIndexMapping struct {
	ns *shardmeta.Namespace
}

func newVectorIndexMapping(db *shardmeta.DB) *vectorIndexMapping {
	return &vectorIndexMapping{ns: db.Namespace(vectorIndexMappingNamespace)}
}

// vectorIndexMappingKey is the on-disk key of a logical vector name.
func vectorIndexMappingKey(name string) string {
	if name == "" {
		return vectorIndexMappingLegacyKey
	}
	return vectorIndexMappingNamedPrefix + name
}

// vectorIndexMappingName is the inverse of vectorIndexMappingKey. ok is
// false for a key that does not name a vector.
func vectorIndexMappingName(key string) (name string, ok bool) {
	if key == vectorIndexMappingLegacyKey {
		return "", true
	}
	if strings.HasPrefix(key, vectorIndexMappingNamedPrefix) {
		return strings.TrimPrefix(key, vectorIndexMappingNamedPrefix), true
	}
	return "", false
}
