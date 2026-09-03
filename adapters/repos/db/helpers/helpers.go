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

package helpers

import (
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

var (
	ObjectsBucket              = []byte("objects")
	ObjectsBucketLSM           = "objects"
	VectorsBucketLSM           = "vectors"
	DimensionsBucketLSM        = "dimensions"
	VectorsCompressedBucketLSM = "vectors_compressed"
)

const ObjectsBucketLSMDocIDSecondaryIndex int = 0

func GetCompressedBucketName(targetVector string) string {
	if targetVector != "" {
		return fmt.Sprintf("%s_%s", VectorsCompressedBucketLSM, targetVector)
	}
	return VectorsCompressedBucketLSM
}

func GetVectorsBucketName(targetVector string) string {
	if targetVector != "" {
		return fmt.Sprintf("%s_%s", VectorsBucketLSM, targetVector)
	}
	return VectorsBucketLSM
}

// VectorIndexIDForTarget derives the canonical physical index ID for a
// target vector: "main" for the legacy unnamed vector, "vectors_<tv>"
// otherwise. This is the same rule as the shard's private vectorIndexID;
// it lives here so packages below the shard can derive it too.
func VectorIndexIDForTarget(targetVector string) string {
	if targetVector != "" {
		return fmt.Sprintf("%s_%s", VectorsBucketLSM, targetVector)
	}
	return "main"
}

// PhysicalIDSuffix extracts the naming suffix from a physical index ID:
// "vectors_<x>" → "<x>", anything else (including "main") → "". Suffix-based
// names (compressed bucket, flat metadata, dynamic state key) append
// "_<suffix>" when it is non-empty and use their bare legacy form otherwise.
// The anything-else→"" rule reproduces hnsw's historical CutPrefix fallback,
// which is what shipped bytes on disk for geo and centroid IDs depend on.
func PhysicalIDSuffix(physicalID string) string {
	if suffix, found := strings.CutPrefix(physicalID, VectorsBucketLSM+"_"); found {
		return suffix
	}
	return ""
}

// VectorsBucketNameForID names the raw-vectors LSM bucket for a physical
// index ID. NOTE the legacy asymmetry: ID "main" stores raw vectors in
// bucket "vectors", not "main".
func VectorsBucketNameForID(physicalID string) string {
	return GetVectorsBucketName(PhysicalIDSuffix(physicalID))
}

// CompressedBucketNameForID names the quantized-vectors LSM bucket for a
// physical index ID.
func CompressedBucketNameForID(physicalID string) string {
	return GetCompressedBucketName(PhysicalIDSuffix(physicalID))
}

// HFresh keeps more on-disk state than the other index types: a directory of
// its own under the shard, plus two dedicated LSM buckets. All three are keyed
// on the index ID (vectorIndexID, i.e. "vectors_<target>" for a named vector).
// The postings and shared bucket names now live in package hfresh, next to the
// index that creates them; HFreshDirName and CentroidsID stay here because the
// shard's initVectorIndex uses them directly.

// HFreshDirName is the hfresh index's own directory under the shard.
func HFreshDirName(indexID string) string {
	return fmt.Sprintf("%s.hfresh.d", indexID)
}

// CentroidsID derives the hfresh centroid sub-index's physical ID from its
// parent's. The centroid HNSW stores its compressed bucket in the SHARD's
// lsm dir under the name this ID yields via CompressedBucketNameForID.
func CentroidsID(physicalID string) string {
	return physicalID + "_centroids"
}

// MetaCountProp helps create an internally used propName for meta props that
// don't explicitly exist in the user schema, but are required for proper
// indexing, such as the count of arrays.
func MetaCountProp(propName string) string {
	return propName + schema.InternalMetaCountSuffix
}

func PropLength(propName string) string {
	return propName + schema.InternalPropertyLengthSuffix
}

func PropNull(propName string) string {
	return propName + schema.InternalNullStateSuffix
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
	return bucketName + schema.InternalTempSuffix
}

func BucketNestedFromPropNameLSM(propName string) string {
	return fmt.Sprintf("property.nested_%s", propName)
}

func BucketNestedMetaFromPropNameLSM(propName string) string {
	return fmt.Sprintf("property.nestedmeta_%s", propName)
}

func BucketSearchableFromPropNameLSM(propName string) string {
	return BucketFromPropNameLSM(propName + schema.InternalSearchableSuffix)
}

func BucketRangeableFromPropNameLSM(propName string) string {
	return BucketFromPropNameLSM(propName + schema.InternalRangeableSuffix)
}

// propertyBucketGenSuffix returns the suffix appended to a property bucket
// name to disambiguate generations created by semantic runtime-reindex
// migrations. Generation 0 (the default for never-migrated properties)
// returns "" so the bucket name matches the legacy unsuffixed form —
// existing clusters on disk continue to find their buckets without a
// rename. Generations >= 1 return "__gen<N>".
//
// The double-underscore is chosen to be unambiguous: property names are
// user-supplied identifiers that, in practice, do not end in "__gen<digits>".
// Even if one did, the suffix would never collide because the resolver
// only appends a non-empty suffix when the schema-tracked
// BucketGeneration is non-zero.
func propertyBucketGenSuffix(gen int64) string {
	if gen <= 0 {
		return ""
	}
	return fmt.Sprintf("__gen%d", gen)
}

// BucketFromPropNameLSMAtGen is the generation-aware variant of
// [BucketFromPropNameLSM]. For gen=0 it returns the legacy unsuffixed
// bucket name (no behavior change for properties that have never been
// semantically reindexed). For gen>=1 the returned name includes a
// "__gen<N>" suffix so old and new generations can coexist on disk during
// a migration: the reindex builds the next-generation bucket alongside
// the active one, and the cluster-wide cutover is the RAFT commit that
// bumps the property's BucketGeneration field.
func BucketFromPropNameLSMAtGen(propName string, gen int64) string {
	return BucketFromPropNameLSM(propName) + propertyBucketGenSuffix(gen)
}

// BucketSearchableFromPropNameLSMAtGen is the generation-aware variant of
// [BucketSearchableFromPropNameLSM]. See [BucketFromPropNameLSMAtGen] for
// the cutover semantics.
func BucketSearchableFromPropNameLSMAtGen(propName string, gen int64) string {
	return BucketSearchableFromPropNameLSM(propName) + propertyBucketGenSuffix(gen)
}

// BucketRangeableFromPropNameLSMAtGen is the generation-aware variant of
// [BucketRangeableFromPropNameLSM]. See [BucketFromPropNameLSMAtGen] for
// the cutover semantics.
func BucketRangeableFromPropNameLSMAtGen(propName string, gen int64) string {
	return BucketRangeableFromPropNameLSM(propName) + propertyBucketGenSuffix(gen)
}

// BucketFromPropertyLSM returns the filterable bucket name for a property
// at its currently-active generation, as recorded in the schema. Equivalent
// to [BucketFromPropNameLSMAtGen] with the property's BucketGeneration.
// Prefer this form at sites that already hold a *models.Property — the
// schema is the single source of truth for which generation is active,
// so passing the resolved name into a downstream API risks staleness.
func BucketFromPropertyLSM(prop *models.Property) string {
	return BucketFromPropNameLSMAtGen(prop.Name, prop.BucketGeneration)
}

// BucketSearchableFromPropertyLSM returns the searchable bucket name for
// a property at its currently-active generation. See [BucketFromPropertyLSM].
func BucketSearchableFromPropertyLSM(prop *models.Property) string {
	return BucketSearchableFromPropNameLSMAtGen(prop.Name, prop.BucketGeneration)
}

// BucketRangeableFromPropertyLSM returns the rangeable bucket name for
// a property at its currently-active generation. See [BucketFromPropertyLSM].
func BucketRangeableFromPropertyLSM(prop *models.Property) string {
	return BucketRangeableFromPropNameLSMAtGen(prop.Name, prop.BucketGeneration)
}
