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

// A multivector index keeps one bucket of its own, named off the index ID:
// muvera encodings when muvera is on, node-to-doc mappings when it is off. The
// two are mutually exclusive.
//
// These live here rather than being concatenated at the point of use so the
// code that CREATES the bucket and the drop that removes it share one
// definition. Concatenated inline, a rename on the hnsw side compiles cleanly
// while the cleanup keeps deleting the old name: removeBucket no-ops and the
// leak returns silently.

// MuveraBucketName is the bucket a muvera-encoded multivector index stores its
// encoded vectors in. indexID is the vector index's ID.
func MuveraBucketName(indexID string) string {
	return fmt.Sprintf("%s_muvera_vectors", indexID)
}

// MVMappingsBucketName is the bucket a multivector index WITHOUT muvera stores
// its node-to-doc mappings in. indexID is the vector index's ID.
func MVMappingsBucketName(indexID string) string {
	return fmt.Sprintf("%s_mv_mappings", indexID)
}

// HFresh keeps more on-disk state than the other index types: a directory of
// its own under the shard, plus two dedicated LSM buckets. All three are keyed
// on the index ID (vectorIndexID, i.e. "vectors_<target>" for a named vector),
// and they live here so the index that creates them and the drop that removes
// them cannot drift apart.

// HFreshDirName is the hfresh index's own directory under the shard.
func HFreshDirName(indexID string) string {
	return fmt.Sprintf("%s.hfresh.d", indexID)
}

// HFreshPostingsBucketName is the LSM bucket holding hfresh's posting lists.
func HFreshPostingsBucketName(indexID string) string {
	return fmt.Sprintf("hfresh_postings_%s", indexID)
}

// HFreshSharedBucketName is the LSM bucket holding hfresh's shared metadata.
func HFreshSharedBucketName(indexID string) string {
	return fmt.Sprintf("hfresh_shared_%s", indexID)
}

// FlatMetadataFileName is the flat index's quantisation metadata, under the
// shard directory (see flat.getMetadataFile).
func FlatMetadataFileName(targetVector string) string {
	if targetVector != "" {
		return fmt.Sprintf("meta_%s.db", targetVector)
	}
	return "meta.db"
}

// VectorIndexArtifacts is everything a named vector's index owns on disk:
// LSMBuckets are directories under <shard>/lsm; ShardDirs are entries under
// <shard> — directories, plus the flat metadata file. Both go via os.RemoveAll.
type VectorIndexArtifacts struct {
	LSMBuckets []string
	ShardDirs  []string
}

// All returns every artifact as a single slice, LSM buckets first.
func (a VectorIndexArtifacts) All() []string {
	return append(append([]string{}, a.LSMBuckets...), a.ShardDirs...)
}

// vectorIndexArtifactNames is the raw, unfiltered artifact set for a target
// vector. Split out from VectorIndexArtifactsFor so the sibling-collision guard
// can compute what OTHER vectors own without recursing through the filter.
func vectorIndexArtifactNames(targetVector string) VectorIndexArtifacts {
	indexID := GetVectorsBucketName(targetVector)
	return VectorIndexArtifacts{
		LSMBuckets: []string{
			indexID,                               // raw vectors
			GetCompressedBucketName(targetVector), // BQ/PQ/SQ/RQ
			MuveraBucketName(indexID),             // multivector + muvera
			MVMappingsBucketName(indexID),         // multivector without muvera
			HFreshPostingsBucketName(indexID),     // hfresh
			HFreshSharedBucketName(indexID),       // hfresh
			// hfresh runs a nested centroids HNSW whose id is
			// "<indexID>_centroids"; hnsw derives its compressed bucket from
			// that id with the "vectors_" prefix stripped, so it lands in the
			// shard's lsm dir under this name. Its commitlog and snapshot dirs
			// do NOT need listing — they live inside the .hfresh.d directory
			// below, which goes wholesale.
			GetCompressedBucketName(targetVector + "_centroids"),
		},
		ShardDirs: []string{
			GetHNSWCommitLogDirName(targetVector),
			GetHNSWSnapshotDirName(targetVector),
			HFreshDirName(indexID),
			// The async-indexing queue. The live drop closes it via queue.Drop,
			// but every files-only path (cold lazy shard, inactive tenant, crash
			// before the live drop) leaves it — and DiskQueue.Init replays stale
			// chunks into a re-created index of the same name, so this is wrong
			// vectors and dimension errors, not just disk cost.
			indexID + ".queue.d",
			// flat.Drop removes this on the live path only; the files-only
			// paths leave it, same gap as the queue directory above.
			FlatMetadataFileName(targetVector),
		},
	}
}

// VectorIndexArtifactsFor lists what dropping targetVector has to remove. It is
// the single source of truth for that set: the live drop, the file sweep and
// the tests all read it, because three hand-maintained copies is exactly how
// "<indexID>_mv_mappings" ended up missing from all of them at once.
//
// Entries that only exist for some index types are listed unconditionally:
// removal is a no-op when the artifact is absent, whereas reading the config
// back to decide would miss an index that failed to load, or one whose config
// changed since it was written.
//
// otherTargetVectors is not optional. TargetVectorNameRegex permits names like
// "<other>_muvera_vectors" or "<other>_centroids", which make one of THIS
// target's artifacts byte-identical to a bucket a live sibling owns. Any
// artifact a sibling claims is therefore dropped from the list: leaking beats
// deleting data that is still in use.
func VectorIndexArtifactsFor(targetVector string, otherTargetVectors []string) VectorIndexArtifacts {
	artifacts := vectorIndexArtifactNames(targetVector)

	// Skipping the target itself is what keeps its OWN artifacts in the list,
	// for a caller that passes the whole schema rather than filtering first.
	protected := map[string]struct{}{}
	for _, other := range otherTargetVectors {
		if other == targetVector {
			continue
		}
		for _, name := range vectorIndexArtifactNames(other).All() {
			protected[name] = struct{}{}
		}
	}
	if len(protected) == 0 {
		return artifacts
	}

	// Only LSM buckets can collide. Every ShardDirs entry ends in a dotted
	// suffix (".hnsw.commitlog.d", ".hfresh.d", ".queue.d") and
	// TargetVectorNameRegex forbids dots in vector names, so no sibling's
	// artifact can ever equal one — filtering them would be unreachable code.
	// A future shard directory WITHOUT a dotted suffix would break that and
	// needs the guard extended.
	keptBuckets := artifacts.LSMBuckets[:0:0]
	for _, name := range artifacts.LSMBuckets {
		if _, clash := protected[name]; clash {
			continue
		}
		keptBuckets = append(keptBuckets, name)
	}
	artifacts.LSMBuckets = keptBuckets
	return artifacts
}

func GetHNSWCommitLogDirName(targetVector string) string {
	if targetVector != "" {
		return fmt.Sprintf("%s.hnsw.commitlog.d", GetVectorsBucketName(targetVector))
	}
	return "main.hnsw.commitlog.d"
}

func GetHNSWSnapshotDirName(targetVector string) string {
	if targetVector != "" {
		return fmt.Sprintf("%s.hnsw.snapshot.d", GetVectorsBucketName(targetVector))
	}
	return "main.hnsw.snapshot.d"
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
