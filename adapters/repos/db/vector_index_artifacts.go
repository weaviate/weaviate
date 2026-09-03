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
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
)

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
//
// Every name is derived from the canonical physical ID through the same ForID
// helpers the live indexes use, so the list and the storage cannot disagree.
// For a named vector the ID is "vectors_<tv>" and every string is what it
// always was; for the legacy unnamed vector ("") the ID is "main", which the
// previous derivation got wrong (it assumed "vectors").
func vectorIndexArtifactNames(targetVector string) VectorIndexArtifacts {
	indexID := helpers.VectorIndexIDForTarget(targetVector)
	return VectorIndexArtifacts{
		LSMBuckets: []string{
			helpers.VectorsBucketNameForID(indexID),    // raw vectors
			helpers.CompressedBucketNameForID(indexID), // BQ/PQ/SQ/RQ
			hnsw.MuveraBucketName(indexID),             // multivector + muvera
			hnsw.MVMappingsBucketName(indexID),         // multivector without muvera
			helpers.HFreshPostingsBucketName(indexID),  // hfresh
			helpers.HFreshSharedBucketName(indexID),    // hfresh
			// hfresh runs a nested centroids HNSW whose physical id is
			// CentroidsID(indexID); hnsw derives its compressed bucket from that
			// id the same way as any other, so it lands in the shard's lsm dir
			// under this name. Its commitlog and snapshot dirs do NOT need
			// listing — they live inside the .hfresh.d directory below, which
			// goes wholesale.
			helpers.CompressedBucketNameForID(helpers.CentroidsID(indexID)),
		},
		ShardDirs: []string{
			hnsw.CommitLogDirName(indexID),
			hnsw.SnapshotDirName(indexID),
			helpers.HFreshDirName(indexID),
			// The async-indexing queue. The live drop closes it via queue.Drop,
			// but every files-only path (cold lazy shard, inactive tenant, crash
			// before the live drop) leaves it — and DiskQueue.Init replays stale
			// chunks into a re-created index of the same name, so this is wrong
			// vectors and dimension errors, not just disk cost.
			indexID + ".queue.d",
			// flat.Drop removes this on the live path only; the files-only
			// paths leave it, same gap as the queue directory above.
			helpers.FlatMetadataFileNameForID(indexID),
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
