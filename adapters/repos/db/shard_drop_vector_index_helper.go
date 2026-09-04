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
	"fmt"
	"os"
	"path/filepath"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/vectorindex"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type vectorDropIndexHelper struct{}

func newVectorDropIndexHelper() *vectorDropIndexHelper {
	return &vectorDropIndexHelper{}
}

// ensureFilesAreRemovedForDroppedVectorIndexes removes vector index files
// for named vectors whose index has been dropped (VectorIndexType set to "none").
// This handles two cases:
// - tenant was inactive during a drop vector index operation, so files remain on disk
// - an error occurred during the drop operation and files were not fully cleaned up
func (h *vectorDropIndexHelper) ensureFilesAreRemovedForDroppedVectorIndexes(
	indexPath, shardName string, class *models.Class,
) error {
	for name, cfg := range class.VectorConfig {
		if !modelsext.IsVectorIndexDropped(cfg) {
			continue
		}
		if err := h.removeVectorIndexFiles(indexPath, shardName, name,
			siblingVectors(class, name)); err != nil {
			return fmt.Errorf("failed to remove dropped vector index %q files for class %s: %w",
				name, class.Class, err)
		}
	}
	return nil
}

// removeVectorIndexFiles removes every on-disk artifact of a named vector index
// (see helpers.VectorIndexArtifactsFor for the set and why it is centralised).
// siblings are the collection's remaining vectors, needed so a sibling whose
// own bucket collides with one of this target's artifact names is not deleted
// along with it.
func (h *vectorDropIndexHelper) removeVectorIndexFiles(
	indexPath, shardName, targetVector string, siblings []helpers.SiblingVector,
) error {
	lsmDir := filepath.Join(indexPath, shardName, "lsm")
	shardDir := filepath.Join(indexPath, shardName)

	artifacts := helpers.VectorIndexArtifactsFor(targetVector, siblings)

	var dirs []string
	for _, bucket := range artifacts.LSMBuckets {
		dirs = append(dirs, filepath.Join(lsmDir, bucket))
	}
	for _, dir := range artifacts.ShardDirs {
		dirs = append(dirs, filepath.Join(shardDir, dir))
	}

	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove %s: %w", dir, err)
		}
	}

	// A dynamic index records its flat-to-hnsw upgrade as a key in the shard's
	// index.db, which no artifact above can reach: that file is one per shard,
	// not one per vector, so removing it would take every sibling's state too.
	//
	// Unconditional because nothing here can tell a dynamic vector from any
	// other — the drop rewrote this entry's VectorIndexType to "none" and
	// discarded the original type along with its config. A shard that never ran
	// a dynamic index has no index.db, so this costs it one failed open.
	if err := dynamic.RemoveStateKey(shardDir, targetVector); err != nil {
		return fmt.Errorf("remove dynamic state for %q: %w", targetVector, err)
	}

	return nil
}

// siblingVectors lists the collection's vectors except `exclude` — the
// siblings whose artifacts a drop must not touch — each marked with whether
// its index can own a compressed bucket.
//
// The legacy vector ("") is a sibling too, although it never appears in
// VectorConfig: a class that gained named vectors next to its legacy one can
// hold a named vector whose artifacts collide with the legacy vector's, e.g.
// one called "compressed" owns the raw bucket "vectors_compressed", which is
// the legacy vector's quantized bucket. Left out, the legacy vector's data
// went with such a drop.
func siblingVectors(class *models.Class, exclude string) []helpers.SiblingVector {
	if class == nil {
		// Nothing to protect, so the drop runs unfiltered: a name collision
		// with a live sibling takes that sibling's bucket with it.
		return nil
	}
	siblings := make([]helpers.SiblingVector, 0, len(class.VectorConfig)+1)
	if exclude != "" && modelsext.ClassHasLegacyVectorIndex(class) {
		siblings = append(siblings, helpers.SiblingVector{
			Quantized: canOwnCompressedBucket(class.VectorIndexType, class.VectorIndexConfig),
		})
	}
	for name, cfg := range class.VectorConfig {
		if name != exclude {
			siblings = append(siblings, helpers.SiblingVector{
				Name:      name,
				Quantized: canOwnCompressedBucket(cfg.VectorIndexType, cfg.VectorIndexConfig),
			})
		}
	}
	return siblings
}

// canOwnCompressedBucket reports whether an index with this configuration can
// have written a compressed bucket: hnsw, flat and dynamic only with a
// quantizer enabled (compression cannot be switched off again, so a config
// without one means the bucket was never written), hfresh always, through its
// centroid graph. A configuration this cannot read is treated as quantized:
// leaking beats deleting.
func canOwnCompressedBucket(indexType string, cfg interface{}) bool {
	switch indexType {
	case vectorindex.VectorIndexTypeHNSW:
		if uc, ok := cfg.(hnswent.UserConfig); ok {
			return hnswQuantized(uc)
		}
	case vectorindex.VectorIndexTypeFLAT:
		if uc, ok := cfg.(flatent.UserConfig); ok {
			return flatQuantized(uc)
		}
	case vectorindex.VectorIndexTypeDYNAMIC:
		if uc, ok := cfg.(dynamicent.UserConfig); ok {
			return flatQuantized(uc.FlatUC) || hnswQuantized(uc.HnswUC)
		}
	}
	return true
}

func hnswQuantized(uc hnswent.UserConfig) bool {
	return uc.PQ.Enabled || uc.BQ.Enabled || uc.SQ.Enabled || uc.RQ.Enabled
}

func flatQuantized(uc flatent.UserConfig) bool {
	return uc.PQ.Enabled || uc.BQ.Enabled || uc.SQ.Enabled || uc.RQ.Enabled
}
