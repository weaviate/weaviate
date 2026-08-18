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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
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
			otherTargetVectors(class, name)); err != nil {
			return fmt.Errorf("failed to remove dropped vector index %q files for class %s: %w",
				name, class.Class, err)
		}
	}
	return nil
}

// removeVectorIndexFiles removes every on-disk artifact of a named vector index
// (see helpers.VectorIndexArtifactsFor for the set and why it is centralised).
// otherTargetVectors are the collection's remaining vector names, needed so a
// sibling whose own bucket collides with one of this target's artifact names is
// not deleted along with it.
func (h *vectorDropIndexHelper) removeVectorIndexFiles(
	indexPath, shardName, targetVector string, otherTargetVectors []string,
) error {
	lsmDir := filepath.Join(indexPath, shardName, "lsm")
	shardDir := filepath.Join(indexPath, shardName)

	artifacts := helpers.VectorIndexArtifactsFor(targetVector, otherTargetVectors)

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

	return nil
}

// otherTargetVectors lists the collection's vector names except `exclude` —
// the set whose primary buckets a drop must not touch.
func otherTargetVectors(class *models.Class, exclude string) []string {
	if class == nil {
		// No schema to consult: protect nothing rather than guess. The caller
		// still removes this target's own artifacts; a collision would leak
		// rather than delete a sibling's data.
		return nil
	}
	others := make([]string, 0, len(class.VectorConfig))
	for name := range class.VectorConfig {
		if name != exclude {
			others = append(others, name)
		}
	}
	return others
}
