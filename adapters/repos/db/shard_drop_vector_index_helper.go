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
// for named vectors whose index has been dropped (VectorIndexType cleared to "").
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
		if err := h.removeVectorIndexFiles(indexPath, shardName, name); err != nil {
			return fmt.Errorf("failed to remove dropped vector index %q files for class %s: %w",
				name, class.Class, err)
		}
	}
	return nil
}

// removeVectorIndexFiles removes all on-disk artifacts for a named vector index:
// - LSM bucket: vectors_{name}
// - LSM compressed bucket: vectors_compressed_{name}
// - HNSW commit log directory: vectors_{name}.hnsw.commitlog.d
// - HNSW snapshot directory: vectors_{name}.hnsw.snapshot.d
func (h *vectorDropIndexHelper) removeVectorIndexFiles(indexPath, shardName, targetVector string) error {
	lsmDir := filepath.Join(indexPath, shardName, "lsm")
	shardDir := filepath.Join(indexPath, shardName)

	vectorsBucket := helpers.GetVectorsBucketName(targetVector)
	compressedBucket := helpers.GetCompressedBucketName(targetVector)
	hnswCommitLogDir := helpers.GetHNSWCommitLogDirName(targetVector)
	hnswSnapshotDir := helpers.GetHNSWSnapshotDirName(targetVector)

	vectorIndexDirectories := []string{
		filepath.Join(lsmDir, vectorsBucket),
		filepath.Join(lsmDir, compressedBucket),
		filepath.Join(shardDir, hnswCommitLogDir),
		filepath.Join(shardDir, hnswSnapshotDir),
	}

	for _, dir := range vectorIndexDirectories {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove %s: %w", dir, err)
		}
	}

	return nil
}
