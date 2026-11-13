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
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const migrationNamedVectorsQuantizationIssuePerformedFlag = "migration.nvqi.performed.flag"

type compressedVectorsMigrator struct {
	logger logrus.FieldLogger
}

func newCompressedVectorsMigrator(logger logrus.FieldLogger) compressedVectorsMigrator {
	return compressedVectorsMigrator{logger: logger.WithField("action", "compressed_vector_migration")}
}

// removeVectorsCompressedLink removes the vectors_compressed link that 1.30.19
// to 1.40 kept to a named vector's compressed bucket for downgrades to 1.30.18:
// a vector named "compressed" would write its raw vectors through it.
func (m compressedVectorsMigrator) removeVectorsCompressedLink(lsmDir string) error {
	path := filepath.Join(lsmDir, helpers.VectorsCompressedBucketLSM)
	fi, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		return nil
	}
	target, err := os.Readlink(path)
	if err != nil {
		return fmt.Errorf("read link %s: %w", path, err)
	}
	err = os.Remove(path)
	if err != nil {
		return fmt.Errorf("remove link %s: %w", path, err)
	}
	m.logger.Infof("removed the %s link to %s", helpers.VectorsCompressedBucketLSM, target)
	return nil
}

// do runs the compressed-vectors layout migration. records say where each
// vector's files live: the vector at "main" owns vectors_compressed, every
// other vector is a target. configs are the vectors' configs, the legacy one
// under the empty name.
func (m compressedVectorsMigrator) do(s *Shard, records map[string]vectorIndexRecord,
	configs map[string]schemaConfig.VectorIndexConfig,
) error {
	lsmDir := s.store.GetDir()
	if m.isMigrationDone(lsmDir) {
		// migration was performed, nothing to do
		return nil
	}

	owner, hasOwner := "", false
	targets := make(map[string]vectorIndexRecord, len(records))
	for name, rec := range records {
		if rec.storageID() == vectorIndexID("") {
			owner, hasOwner = name, true
			continue
		}
		targets[name] = rec
	}

	vectorsCompressedPath := filepath.Join(lsmDir, helpers.VectorsCompressedBucketLSM)
	if _, err := os.Stat(vectorsCompressedPath); !os.IsNotExist(err) {
		// Only a vector with an index of its own can have written to the shared
		// bucket; a newcomer must not start from another vector's codes. An
		// hfresh owner's bucket holds its centroids' codes, never a target's.
		var err error
		targets, err = m.targetsWithStorage(s, targets)
		if err != nil {
			return err
		}
		if hasOwner && records[owner].IndexType == vectorindex.VectorIndexTypeHFresh {
			targets = nil
		}
		switch {
		case len(records) == 0:
			// do nothing
		case len(targets) == 0:
			// nothing to move: the bucket stays with its owner, or unattributed
			if err := m.markMigrationDone(lsmDir); err != nil {
				return fmt.Errorf("failed to mark migration as done: %w", err)
			}
		case !hasOwner && len(targets) == 1:
			for name, rec := range targets {
				// rename old bucket to new target vector bucket
				if err := m.migrate(name, rec, configs[name], lsmDir, true); err != nil {
					return fmt.Errorf("failed to rename old compressed vector bucket for target vector %s: %w", name, err)
				}
			}
			if err := m.markMigrationDone(lsmDir); err != nil {
				return fmt.Errorf("failed to mark migration as done: %w", err)
			}
		default:
			// copy old buckets to new target vector buckets
			for name, rec := range targets {
				if err := m.migrate(name, rec, configs[name], lsmDir, false); err != nil {
					return fmt.Errorf("failed to copy from old compressed vector bucket to new bucket for target vector: %s: %w", name, err)
				}
			}
			if !hasOwner {
				// remove the old bucket directory after all copies are complete, only if no vector owns it
				if err := os.RemoveAll(vectorsCompressedPath); err != nil {
					return fmt.Errorf("failed to remove old bucket directory after copying all target vectors: %w", err)
				}
				m.logger.Info("removed old vectors compressed bucket")
			} else {
				// its owner keeps the bucket, we need to mark that the migration was performed
				if err := m.markMigrationDone(lsmDir); err != nil {
					return fmt.Errorf("failed to mark migration as done: %w", err)
				}
			}
		}
	} else if hasOwner && m.isQuantizationEnabled(configs[owner]) {
		// a new legacy vector config was created, quantization is enabled but we didn't create the vectors_compressed
		// folder yet but we need to mark that the migration was done in order for it to not be trigered on healthy vector indexes
		if err := m.markMigrationDone(lsmDir); err != nil {
			return fmt.Errorf("failed to mark migration as done: %w", err)
		}
	}
	return nil
}

// targetsWithStorage keeps the targets whose index already has its files.
func (m compressedVectorsMigrator) targetsWithStorage(s *Shard, targets map[string]vectorIndexRecord,
) (map[string]vectorIndexRecord, error) {
	kept := make(map[string]vectorIndexRecord, len(targets))
	for name, rec := range targets {
		dirs, err := s.vectorIndexStorageDirsFor(rec)
		if err != nil {
			return nil, fmt.Errorf("vector %q: %w", name, err)
		}
		exists, err := vectorIndexStorageExists(dirs)
		if err != nil {
			return nil, fmt.Errorf("vector %q: %w", name, err)
		}
		if !exists {
			m.logger.Infof("skipping migration, target vector %s has no index of its own yet", name)
			continue
		}
		kept[name] = rec
	}
	return kept, nil
}

func (m compressedVectorsMigrator) migrate(targetVector string, rec vectorIndexRecord,
	vectorIndexConfig schemaConfig.VectorIndexConfig,
	lsmDir string,
	renameBucket bool,
) error {
	if !m.isQuantizationEnabled(vectorIndexConfig) {
		m.logger.Infof("skipping migration, quantization not enabled for target vector: %s", targetVector)
		return nil
	}

	targetVectorBucket := helpers.CompressedBucketNameForID(rec.storageID())
	targetVectorBucketPath := filepath.Join(lsmDir, targetVectorBucket)

	// Skip if the proper bucket name already exists
	if _, err := os.Stat(targetVectorBucketPath); !renameBucket && err == nil {
		m.logger.Infof("skipping migration, proper bucket exists for target vector: %s", targetVector)
		return nil
	}

	vectorsCompressedPath := filepath.Join(lsmDir, helpers.VectorsCompressedBucketLSM)

	if renameBucket {
		// We might have restored files from a backup, we need to redo the migration
		if _, err := os.Stat(targetVectorBucketPath); !os.IsNotExist(err) {
			m.logger.Infof("restored data from backup, we need to recreate the vectors compressed folder for target vector: %s", targetVector)
			if err := os.RemoveAll(targetVectorBucketPath); err != nil {
				return err
			}
		}
		// Rename old bucket to new bucket
		if err := os.Rename(vectorsCompressedPath, targetVectorBucketPath); err != nil {
			return err
		}
		m.logger.Infof("renamed old vectors compressed bucket for target vector: %s", targetVector)
		return nil
	} else {
		// Define temporary target vector bucket name
		targetVectorBucketPathTmp := fmt.Sprintf("%s_tmp", targetVectorBucketPath)
		// Check if temporary target vector bucket folder exists
		if _, err := os.Stat(targetVectorBucketPathTmp); !os.IsNotExist(err) {
			if err := os.RemoveAll(targetVectorBucketPathTmp); err != nil {
				return err
			}
		}
		// Copy old bucket to temporary target vector bucket
		if err := m.copyBucketContents(vectorsCompressedPath, targetVectorBucketPathTmp); err != nil {
			return err
		}
		// Rename temporary target vector bucket to target vector bucket
		if err := os.Rename(targetVectorBucketPathTmp, targetVectorBucketPath); err != nil {
			return err
		}
		m.logger.Infof("copied old vectors compressed bucket for target vector: %s", targetVector)
		return nil
	}
}

func (m compressedVectorsMigrator) isQuantizationEnabled(vectorConfig schemaConfig.VectorIndexConfig) bool {
	switch vc := vectorConfig.(type) {
	case hnsw.UserConfig:
		return vc.BQ.Enabled || vc.PQ.Enabled || vc.SQ.Enabled || vc.RQ.Enabled
	case flat.UserConfig:
		return vc.BQ.Enabled || vc.PQ.Enabled || vc.SQ.Enabled
	case dynamic.UserConfig:
		flatCompressionEnabled := vc.FlatUC.BQ.Enabled || vc.FlatUC.PQ.Enabled || vc.FlatUC.SQ.Enabled
		hnswCompressionEnabled := vc.HnswUC.BQ.Enabled || vc.HnswUC.PQ.Enabled || vc.HnswUC.SQ.Enabled || vc.HnswUC.RQ.Enabled
		return flatCompressionEnabled || hnswCompressionEnabled
	default:
		return false
	}
}

func (m compressedVectorsMigrator) copyBucketContents(srcDir, dstDir string) error {
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Calculate destination path
		relPath, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dstDir, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		// Copy file
		return m.copyFile(path, dstPath)
	})
}

func (m compressedVectorsMigrator) copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

func (m compressedVectorsMigrator) migrationPerformedFlagFile(lsmDir string) string {
	return filepath.Join(m.migrationDirectory(lsmDir), migrationNamedVectorsQuantizationIssuePerformedFlag)
}

func (m compressedVectorsMigrator) migrationDirectory(lsmDir string) string {
	return filepath.Join(lsmDir, ".migrations")
}

func (m compressedVectorsMigrator) markMigrationDone(lsmDir string) error {
	if _, err := os.Stat(m.migrationDirectory(lsmDir)); os.IsNotExist(err) {
		if err := os.Mkdir(m.migrationDirectory(lsmDir), os.FileMode(0o755)); err != nil {
			return err
		}
	}
	file, err := os.Create(m.migrationPerformedFlagFile(lsmDir))
	if err != nil {
		return err
	}
	defer file.Close()
	m.logger.Info("migration performed successfully")
	return nil
}

func (m compressedVectorsMigrator) isMigrationDone(lsmDir string) bool {
	_, err := os.Stat(m.migrationPerformedFlagFile(lsmDir))
	return err == nil
}
