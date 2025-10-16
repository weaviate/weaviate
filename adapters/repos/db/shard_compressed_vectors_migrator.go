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

package db

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
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

func (m compressedVectorsMigrator) do(s *Shard) error {
	lsmDir := s.store.GetDir()
	totalVectors := len(s.index.vectorIndexUserConfigs)
	if s.index.vectorIndexUserConfig != nil {
		totalVectors++
	}

	if m.isMigrationDone(lsmDir, totalVectors) {
		// migration was performed, nothing to do
		return nil
	}

	vectorsCompressedPath := filepath.Join(lsmDir, helpers.VectorsCompressedBucketLSM)
	if _, err := os.Stat(vectorsCompressedPath); !os.IsNotExist(err) {
		switch totalVectors {
		case 0:
			// do nothing
		case 1:
			if len(s.index.vectorIndexUserConfigs) > 0 {
				for targetVector, vectorIndexConfig := range s.index.vectorIndexUserConfigs {
					// rename old bucket to new target vector bucket
					if err := m.migrate(targetVector, vectorIndexConfig, lsmDir, true); err != nil {
						return fmt.Errorf("failed to rename old compressed vector bucket for target vector %s: %w", targetVector, err)
					}
				}
			}
			if err := m.markMigrationDone(lsmDir); err != nil {
				return fmt.Errorf("failed to mark migration as done: %w", err)
			}
		default:
			// copy old buckets to new target vector buckets
			for targetVector, vectorIndexConfig := range s.index.vectorIndexUserConfigs {
				if err := m.migrate(targetVector, vectorIndexConfig, lsmDir, false); err != nil {
					return fmt.Errorf("failed to copy from old compressed vector bucket to new bucket for target vector: %s: %w", targetVector, err)
				}
			}
			if s.index.vectorIndexUserConfig == nil {
				// remove the old bucket directory after all copies are complete, only if was not defined for legacy vector
				if err := os.RemoveAll(vectorsCompressedPath); err != nil {
					return fmt.Errorf("failed to remove old bucket directory after copying all target vectors: %w", err)
				}
				m.logger.Info("removed old vectors compressed bucket")
			} else {
				// legacy vector defined together with named vectors, we need to mark that the migration was performed
				if err := m.markMigrationDone(lsmDir); err != nil {
					return fmt.Errorf("failed to mark migration as done: %w", err)
				}
			}
		}
	} else if s.index.vectorIndexUserConfig != nil && m.isQuantizationEnabled(s.index.vectorIndexUserConfig) {
		// a new legacy vector config was created, quantization is enabled but we didn't create the vectors_compressed
		// folder yet but we need to mark that the migration was done in order for it to not be trigered on healthy vector indexes
		if err := m.markMigrationDone(lsmDir); err != nil {
			return fmt.Errorf("failed to mark migration as done: %w", err)
		}
	}
	return nil
}

func (m compressedVectorsMigrator) migrate(targetVector string,
	vectorIndexConfig schemaConfig.VectorIndexConfig,
	lsmDir string,
	renameBucket bool,
) error {
	if !m.isQuantizationEnabled(vectorIndexConfig) {
		m.logger.Infof("skipping migration, quantization not enabled for target vector: %s", targetVector)
		return nil
	}

	targetVectorBucket := helpers.GetCompressedBucketName(targetVector)

	if !renameBucket && targetVectorBucket == helpers.VectorsCompressedBucketLSM {
		m.logger.Info("skipping migration, proper bucket exists for legacy vector")
		return nil
	}

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
		if err := os.Chdir(lsmDir); err != nil {
			return err
		}
		err := os.Symlink(targetVectorBucket, vectorsCompressedPath)
		if err != nil {
			return err
		}
		m.logger.Infof("created symbolic link to old vectors compressed folder for target vector: %s", targetVector)
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

func (m compressedVectorsMigrator) isMigrationDone(lsmDir string, totalVectors int) bool {
	_, err := os.Stat(m.migrationPerformedFlagFile(lsmDir))
	if os.IsNotExist(err) {
		return false
	}
	isMigrationDone := err == nil
	if totalVectors == 1 {
		isVectorsCompressedFolderSymlink := m.isVectorsCompressedFolderSymlink(lsmDir)
		return isMigrationDone && isVectorsCompressedFolderSymlink
	}
	return isMigrationDone
}

func (m compressedVectorsMigrator) isVectorsCompressedFolderSymlink(lsmDir string) bool {
	fileInfo, err := os.Lstat(filepath.Join(lsmDir, helpers.VectorsCompressedBucketLSM))
	if err != nil {
		return false
	}
	return (fileInfo.Mode() & os.ModeSymlink) != 0
}
