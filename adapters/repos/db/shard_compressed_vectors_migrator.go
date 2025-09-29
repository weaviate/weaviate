//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

type compressedVectorsMigrator struct {
}

func newMigrator() compressedVectorsMigrator {
	return compressedVectorsMigrator{}
}

func (migrator compressedVectorsMigrator) do(s *Shard) error {
	totalVectors := len(s.index.vectorIndexUserConfigs)
	if s.index.vectorIndexUserConfig != nil {
		totalVectors++
	}

	oldBucketName := helpers.VectorsCompressedBucketLSM
	lsmDir := s.store.GetDir()
	oldBucketPath := filepath.Join(lsmDir, oldBucketName)
	if _, err := os.Stat(oldBucketPath); !os.IsNotExist(err) {
		switch totalVectors {
		case 0:
			// do nothing
		case 1:
			if len(s.index.vectorIndexUserConfigs) > 0 {
				// rename
				for key := range s.index.vectorIndexUserConfigs {
					newBucketName := helpers.GetCompressedBucketName(key)

					// Skip if bucket names are the same (shouldn't happen with multiple configs)
					if oldBucketName == newBucketName {
						continue
					}

					newBucketPath := filepath.Join(lsmDir, newBucketName)

					// Check if new bucket directory already exists
					if _, err := os.Stat(newBucketPath); err == nil {
						os.RemoveAll(newBucketPath)
					}
					// Rename old bucket to new bucket
					if err := os.Rename(oldBucketPath, newBucketPath); err != nil {
						return errors.Wrapf(err, "failed to rename old bucket from %s to %s", oldBucketPath, newBucketPath)
					}
				}
			}
		default:
			// copy
			for key := range s.index.vectorIndexUserConfigs {
				newBucketName := helpers.GetCompressedBucketName(key)

				// Skip if bucket names are the same (shouldn't happen with multiple configs)
				if oldBucketName == newBucketName {
					continue
				}

				newBucketPath := filepath.Join(lsmDir, newBucketName)

				// Check if new bucket directory already exists
				if _, err := os.Stat(newBucketPath); err == nil {
					os.RemoveAll(newBucketPath)
				}

				// Copy old bucket to new target vector bucket
				if err := migrator.copyBucketContents(oldBucketPath, newBucketPath); err != nil {
					return errors.Wrap(err, "failed to copy from old compressed vector bucket to new bucket: ")
				}
			}

			// Remove the old bucket directory after all copies are complete
			if err := os.RemoveAll(oldBucketPath); err != nil {
				return errors.Wrap(err, "failed to remove old bucket directory after copying to all target vectors")
			}
		}
	}
	return nil
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

func (migrator compressedVectorsMigrator) copyFile(src, dst string) error {
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
