//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

const (
	flatFSIndexFileRegex = `[_0-9A-Za-z]*_[A-Za-z0-9\-\_]{1,64}`
	flatFSPQFileRegex    = `^[A-Z][A-Za-z0-9\_]*`
	vectorIndexCommitLog = `hnsw.commitlog.d`
)

var validateIndexFileRegex *regexp.Regexp
var validatePQFileRegex *regexp.Regexp

func init() {
	validateIndexFileRegex = regexp.MustCompile(flatFSIndexFileRegex)
	validatePQFileRegex = regexp.MustCompile(flatFSPQFileRegex)
}

func (db *DB) migrateFileStructureIfNecessary() error {
	fsMigrationPath := path.Join(db.config.RootPath, "migration1.22.fs.hierarchy")
	exists, err := fileExists(fsMigrationPath)
	if err != nil {
		return err
	}
	if !exists {
		if err = db.migrateToHierarchicalFS(); err != nil {
			return fmt.Errorf("migrate to hierarchical fs: %w", err)
		}
		if _, err = os.Create(fsMigrationPath); err != nil {
			return fmt.Errorf("create hierarchical fs indicator: %w", err)
		}
	}
	return nil
}

func (db *DB) migrateToHierarchicalFS() error {
	before := time.Now()

	root, err := os.ReadDir(db.config.RootPath)
	if err != nil {
		return fmt.Errorf("read db root: %w", err)
	}

	plan, err := db.assembleFSMigrationPlan(root)
	if err != nil {
		return err
	}

	for newRoot, parts := range plan {
		for _, part := range parts {
			newPath := path.Join(newRoot, part.newRelPath)
			absDir, _ := filepath.Split(newPath)
			if err := os.MkdirAll(absDir, os.ModePerm); err != nil {
				return fmt.Errorf("mkdir %q: %w", absDir, err)
			}
			if err = os.Rename(part.oldAbsPath, newPath); err != nil {
				if os.IsExist(err) {
					if err = os.RemoveAll(part.oldAbsPath); err != nil {
						return fmt.Errorf("rm file %q: %w", part.oldAbsPath, err)
					}
				} else {
					return fmt.Errorf("mv %s %s: %w", part.oldAbsPath, newPath, err)
				}
			}
		}
	}

	db.logger.WithField("action", "hierarchical_fs_migration").
		Debugf("fs migration took %s\n", time.Since(before))

	return nil
}

type migrationPart struct {
	oldAbsPath string
	newRelPath string
	index      string
	shard      string
}

type shardRoot = string

type migrationPlan map[shardRoot][]migrationPart

func (db *DB) assembleFSMigrationPlan(entries []os.DirEntry) (migrationPlan, error) {
	plan := make(migrationPlan, len(entries))
	for _, entry := range entries {
		idxFile := validateIndexFileRegex.FindString(entry.Name())
		if idxFile != "" {
			parts := strings.Split(idxFile, "_")
			if len(parts) > 1 {
				idx, shard := parts[0], parts[1]
				root := path.Join(db.config.RootPath, idx, shard)
				plan[root] = append(plan[root],
					migrationPart{
						oldAbsPath: path.Join(db.config.RootPath, entry.Name()),
						newRelPath: makeNewRelPath(entry.Name(), fmt.Sprintf("%s_%s", idx, shard)),
						index:      idx,
						shard:      shard,
					})
			}
		}

		pqFile := validatePQFileRegex.FindString(entry.Name())
		if pqFile != "" && entry.IsDir() {
			oldIndexRoot := path.Join(db.config.RootPath, entry.Name())
			newIndexRoot := path.Join(db.config.RootPath, strings.ToLower(entry.Name()))
			err := os.Rename(oldIndexRoot, newIndexRoot)
			if err != nil {
				return nil, fmt.Errorf(
					"rename pq index dir to avoid collision, old: %q, new: %q, err: %w",
					oldIndexRoot, newIndexRoot, err)
			}
			shards, err := os.ReadDir(newIndexRoot)
			if err != nil {
				return nil, fmt.Errorf("read pq class dir %q: %w", entry.Name(), err)
			}
			for _, shard := range shards {
				if shard.IsDir() {
					root := path.Join(newIndexRoot, shard.Name())
					files, err := os.ReadDir(path.Join(newIndexRoot, shard.Name(), "compressed_objects"))
					if err != nil {
						return nil, fmt.Errorf("read pq shard dir %q: %w", shard.Name(), err)
					}
					for _, f := range files {
						plan[root] = append(plan[root], migrationPart{
							oldAbsPath: path.Join(root, "compressed_objects", f.Name()),
							newRelPath: path.Join("lsm", helpers.VectorsHNSWPQBucketLSM, f.Name()),
						})
					}
				}
			}
		}
	}
	return plan, nil
}

func makeNewRelPath(oldPath, prefix string) (newPath string) {
	newPath = strings.TrimPrefix(oldPath, prefix)[1:]
	if newPath == vectorIndexCommitLog {
		// the primary commitlog for the index
		newPath = fmt.Sprintf("main.%s", newPath)
	} else if strings.Contains(newPath, vectorIndexCommitLog) {
		// commitlog for a geo property
		newPath = fmt.Sprintf("geo.%s", newPath)
	}
	return
}

func fileExists(file string) (bool, error) {
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
