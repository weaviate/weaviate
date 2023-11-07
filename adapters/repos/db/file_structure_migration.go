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
	"regexp"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const (
	flatFSIndexFileRegex = `[_0-9A-Za-z]*_[A-Za-z0-9\-\_]{1,64}`
	vectorIndexCommitLog = `hnsw.commitlog.d`
)

var validateIndexFileRegex *regexp.Regexp

func init() {
	validateIndexFileRegex = regexp.MustCompile(flatFSIndexFileRegex)
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
			if err = os.Rename(part.oldAbsPath, newPath); err != nil {
				return fmt.Errorf("mv %s %s: %w", part.oldAbsPath, newPath, err)
			}
		}
	}

	err = db.migratePQVectors(plan)
	if err != nil {
		return fmt.Errorf("migrate pq vectors: %w", err)
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
				if err := os.MkdirAll(root, os.ModePerm); err != nil {
					return nil, fmt.Errorf("mkdir index/shard %s/%s: %w", idx, shard, err)
				}
				plan[root] = append(plan[root],
					migrationPart{
						oldAbsPath: path.Join(db.config.RootPath, entry.Name()),
						newRelPath: makeNewRelPath(entry.Name(), fmt.Sprintf("%s_%s", idx, shard)),
						index:      idx,
						shard:      shard,
					})
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

func (db *DB) migratePQVectors(plan migrationPlan) error {
	for _, parts := range plan {
		oldClassPath := path.Join(
			db.config.RootPath,
			toTitleCase(parts[0].index),
		)
		oldStorePath := path.Join(
			oldClassPath,
			parts[0].shard,
			"compressed_objects",
		)

		exists, err := fileExists(oldStorePath)
		if err != nil {
			return fmt.Errorf("check store exists: %w", err)
		}

		if !exists {
			continue
		}

		newStorePath := path.Join(
			oldClassPath,
			parts[0].shard,
			"pq",
			helpers.CompressedVectorsBucketLSM,
		)
		if err := os.MkdirAll(newStorePath, os.ModePerm); err != nil {
			return fmt.Errorf("make new store: %w", err)
		}

		entries, err := os.ReadDir(oldStorePath)
		if err != nil {
			return fmt.Errorf("read store: %w", err)
		}

		for _, entry := range entries {
			oldPath := path.Join(oldStorePath, entry.Name())
			newPath := path.Join(newStorePath, entry.Name())
			if err = os.Rename(oldPath, newPath); err != nil {
				return fmt.Errorf("rename store: %w", err)
			}
		}

		if err := os.Remove(oldStorePath); err != nil {
			return fmt.Errorf("rm old store: %w", err)
		}

		newClassPath := path.Join(
			db.config.RootPath,
			parts[0].index,
		)
		if err = os.Rename(oldClassPath, newClassPath); err != nil {
			return fmt.Errorf("rename class path")
		}
	}

	return nil
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

func toTitleCase(lower string) string {
	return cases.Title(language.English).String(lower)
}
