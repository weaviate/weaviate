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
	if _, err := os.Stat(fsMigrationPath); err != nil {
		if os.IsNotExist(err) {
			if err = db.migrateToHierarchicalFS(); err != nil {
				return fmt.Errorf("migrate to hierarchical fs: %w", err)
			}
		}
		if _, err := os.Create(fsMigrationPath); err != nil {
			return fmt.Errorf("create hierarchical fs indicator: %w", err)
		}
	}
	return nil
}

func (db *DB) migrateToHierarchicalFS() error {
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

	return nil
}

type migrationPart struct {
	oldAbsPath string
	newRelPath string
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
