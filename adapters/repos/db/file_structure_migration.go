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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/schema"
)

const vectorIndexCommitLog = `hnsw.commitlog.d`

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

	for newRoot, parts := range plan.partsByShard {
		for _, part := range parts {
			newPath := path.Join(newRoot, part.newRelPath)
			absDir, _ := filepath.Split(newPath)
			if err := os.MkdirAll(absDir, os.ModePerm); err != nil {
				return fmt.Errorf("mkdir %q: %w", absDir, err)
			}
			if err = os.Rename(part.oldAbsPath, newPath); err != nil {
				return fmt.Errorf("mv %s %s: %w", part.oldAbsPath, newPath, err)
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
}

type shardRoot = string

type migrationPlan struct {
	rootPath     string
	partsByShard map[shardRoot][]migrationPart
}

func newMigrationPlan(rootPath string) *migrationPlan {
	return &migrationPlan{rootPath: rootPath, partsByShard: make(map[string][]migrationPart)}
}

func (p *migrationPlan) append(class, shard, oldRootRelPath, newShardRelPath string) {
	shardRoot := path.Join(p.rootPath, strings.ToLower(class), shard)
	p.partsByShard[shardRoot] = append(p.partsByShard[shardRoot], migrationPart{
		oldAbsPath: path.Join(p.rootPath, oldRootRelPath),
		newRelPath: newShardRelPath,
	})
}

func (p *migrationPlan) prepend(class, shard, oldRootRelPath, newShardRelPath string) {
	shardRoot := path.Join(p.rootPath, strings.ToLower(class), shard)
	p.partsByShard[shardRoot] = append([]migrationPart{{
		oldAbsPath: path.Join(p.rootPath, oldRootRelPath),
		newRelPath: newShardRelPath,
	}}, p.partsByShard[shardRoot]...)
}

func (db *DB) assembleFSMigrationPlan(entries []os.DirEntry) (*migrationPlan, error) {
	fm := newFileMatcher(db.schemaGetter, db.config.RootPath)
	plan := newMigrationPlan(db.config.RootPath)

	for _, entry := range entries {
		if ok, cs := fm.isShardLsmDir(entry); ok {
			// make sure lsm dir is moved first, otherwise os.Rename may fail
			// if directory already exists (created by other files/dirs moved before)
			plan.prepend(cs.class, cs.shard,
				entry.Name(),
				"lsm")
		} else if ok, cs, suffix := fm.isShardFile(entry); ok {
			plan.append(cs.class, cs.shard,
				entry.Name(),
				suffix)
		} else if ok, cs := fm.isShardCommitLogDir(entry); ok {
			plan.append(cs.class, cs.shard,
				entry.Name(),
				fmt.Sprintf("main.%s", vectorIndexCommitLog))
		} else if ok, csp := fm.isShardGeoCommitLogDir(entry); ok {
			plan.append(csp.class, csp.shard,
				entry.Name(),
				fmt.Sprintf("geo.%s.%s", csp.geoProp, vectorIndexCommitLog))
		} else if ok, css := fm.isPqDir(entry); ok {
			for _, cs := range css {
				plan.append(cs.class, cs.shard,
					path.Join(strings.ToLower(entry.Name()), cs.shard, "compressed_objects"),
					path.Join("lsm", helpers.VectorsCompressedBucketLSM))
			}

			// explicitly rename Class directory starting with uppercase to lowercase
			// as MkdirAll will not create lowercased dir if uppercased one exists
			oldClassRoot := path.Join(db.config.RootPath, entry.Name())
			newClassRoot := path.Join(db.config.RootPath, strings.ToLower(entry.Name()))
			if err := os.Rename(oldClassRoot, newClassRoot); err != nil {
				return nil, fmt.Errorf(
					"rename pq index dir to avoid collision, old: %q, new: %q, err: %w",
					oldClassRoot, newClassRoot, err)
			}
		}
	}
	return plan, nil
}

type classShard struct {
	class string
	shard string
}

type classShardGeoProp struct {
	class   string
	shard   string
	geoProp string
}

type fileMatcher struct {
	rootPath            string
	shardLsmDirs        map[string]*classShard
	shardFilePrefixes   map[string]*classShard
	shardGeoDirPrefixes map[string]*classShardGeoProp
	classes             map[string][]*classShard
}

func newFileMatcher(schemaGetter schema.SchemaGetter, rootPath string) *fileMatcher {
	shardLsmDirs := make(map[string]*classShard)
	shardFilePrefixes := make(map[string]*classShard)
	shardGeoDirPrefixes := make(map[string]*classShardGeoProp)
	classes := make(map[string][]*classShard)

	sch := schemaGetter.GetSchemaSkipAuth()
	for _, class := range sch.Objects.Classes {
		shards := schemaGetter.CopyShardingState(class.Class).AllLocalPhysicalShards()
		lowercasedClass := strings.ToLower(class.Class)

		var geoProps []string
		for _, prop := range class.Properties {
			if dt, ok := entschema.AsPrimitive(prop.DataType); ok && dt == entschema.DataTypeGeoCoordinates {
				geoProps = append(geoProps, prop.Name)
			}
		}

		classes[class.Class] = make([]*classShard, 0, len(shards))
		for _, shard := range shards {
			cs := &classShard{class: class.Class, shard: shard}
			shardLsmDirs[fmt.Sprintf("%s_%s_lsm", lowercasedClass, shard)] = cs
			shardFilePrefixes[fmt.Sprintf("%s_%s", lowercasedClass, shard)] = cs
			classes[class.Class] = append(classes[class.Class], cs)

			for _, geoProp := range geoProps {
				csp := &classShardGeoProp{class: class.Class, shard: shard, geoProp: geoProp}
				shardGeoDirPrefixes[fmt.Sprintf("%s_%s_%s", lowercasedClass, shard, geoProp)] = csp
			}
		}
	}

	return &fileMatcher{
		rootPath:            rootPath,
		shardLsmDirs:        shardLsmDirs,
		shardFilePrefixes:   shardFilePrefixes,
		shardGeoDirPrefixes: shardGeoDirPrefixes,
		classes:             classes,
	}
}

// Checks if entry is directory with name (class is lowercased):
// class_shard_lsm
func (fm *fileMatcher) isShardLsmDir(entry os.DirEntry) (bool, *classShard) {
	if !entry.IsDir() {
		return false, nil
	}
	if cs, ok := fm.shardLsmDirs[entry.Name()]; ok {
		return true, cs
	}
	return false, nil
}

// Checks if entry is file with name (class is lowercased):
// class_shard.*
// (e.g. class_shard.version, class_shard.indexcount)
func (fm *fileMatcher) isShardFile(entry os.DirEntry) (bool, *classShard, string) {
	if !entry.Type().IsRegular() {
		return false, nil, ""
	}
	parts := strings.SplitN(entry.Name(), ".", 2)
	if len(parts) != 2 {
		return false, nil, ""
	}
	if cs, ok := fm.shardFilePrefixes[parts[0]]; ok {
		return true, cs, parts[1]
	}
	return false, nil, ""
}

// Checks if entry is directory with name (class is lowercased):
// class_shard.hnsw.commitlog.d
func (fm *fileMatcher) isShardCommitLogDir(entry os.DirEntry) (bool, *classShard) {
	if !entry.IsDir() {
		return false, nil
	}
	parts := strings.SplitN(entry.Name(), ".", 2)
	if len(parts) != 2 {
		return false, nil
	}
	if parts[1] != vectorIndexCommitLog {
		return false, nil
	}
	if cs, ok := fm.shardFilePrefixes[parts[0]]; ok {
		return true, cs
	}
	return false, nil
}

// Checks if entry is directory with name (class is lowercased):
// class_shard_prop.hnsw.commitlog.d
func (fm *fileMatcher) isShardGeoCommitLogDir(entry os.DirEntry) (bool, *classShardGeoProp) {
	if !entry.IsDir() {
		return false, nil
	}
	parts := strings.SplitN(entry.Name(), ".", 2)
	if len(parts) != 2 {
		return false, nil
	}
	if parts[1] != vectorIndexCommitLog {
		return false, nil
	}
	if csp, ok := fm.shardGeoDirPrefixes[parts[0]]; ok {
		return true, csp
	}
	return false, nil
}

// Checks if entry is directory containing PQ index:
// Class/shard/compressed_object
func (fm *fileMatcher) isPqDir(entry os.DirEntry) (bool, []*classShard) {
	if !entry.IsDir() {
		return false, nil
	}

	resultcss := []*classShard{}
	if css, ok := fm.classes[entry.Name()]; ok {
		for _, cs := range css {
			pqDir := path.Join(fm.rootPath, cs.class, cs.shard, "compressed_objects")
			if info, err := os.Stat(pqDir); err == nil && info.IsDir() {
				resultcss = append(resultcss, cs)
			}
		}
		return true, resultcss
	}
	return false, nil
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
