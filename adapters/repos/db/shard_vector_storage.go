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
	"io/fs"
	"os"
	"path/filepath"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex"
)

// vectorIndexStorageDirs lists the directories an index of indexType at
// physicalID occupies under shardDir. The startup probe and the durability
// sync both use it, so they cannot drift. A dynamic index is in its flat
// bucket until its verdict says it upgraded.
func vectorIndexStorageDirs(shardDir string, state dynamic.StateOps, indexType, physicalID string) ([]string, error) {
	hnswDir := filepath.Join(shardDir, helpers.HNSWCommitLogDirNameForID(physicalID))
	flatDir := filepath.Join(shardDir, "lsm", helpers.VectorsBucketNameForID(physicalID))

	switch indexType {
	case vectorindex.VectorIndexTypeHNSW:
		return []string{hnswDir}, nil
	case vectorindex.VectorIndexTypeFLAT:
		return []string{flatDir}, nil
	case vectorindex.VectorIndexTypeHFresh:
		return []string{filepath.Join(shardDir, helpers.HFreshDirName(physicalID))}, nil
	case vectorindex.VectorIndexTypeDYNAMIC:
		upgraded, err := dynamic.UpgradedInState(state, shardDir, physicalID)
		if err != nil {
			return nil, fmt.Errorf("dynamic index %q: %w", physicalID, err)
		}
		if upgraded {
			return []string{hnswDir}, nil
		}
		return []string{flatDir}, nil
	default:
		return nil, fmt.Errorf("unknown vector index type %q", indexType)
	}
}

// vectorIndexStorageExists reports whether every dir exists; a file in a
// dir's place is an error.
func vectorIndexStorageExists(dirs []string) (bool, error) {
	for _, dir := range dirs {
		info, err := os.Stat(dir)
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if !info.IsDir() {
			return false, fmt.Errorf("%s is not a directory", dir)
		}
	}
	return true, nil
}

// syncVectorIndexStorage fsyncs every dir and its parent: the constructors
// MkdirAll without a sync, and a record must not outlive the entries it
// points at.
func syncVectorIndexStorage(dirs []string) error {
	synced := map[string]struct{}{}
	for _, dir := range dirs {
		for _, path := range []string{dir, filepath.Dir(dir)} {
			if _, done := synced[path]; done {
				continue
			}
			err := fsyncDir(path)
			if err != nil {
				return fmt.Errorf("sync directory %s: %w", path, err)
			}
			synced[path] = struct{}{}
		}
	}
	return nil
}
