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

// vectorIndexStorageDirs lists the directories a vector index of indexType
// occupies at physicalID under shardDir. The startup probe checks that they
// exist and the durability step syncs them, both from this one list, so the
// two cannot drift apart. It is a presence list, not a validity check: a
// directory that exists but is corrupt fails inside the constructor.
//
// A dynamic index lives in its flat bucket until it upgrades, then in its
// hnsw commit log directory; state is the shard's dynamic namespace, read
// through the open handle. PR5 moves this into the implementations along
// with cleanup.
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

// vectorIndexStorageExists reports whether every directory in dirs exists.
// A path that exists but is not a directory is an error, not an absence.
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
