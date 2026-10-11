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
	"os"
	"path/filepath"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/indexcounter"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

var (
	errNoPersistedHashtree       = errors.New("no persisted hashtree")
	errPersistedHashtreeOrphaned = errors.New("persisted hashtree has no object store behind it")
)

// persistedHashtreeHasObjectStore refuses to trust a .ht when doc ids were allocated but lsm/objects holds no segment or WAL.
func persistedHashtreeHasObjectStore(indexPath, shardName string) error {
	count, err := indexcounter.Read(shardPath(indexPath, shardName))
	if err != nil {
		return fmt.Errorf("read index counter: %w", err)
	}
	if count == 0 {
		return nil
	}
	entries, err := os.ReadDir(filepath.Join(shardPathLSM(indexPath, shardName), helpers.ObjectsBucketLSM))
	if err != nil {
		return fmt.Errorf("%w: %w", errPersistedHashtreeOrphaned, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		switch filepath.Ext(entry.Name()) {
		case ".db", ".wal":
			return nil
		}
	}
	return fmt.Errorf("%w: %d doc ids allocated, no segment or WAL", errPersistedHashtreeOrphaned, count)
}

// newestPersistedHashTreeRoot is read-only and, like the loader, never falls back to an older .ht.
func newestPersistedHashTreeRoot(dir string) (root hashtree.Digest, filename string, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return hashtree.Digest{}, "", errNoPersistedHashtree
		}
		return hashtree.Digest{}, "", err
	}

	// Names embed a big-endian nanosecond timestamp, so the last .ht entry is the newest.
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".ht" {
			continue
		}
		filename = filepath.Join(dir, entry.Name())

		f, err := os.Open(filename)
		if err != nil {
			return hashtree.Digest{}, "", fmt.Errorf("open hashtree file %q: %w", filename, err)
		}
		root, _, err = hashtree.ReadHashTreeRoot(f)
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		if err != nil {
			return hashtree.Digest{}, "", fmt.Errorf("read hashtree file %q: %w", filename, err)
		}
		return root, filename, nil
	}

	return hashtree.Digest{}, "", errNoPersistedHashtree
}
