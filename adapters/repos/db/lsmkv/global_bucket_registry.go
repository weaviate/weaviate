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

package lsmkv

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

type globalBucketRegistry struct {
	buckets map[string]struct{}
	mu      sync.Mutex
}

func newGlobalBucketRegistry() *globalBucketRegistry {
	return &globalBucketRegistry{
		buckets: make(map[string]struct{}),
	}
}

var GlobalBucketRegistry *globalBucketRegistry

func init() {
	GlobalBucketRegistry = newGlobalBucketRegistry()
}

var ErrBucketAlreadyRegistered = errors.New("bucket already registered")

func (r *globalBucketRegistry) TryAdd(absoluteBucketPath string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.buckets[absoluteBucketPath]; ok {
		return fmt.Errorf("bucket %q: %w", absoluteBucketPath, ErrBucketAlreadyRegistered)
	}

	r.buckets[absoluteBucketPath] = struct{}{}
	return nil
}

func (r *globalBucketRegistry) Remove(absoluteBucketPath string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.buckets, absoluteBucketPath)
}

// RemoveByPrefix deletes every registered path under dir: dir itself and any
// path that begins with dir + separator.
//
// The separator boundary is exact: RemoveByPrefix(".../t1/lsm") purges
// ".../t1/lsm" and ".../t1/lsm/property__id" but never a sibling like
// ".../t10/lsm", whose registration belongs to a different, possibly live shard.
func (r *globalBucketRegistry) RemoveByPrefix(dir string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removeByPrefixesLocked([]string{dir})
}

// RemoveByPrefixes is RemoveByPrefix batched: it deletes every registered path
// under any dir in dirs in a single locked scan.
func (r *globalBucketRegistry) RemoveByPrefixes(dirs ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removeByPrefixesLocked(dirs)
}

func (r *globalBucketRegistry) removeByPrefixesLocked(dirs []string) {
	if len(dirs) == 0 {
		return
	}

	prefixes := make([]string, len(dirs))
	for i, dir := range dirs {
		prefixes[i] = dir + string(filepath.Separator)
	}

	for key := range r.buckets {
		for i, dir := range dirs {
			if key == dir || strings.HasPrefix(key, prefixes[i]) {
				delete(r.buckets, key)
				break
			}
		}
	}
}
