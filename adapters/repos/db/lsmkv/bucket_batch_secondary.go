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
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

// BatchGetBySecondary retrieves values for a batch of secondary-index keys in
// a single operation. It is semantically equivalent to calling
// GetBySecondaryWithBuffer for each key individually, but for segments that
// require disk reads (readFromMemory==false) it batches all pread operations
// using io_uring (Linux) or sequential ReadAt (other platforms).
//
// The returned slice has the same length as keys. A nil entry means the key
// was not found or has been deleted.
//
// With secondary tombstones (delete_secondary / .d1 segments), deleted entries
// are caught directly in the secondary index lookup — no primary-key
// validation round is needed.
//
// The bucket's flushLock is held for the full duration of this call (index
// lookups + disk reads) to prevent compaction from closing segment files
// mid-flight.
func (b *Bucket) BatchGetBySecondary(pos int, keys [][]byte) ([][]byte, error) {
	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	if pos >= int(b.secondaryIndices) {
		return nil, fmt.Errorf("no secondary index at pos %d", pos)
	}

	results := make([][]byte, len(keys))

	// --- Phase 1: memtable lookups ---
	// Check the active and flushing memtables for all keys. Keys resolved here
	// skip the disk segment search entirely.
	diskKeys := make([]int, 0, len(keys)) // indices of keys not yet resolved

	for i, key := range keys {
		v, err := b.active.getBySecondary(pos, key)
		if err == nil {
			results[i] = v
			continue
		}
		if errors.Is(err, lsmkv.Deleted) {
			// results[i] stays nil
			continue
		}
		if !errors.Is(err, lsmkv.NotFound) {
			return nil, fmt.Errorf("BatchGetBySecondary active memtable: %w", err)
		}

		if b.flushing != nil {
			v, err = b.flushing.getBySecondary(pos, key)
			if err == nil {
				results[i] = v
				continue
			}
			if errors.Is(err, lsmkv.Deleted) {
				continue
			}
			if !errors.Is(err, lsmkv.NotFound) {
				return nil, fmt.Errorf("BatchGetBySecondary flushing memtable: %w", err)
			}
		}

		diskKeys = append(diskKeys, i)
	}

	if len(diskKeys) == 0 {
		return results, nil
	}

	// --- Phase 2: disk segment index lookups ---
	// Collect positions for all unresolved keys in a single pass through the
	// segment list (one getConsistentViewOfSegments() call for the whole batch).
	diskLookupKeys := make([][]byte, len(diskKeys))
	for j, i := range diskKeys {
		diskLookupKeys[j] = keys[i]
	}

	positions, err := b.disk.batchGetSecondaryNodePos(pos, diskLookupKeys)
	if err != nil {
		return nil, err
	}

	// --- Phase 3: batch disk reads ---
	// For pread-backed positions use batchPread (io_uring on Linux, sequential
	// ReadAt on other platforms). For in-memory positions the data was already
	// copied during the index lookup.
	rawData := make([][]byte, len(diskKeys))

	// Copy in-memory results eagerly; collect pread ops.
	for j, p := range positions {
		if p.deleted {
			// rawData[j] stays nil
			continue
		}
		if p.inMemoryData != nil {
			rawData[j] = p.inMemoryData
		}
		// pread-backed entries are filled by batchPread below.
	}

	if err := batchPread(positions, rawData); err != nil {
		return nil, fmt.Errorf("batch pread: %w", err)
	}

	// --- Phase 4: parse secondary node data ---
	// With secondary tombstones (.d1 segments), the secondary index already
	// handles deletions — no primary-key validation is needed.
	for j, i := range diskKeys {
		data := rawData[j]
		if data == nil {
			continue
		}
		_, value, err := parseReplaceNodeData(data)
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) || errors.Is(err, lsmkv.Deleted) {
				continue
			}
			return nil, errors.Wrapf(err, "parse node data for key index %d", i)
		}
		results[i] = value
	}

	return results, nil
}

// BatchGetBySecondaryWithView is like BatchGetBySecondary but uses a
// pre-acquired BucketConsistentView instead of taking the flushLock itself.
// This is useful when the caller already holds a view (e.g. the HNSW rescore
// path) and wants io_uring batching without double-locking.
//
// The view parameter must be a BucketConsistentView. It is typed as any so
// that the method can satisfy interfaces in packages that cannot import lsmkv.
//
// The caller must keep the view alive (do not call ReleaseView) until this
// method returns.
func (b *Bucket) BatchGetBySecondaryWithView(pos int, keys [][]byte, viewAny any) ([][]byte, error) {
	view, ok := viewAny.(BucketConsistentView)
	if !ok {
		return nil, fmt.Errorf("BatchGetBySecondaryWithView: expected BucketConsistentView, got %T", viewAny)
	}
	if pos >= int(b.secondaryIndices) {
		return nil, fmt.Errorf("no secondary index at pos %d", pos)
	}

	results := make([][]byte, len(keys))

	// --- Phase 1: memtable lookups ---
	diskKeys := make([]int, 0, len(keys))

	for i, key := range keys {
		v, err := view.Active.getBySecondary(pos, key)
		if err == nil {
			results[i] = v
			continue
		}
		if errors.Is(err, lsmkv.Deleted) {
			continue
		}
		if !errors.Is(err, lsmkv.NotFound) {
			return nil, fmt.Errorf("BatchGetBySecondaryWithView active memtable: %w", err)
		}

		if view.Flushing != nil {
			v, err = view.Flushing.getBySecondary(pos, key)
			if err == nil {
				results[i] = v
				continue
			}
			if errors.Is(err, lsmkv.Deleted) {
				continue
			}
			if !errors.Is(err, lsmkv.NotFound) {
				return nil, fmt.Errorf("BatchGetBySecondaryWithView flushing memtable: %w", err)
			}
		}

		diskKeys = append(diskKeys, i)
	}

	if len(diskKeys) == 0 {
		return results, nil
	}

	// --- Phase 2: disk segment index lookups ---
	diskLookupKeys := make([][]byte, len(diskKeys))
	for j, i := range diskKeys {
		diskLookupKeys[j] = keys[i]
	}

	positions, err := b.disk.batchGetSecondaryNodePosWithSegments(pos, diskLookupKeys, view.Disk)
	if err != nil {
		return nil, err
	}

	// --- Phase 3: batch disk reads ---
	rawData := make([][]byte, len(diskKeys))

	for j, p := range positions {
		if p.deleted {
			continue
		}
		if p.inMemoryData != nil {
			rawData[j] = p.inMemoryData
		}
	}

	if err := batchPread(positions, rawData); err != nil {
		return nil, fmt.Errorf("batch pread: %w", err)
	}

	// --- Phase 4: parse secondary node data ---
	for j, i := range diskKeys {
		data := rawData[j]
		if data == nil {
			continue
		}
		_, value, err := parseReplaceNodeData(data)
		if err != nil {
			if errors.Is(err, lsmkv.NotFound) || errors.Is(err, lsmkv.Deleted) {
				continue
			}
			return nil, errors.Wrapf(err, "parse node data for key index %d", i)
		}
		results[i] = value
	}

	return results, nil
}
