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
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

func (s *Shard) deleteNestedInvertedIndicesLSM(nestedProps []inverted.NestedProperty, docID uint64) error {
	for _, np := range nestedProps {
		if np.HasFilterableEntries() {
			if err := s.deleteNestedFilterableIndex(np, docID); err != nil {
				return err
			}
		}
		// HasSearchableEntries() and HasRangeableEntries() return false today;
		// their branches are inert until nested searchable/rangeable bucket types are implemented.
		if np.HasMetaEntries() {
			if err := s.deleteNestedMetaIndex(np, docID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Shard) deleteNestedFilterableIndex(np inverted.NestedProperty, docID uint64) error {
	bucket := s.store.Bucket(helpers.BucketNestedFromPropNameLSM(np.Name))
	if bucket == nil {
		return fmt.Errorf("nested prop %q: no filterable value bucket found", np.Name)
	}
	if err := bucket.RoaringSetRemoveBatch(nestedFilterableEntries(np, docID)); err != nil {
		return fmt.Errorf("nested prop %q: remove filterable values: %w", np.Name, err)
	}
	return nil
}

func (s *Shard) deleteNestedMetaIndex(np inverted.NestedProperty, docID uint64) error {
	bucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(np.Name))
	if bucket == nil {
		return fmt.Errorf("nested prop %q: no meta bucket found", np.Name)
	}
	if err := bucket.RoaringSetRemoveBatch(nestedMetaEntries(np, docID)); err != nil {
		return fmt.Errorf("nested prop %q: remove meta entries: %w", np.Name, err)
	}
	return nil
}

func (s *Shard) extendNestedInvertedIndicesLSM(nestedProps []inverted.NestedProperty, docID uint64) error {
	for _, np := range nestedProps {
		if np.HasFilterableEntries() {
			if err := s.extendNestedFilterableIndex(np, docID); err != nil {
				return err
			}
		}
		// HasSearchableEntries() and HasRangeableEntries() return false today;
		// their branches are inert until nested searchable/rangeable bucket types are implemented.
		if np.HasMetaEntries() {
			if err := s.extendNestedMetaIndex(np, docID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Shard) extendNestedFilterableIndex(np inverted.NestedProperty, docID uint64) error {
	bucket := s.store.Bucket(helpers.BucketNestedFromPropNameLSM(np.Name))
	if bucket == nil {
		return fmt.Errorf("nested prop %q: no filterable value bucket found", np.Name)
	}
	if err := bucket.RoaringSetAddBatch(nestedFilterableEntries(np, docID)); err != nil {
		return fmt.Errorf("nested prop %q: add filterable values: %w", np.Name, err)
	}
	return nil
}

func (s *Shard) extendNestedMetaIndex(np inverted.NestedProperty, docID uint64) error {
	bucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(np.Name))
	if bucket == nil {
		return fmt.Errorf("nested prop %q: no meta bucket found", np.Name)
	}
	if err := bucket.RoaringSetAddBatch(nestedMetaEntries(np, docID)); err != nil {
		return fmt.Errorf("nested prop %q: add meta entries: %w", np.Name, err)
	}
	return nil
}

func nestedFilterableEntries(np inverted.NestedProperty, docID uint64) []lsmkv.RoaringSetBatchEntry {
	entries := make([]lsmkv.RoaringSetBatchEntry, 0, np.NumFilterable())
	for v := range np.Values() {
		if !v.HasFilterableIndex {
			continue
		}
		entries = append(entries, lsmkv.RoaringSetBatchEntry{
			Key:    nested.ValueKey(v.Path, v.Data),
			Values: nested.PositionsWithDocID(docID, v.Positions...),
		})
	}
	return entries
}

func nestedMetaEntries(np inverted.NestedProperty, docID uint64) []lsmkv.RoaringSetBatchEntry {
	entries := make([]lsmkv.RoaringSetBatchEntry, 0, np.NumMetaHint())

	// Single slab for all _idx keys; NumIdx() is exact so the slab is exactly
	// n*IdxKeySize bytes with no over-allocation.
	n := np.NumIdx()
	var slab []byte
	if n > 0 {
		slab = make([]byte, n*nested.IdxKeySize)
	}
	// range-over-func provides no index; track the slab offset manually.
	i := 0
	for idx := range np.Idx() {
		start := i * nested.IdxKeySize
		entries = append(entries, lsmkv.RoaringSetBatchEntry{
			Key:    nested.IdxKeyToBuf(idx.Path, idx.Index, slab[start:start+nested.IdxKeySize:start+nested.IdxKeySize]),
			Values: nested.PositionsWithDocID(docID, idx.Positions...),
		})
		i++
	}
	// np.Exists() and np.Anchors() are iter.Seq iterators; they apply per-leaf
	// gating inline from np.configs; the yield closure is expected to stay
	// on-stack (no allocation) — verified by BenchmarkNestedMetaEntries2_*.
	for e := range np.Exists() {
		entries = append(entries, lsmkv.RoaringSetBatchEntry{
			Key:    nested.ExistsKey(e.Path),
			Values: nested.PositionsWithDocID(docID, e.Positions...),
		})
	}
	for a := range np.Anchors() {
		// AnchorView.Position is a scalar ElemIdx — one self-marker per entry;
		// passed as a single variadic arg (its backing array is expected to stay
		// on-stack, so no extra heap alloc vs the direct-Encode path).
		entries = append(entries, lsmkv.RoaringSetBatchEntry{
			Key:    nested.AnchorKey(a.Path),
			Values: nested.PositionsWithDocID(docID, a.Position),
		})
	}
	return entries
}
