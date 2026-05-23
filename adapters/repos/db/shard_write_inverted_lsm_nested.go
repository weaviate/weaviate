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
		if err := s.deleteNestedFilterableIndex(np, docID); err != nil {
			return err
		}
		// TODO: delete nested searchable index (Phase 2)
		// TODO: delete nested rangeable index (Phase 3)
		if err := s.deleteNestedMetaIndex(np, docID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) deleteNestedFilterableIndex(np inverted.NestedProperty, docID uint64) error {
	if !np.HasFilterableIndex {
		return nil
	}

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
	if len(np.Idx) == 0 && len(np.Exists) == 0 {
		return nil
	}

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
		if err := s.extendNestedFilterableIndex(np, docID); err != nil {
			return err
		}
		// TODO: extend nested searchable index (Phase 2)
		// TODO: extend nested rangeable index (Phase 3)
		if err := s.extendNestedMetaIndex(np, docID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) extendNestedFilterableIndex(np inverted.NestedProperty, docID uint64) error {
	if !np.HasFilterableIndex {
		return nil
	}

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
	if len(np.Idx) == 0 && len(np.Exists) == 0 {
		return nil
	}

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
	entries := make([]lsmkv.RoaringSetBatchEntry, 0, len(np.Values))
	for _, v := range np.Values {
		if !v.HasFilterableIndex {
			continue
		}
		entries = append(entries, lsmkv.RoaringSetBatchEntry{
			Key:    nested.ValueKey(v.Path, v.Data),
			Values: nested.OrDocID(v.Positions, docID),
		})
	}
	return entries
}

func nestedMetaEntries(np inverted.NestedProperty, docID uint64) []lsmkv.RoaringSetBatchEntry {
	entries := make([]lsmkv.RoaringSetBatchEntry, 0, len(np.Idx)+len(np.Exists))
	// Single slab for all _idx keys. Each iteration writes into its own
	// IdxKeySize-byte slice off the slab, so every entry holds a distinct
	// backing array while the function makes one allocation for the key
	// bytes overall (instead of three per IdxKey call).
	var keys []byte
	if n := len(np.Idx); n > 0 {
		keys = make([]byte, n*nested.IdxKeySize)
	}
	for i, idx := range np.Idx {
		buf := keys[i*nested.IdxKeySize : (i+1)*nested.IdxKeySize]
		entries = append(entries, lsmkv.RoaringSetBatchEntry{
			Key:    nested.IdxKeyToBuf(idx.Path, idx.Index, buf),
			Values: nested.OrDocID(idx.Positions, docID),
		})
	}
	for _, exists := range np.Exists {
		entries = append(entries, lsmkv.RoaringSetBatchEntry{
			Key:    nested.ExistsKey(exists.Path),
			Values: nested.OrDocID(exists.Positions, docID),
		})
	}
	return entries
}
