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
	if err := bucket.RoaringSetAddBatch(entries); err != nil {
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

	entries := make([]lsmkv.RoaringSetBatchEntry, 0, len(np.Idx)+len(np.Exists))
	for _, idx := range np.Idx {
		entries = append(entries, lsmkv.RoaringSetBatchEntry{
			Key:    nested.IdxKey(idx.Path, idx.Index),
			Values: nested.OrDocID(idx.Positions, docID),
		})
	}
	for _, exists := range np.Exists {
		entries = append(entries, lsmkv.RoaringSetBatchEntry{
			Key:    nested.ExistsKey(exists.Path),
			Values: nested.OrDocID(exists.Positions, docID),
		})
	}
	if err := bucket.RoaringSetAddBatch(entries); err != nil {
		return fmt.Errorf("nested prop %q: add meta entries: %w", np.Name, err)
	}
	return nil
}
