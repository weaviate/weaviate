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

	for _, v := range np.Values {
		if !v.HasFilterableIndex {
			continue
		}
		key := nested.ValueKey(v.Path, v.Data)
		positions := nested.OrDocID(v.Positions, docID)
		if err := bucket.RoaringSetAddList(key, positions); err != nil {
			return fmt.Errorf("nested prop %q path %q: add filterable value: %w", np.Name, v.Path, err)
		}
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

	for _, idx := range np.Idx {
		key := nested.IdxKey(idx.Path, idx.Index)
		positions := nested.OrDocID(idx.Positions, docID)
		if err := bucket.RoaringSetAddList(key, positions); err != nil {
			return fmt.Errorf("nested prop %q: add _idx %q[%d]: %w", np.Name, idx.Path, idx.Index, err)
		}
	}

	for _, exists := range np.Exists {
		key := nested.ExistsKey(exists.Path)
		positions := nested.OrDocID(exists.Positions, docID)
		if err := bucket.RoaringSetAddList(key, positions); err != nil {
			return fmt.Errorf("nested prop %q: add _exists %q: %w", np.Name, exists.Path, err)
		}
	}
	return nil
}
