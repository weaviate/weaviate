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

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

func (w *invertedWrite) nested(nestedProps []inverted.NestedProperty, slots []propSlots, docID uint64, remove bool) error {
	write := (*lsmkv.Bucket).RoaringSetAddBatch
	if remove {
		write = (*lsmkv.Bucket).RoaringSetRemoveBatch
	}
	for i, np := range nestedProps {
		if sl := slots[i].filterable; sl != noSlot {
			if err := write(w.bucket(sl), nestedFilterableEntries(np, docID)); err != nil {
				return fmt.Errorf("nested prop %q: filterable values: %w", np.Name, err)
			}
		}
		if sl := slots[i].nestedMeta; sl != noSlot {
			if err := write(w.bucket(sl), nestedMetaEntries(np, docID)); err != nil {
				return fmt.Errorf("nested prop %q: meta entries: %w", np.Name, err)
			}
		}
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
		// 3-index slice caps cap at len so a downstream append on Key can't
		// clobber the next entry's bytes in the shared slab.
		start := i * nested.IdxKeySize
		end := start + nested.IdxKeySize
		entries = append(entries, lsmkv.RoaringSetBatchEntry{
			Key:    nested.IdxKeyToBuf(idx.Path, idx.Index, keys[start:end:end]),
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
