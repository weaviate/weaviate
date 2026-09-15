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

package storobj

import (
	"context"
	"encoding/binary"
)

// maxDocIDsPerBucketCall caps the doc ids resolved per bucket call, which
// bounds how long one call holds the bucket's consistent view.
const maxDocIDsPerBucketCall = 500

// docIDBatchBucket resolves doc-id secondary keys, several per call, under one
// consistent view.
type docIDBatchBucket interface {
	GetBySecondaryBatch(ctx context.Context, pos int, keys [][]byte, visit func(i int, value []byte) error) error
}

// docIDIterator yields the doc ids to read and how many there are at most.
type docIDIterator interface {
	Next() (uint64, bool)
	Len() int
}

// docIDSlot holds one doc id's decoded result until its round is handed back
// in iteration order.
type docIDSlot[T any] struct {
	docID   uint64
	value   T
	visited bool
	kept    bool
}

// ReadObjectsByDocID reads the objects for the doc ids it yields, a few hundred
// per bucket call, and returns the kept decode results in iteration order, at
// most limit of them (limit <= 0 reads nothing). decode runs concurrently for
// each found object while its bytes are valid, and sequentially after the call
// with nil bytes for each missing one; ok=false skips a doc id uncounted.
// ObjectsByDocIDWithEmpty is not reused: it needs an id slice and decodes all.
func ReadObjectsByDocID[T any](ctx context.Context, bucket docIDBatchBucket, it docIDIterator, limit int,
	decode func(docID uint64, object []byte) (T, bool, error),
) ([]T, error) {
	if limit <= 0 {
		return nil, nil
	}
	perCall := min(limit, it.Len(), maxDocIDsPerBucketCall)
	keyBacking := make([]byte, perCall*8)
	keys := make([][]byte, 0, perCall)
	slots := make([]docIDSlot[T], 0, perCall)
	out := make([]T, 0, min(limit, it.Len()))

	exhausted := false
	for len(out) < limit && !exhausted {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		keys, slots = keys[:0], slots[:0]
		for len(slots) < min(perCall, limit-len(out)) {
			docID, ok := it.Next()
			if !ok {
				exhausted = true
				break
			}
			key := keyBacking[len(slots)*8 : (len(slots)+1)*8]
			binary.LittleEndian.PutUint64(key, docID)
			keys = append(keys, key)
			slots = append(slots, docIDSlot[T]{docID: docID})
		}
		if len(keys) == 0 {
			break
		}

		err := bucket.GetBySecondaryBatch(ctx, 0, keys, func(i int, object []byte) error {
			value, ok, err := decode(slots[i].docID, object)
			if err != nil {
				return err
			}
			slots[i].value, slots[i].kept, slots[i].visited = value, ok, true
			return nil
		})
		if err != nil {
			return nil, err
		}

		for i := range slots {
			if !slots[i].visited {
				value, ok, err := decode(slots[i].docID, nil)
				if err != nil {
					return nil, err
				}
				slots[i].value, slots[i].kept = value, ok
			}
			if slots[i].kept {
				out = append(out, slots[i].value)
			}
		}
	}
	return out, nil
}
