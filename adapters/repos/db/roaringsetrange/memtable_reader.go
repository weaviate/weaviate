//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package roaringsetrange

import (
	"context"
	"fmt"

	"github.com/weaviate/sroar"
	"github.com/liutizhong/weaviate/adapters/repos/db/roaringset"
	"github.com/liutizhong/weaviate/entities/filters"
)

type MemtableReader struct {
	memtable *Memtable
}

func NewMemtableReader(memtable *Memtable) *MemtableReader {
	return &MemtableReader{memtable: memtable}
}

func (r *MemtableReader) Read(ctx context.Context, value uint64, operator filters.Operator,
) (roaringset.BitmapLayer, error) {
	if err := ctx.Err(); err != nil {
		return roaringset.BitmapLayer{}, err
	}

	switch operator {
	case filters.OperatorEqual:
		return r.read(func(k uint64) bool { return k == value }), nil

	case filters.OperatorNotEqual:
		return r.read(func(k uint64) bool { return k != value }), nil

	case filters.OperatorLessThan:
		return r.read(func(k uint64) bool { return k < value }), nil

	case filters.OperatorLessThanEqual:
		return r.read(func(k uint64) bool { return k <= value }), nil

	case filters.OperatorGreaterThan:
		return r.read(func(k uint64) bool { return k > value }), nil

	case filters.OperatorGreaterThanEqual:
		return r.read(func(k uint64) bool { return k >= value }), nil

	default:
		return roaringset.BitmapLayer{}, fmt.Errorf("operator %v not supported for segments of strategy %q",
			operator.Name(), "roaringsetrange") // TODO move strategies to separate package?
	}
}

func (r *MemtableReader) read(predicate func(k uint64) bool) roaringset.BitmapLayer {
	additions := sroar.NewBitmap()
	deletions := sroar.NewBitmap()

	for v, k := range r.memtable.additions {
		if predicate(k) {
			additions.Set(v)
		}
		deletions.Set(v)
	}
	for v := range r.memtable.deletions {
		deletions.Set(v)
	}

	return roaringset.BitmapLayer{
		Additions: additions,
		Deletions: deletions,
	}
}
