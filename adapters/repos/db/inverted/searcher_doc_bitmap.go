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

package inverted

import (
	"context"
	"fmt"
	"sync"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/entities/filters"
)

// MaxIDBuffer is the amount of bits greater than <maxDocID> to reduce
// the amount of times BitmapFactory has to reallocate
const MaxIDBuffer = uint64(1000)

// BitmapFactory exists to prevent an expensive call to
// NewBitmapPrefill each time NewInvertedBitmap is invoked
type BitmapFactory struct {
	bitmap       *sroar.Bitmap
	maxIDGetter  MaxIDGetterFunc
	currentMaxID uint64
	lock         sync.RWMutex
}

func NewBitmapFactory(maxIDGetter MaxIDGetterFunc) *BitmapFactory {
	maxID := maxIDGetter() + MaxIDBuffer
	return &BitmapFactory{
		bitmap:       roaringset.NewBitmapPrefill(maxID),
		maxIDGetter:  maxIDGetter,
		currentMaxID: maxID,
	}
}

// GetBitmap returns a prefilled bitmap, which is cloned from a shared internal.
// This method is safe to call concurrently. The purpose behind sharing an
// internal bitmap, is that a Clone() operation is much cheaper than prefilling
// a map up to <maxDocID> elements is an expensive operation, and this way we
// only have to do it once.
func (bmf *BitmapFactory) GetBitmap() *sroar.Bitmap {
	bmf.lock.RLock()
	maxID := bmf.maxIDGetter()

	// We don't need to expand, maxID is unchanged
	{
		if maxID <= bmf.currentMaxID {
			cloned := bmf.bitmap.Clone()
			bmf.lock.RUnlock()
			return cloned
		}
	}

	bmf.lock.RUnlock()
	bmf.lock.Lock()
	defer bmf.lock.Unlock()

	// 2nd check to ensure bitmap wasn't expanded by
	// concurrent request white waiting for write lock
	{
		maxID = bmf.maxIDGetter()
		if maxID <= bmf.currentMaxID {
			return bmf.bitmap.Clone()
		}
	}

	// MaxID has grown to exceed even the buffer,
	// time to expand
	{
		length := maxID + MaxIDBuffer - bmf.currentMaxID
		list := make([]uint64, length)
		for i := uint64(0); i < length; i++ {
			list[i] = bmf.currentMaxID + i + 1
		}

		bmf.bitmap.Or(sroar.FromSortedList(list))
		bmf.currentMaxID = maxID + MaxIDBuffer
	}

	return bmf.bitmap.Clone()
}

func (s *Searcher) docBitmap(ctx context.Context, b *lsmkv.Bucket, limit int,
	pv *propValuePair,
) (docBitmap, error) {
	// geo props cannot be served by the inverted index and they require an
	// external index. So, instead of trying to serve this chunk of the filter
	// request internally, we can pass it to an external geo index
	if pv.operator == filters.OperatorWithinGeoRange {
		return s.docBitmapGeo(ctx, pv)
	}
	// all other operators perform operations on the inverted index which we
	// can serve directly

	if pv.hasFilterableIndex {
		// bucket with strategy roaring set serves bitmaps directly
		if b.Strategy() == lsmkv.StrategyRoaringSet {
			return s.docBitmapInvertedRoaringSet(ctx, b, limit, pv)
		}

		// bucket with strategy set serves docIds used to build bitmap
		return s.docBitmapInvertedSet(ctx, b, limit, pv)
	}

	if pv.hasSearchableIndex {
		// bucket with strategy map serves docIds used to build bitmap
		// and frequencies, which are ignored for filtering
		return s.docBitmapInvertedMap(ctx, b, limit, pv)
	}

	return docBitmap{}, fmt.Errorf("property '%s' is neither filterable nor searchable", pv.prop)
}

func (s *Searcher) docBitmapInvertedRoaringSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	var readFn ReadFn = func(k []byte, docIDs *sroar.Bitmap) (bool, error) {
		if isEmpty {
			out.docIDs = docIDs
			isEmpty = false
		} else {
			out.docIDs.Or(docIDs)
		}

		// NotEqual requires the full set of potentially existing doc ids
		if pv.operator == filters.OperatorNotEqual {
			return true, nil
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReaderRoaringSet(b, pv.value, pv.operator, false, s.bitmapFactory)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	if isEmpty {
		return newDocBitmap(), nil
	}
	return out, nil
}

func (s *Searcher) docBitmapInvertedSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	var readFn ReadFn = func(k []byte, ids *sroar.Bitmap) (bool, error) {
		if isEmpty {
			out.docIDs = ids
			isEmpty = false
		} else {
			out.docIDs.Or(ids)
		}

		// NotEqual requires the full set of potentially existing doc ids
		if pv.operator == filters.OperatorNotEqual {
			return true, nil
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReader(b, pv.value, pv.operator, false, s.bitmapFactory)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	if isEmpty {
		return newDocBitmap(), nil
	}
	return out, nil
}

func (s *Searcher) docBitmapInvertedMap(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	var readFn ReadFn = func(k []byte, ids *sroar.Bitmap) (bool, error) {
		if isEmpty {
			out.docIDs = ids
			isEmpty = false
		} else {
			out.docIDs.Or(ids)
		}

		// NotEqual requires the full set of potentially existing doc ids
		if pv.operator == filters.OperatorNotEqual {
			return true, nil
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReaderFrequency(b, pv.value, pv.operator, false, s.shardVersion, s.bitmapFactory)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	if isEmpty {
		return newDocBitmap(), nil
	}
	return out, nil
}

func (s *Searcher) docBitmapGeo(ctx context.Context, pv *propValuePair) (docBitmap, error) {
	out := newDocBitmap()
	propIndex, ok := s.propIndices.ByProp(pv.prop)

	if !ok {
		return out, nil
	}

	res, err := propIndex.GeoIndex.WithinRange(ctx, *pv.valueGeoRange)
	if err != nil {
		return out, fmt.Errorf("geo index range search on prop %q: %w", pv.prop, err)
	}

	out.docIDs.SetMany(res)
	return out, nil
}
