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
	"encoding/binary"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
)

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
	var readFn RoaringSetReadFn = func(k []byte, docIDs *sroar.Bitmap) (bool, error) {
		if isEmpty {
			out.docIDs = docIDs
			isEmpty = false
		} else {
			out.docIDs.Or(docIDs)
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReaderRoaringSet(b, pv.value, pv.operator, false)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, errors.Wrap(err, "read row")
	}

	if isEmpty {
		return newDocBitmap(), nil
	}
	return out, nil
}

func (s *Searcher) docBitmapInvertedSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newDocBitmap()
	var readFn ReadFn = func(k []byte, ids [][]byte) (bool, error) {
		for _, asBytes := range ids {
			out.docIDs.Set(binary.LittleEndian.Uint64(asBytes))
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReader(b, pv.value, pv.operator, false)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, errors.Wrap(err, "read row")
	}

	return out, nil
}

func (s *Searcher) docBitmapInvertedMap(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newDocBitmap()
	var readFn ReadFnFrequency = func(k []byte, pairs []lsmkv.MapPair) (bool, error) {
		for _, pair := range pairs {
			// this entry has a frequency, but that's only used for bm25, not for
			// pure filtering, so we can ignore it here
			if s.shardVersion < 2 {
				out.docIDs.Set(binary.LittleEndian.Uint64(pair.Key))
			} else {
				out.docIDs.Set(binary.BigEndian.Uint64(pair.Key))
			}
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReaderFrequency(b, pv.value, pv.operator, false, s.shardVersion)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, errors.Wrap(err, "read row")
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
		return out, errors.Wrapf(err, "geo index range search on prop %q", pv.prop)
	}

	out.docIDs.SetMany(res)
	return out, nil
}
