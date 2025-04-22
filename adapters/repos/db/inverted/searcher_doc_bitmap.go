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
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
)

var noopRelease = func() {}

func (s *Searcher) docBitmap(ctx context.Context, b *lsmkv.Bucket, limit int,
	pv *propValuePair,
) (bm docBitmap, err error) {
	before := time.Now()
	strategy := "geo"
	defer func() {
		took := time.Since(before)
		vals := map[string]any{
			"prop":        pv.prop,
			"operator":    pv.operator,
			"took":        took,
			"took_string": took.String(),
			"value":       pv.value,
			"count":       bm.count(),
			"strategy":    strategy,
		}

		helpers.AnnotateSlowQueryLogAppend(ctx, "build_allow_list_doc_bitmap", vals)
	}()

	// geo props cannot be served by the inverted index and they require an
	// external index. So, instead of trying to serve this chunk of the filter
	// request internally, we can pass it to an external geo index
	if pv.operator == filters.OperatorWithinGeoRange {
		bm, err = s.docBitmapGeo(ctx, pv)
		return
	}
	strategy = b.Strategy()

	// all other operators perform operations on the inverted index which we
	// can serve directly
	switch b.Strategy() {
	case lsmkv.StrategySetCollection:
		bm, err = s.docBitmapInvertedSet(ctx, b, limit, pv)
	case lsmkv.StrategyRoaringSet:
		bm, err = s.docBitmapInvertedRoaringSet(ctx, b, limit, pv)
	case lsmkv.StrategyRoaringSetRange:
		bm, err = s.docBitmapInvertedRoaringSetRange(ctx, b, pv)
	case lsmkv.StrategyMapCollection:
		bm, err = s.docBitmapInvertedMap(ctx, b, limit, pv)
	case lsmkv.StrategyInverted: // TODO amourao, check
		bm, err = s.docBitmapInvertedMap(ctx, b, limit, pv)
	default:
		return docBitmap{}, fmt.Errorf("property '%s' is neither filterable nor searchable nor rangeable", pv.prop)
	}

	return
}

func (s *Searcher) docBitmapInvertedRoaringSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	var readFn ReadFn = func(k []byte, docIDs *sroar.Bitmap, release func()) (bool, error) {
		if isEmpty {
			out.docIDs = docIDs
			out.release = release
			isEmpty = false
		} else {
			out.docIDs.OrConc(docIDs, concurrency.SROAR_MERGE)
			release()
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

func (s *Searcher) docBitmapInvertedRoaringSetRange(ctx context.Context, b *lsmkv.Bucket,
	pv *propValuePair,
) (docBitmap, error) {
	if len(pv.value) != 8 {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: invalid value length %d, should be 8 bytes", len(pv.value))
	}

	reader := b.ReaderRoaringSetRange()
	defer reader.Close()

	docIds, release, err := reader.Read(ctx, binary.BigEndian.Uint64(pv.value), pv.operator)
	if err != nil {
		return newDocBitmap(), fmt.Errorf("readerRoaringSetRange: %w", err)
	}

	out := newUninitializedDocBitmap()
	out.docIDs = docIds
	out.release = release
	return out, nil
}

func (s *Searcher) docBitmapInvertedSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	var readFn ReadFn = func(k []byte, ids *sroar.Bitmap, release func()) (bool, error) {
		if isEmpty {
			out.docIDs = ids
			out.release = release
			isEmpty = false
		} else {
			out.docIDs.OrConc(ids, concurrency.SROAR_MERGE)
			release()
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
	var readFn ReadFn = func(k []byte, ids *sroar.Bitmap, release func()) (bool, error) {
		if isEmpty {
			out.docIDs = ids
			out.release = release
			isEmpty = false
		} else {
			out.docIDs.OrConc(ids, concurrency.SROAR_MERGE)
			release()
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
