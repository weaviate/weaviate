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

package inverted

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
)

var noopRelease = func() {}

const docBitmapSlowLogKey = "build_allow_list_doc_bitmap"

func (s *Searcher) docBitmap(ctx context.Context, b *lsmkv.Bucket, limit int,
	pv *propValuePair,
) (bm docBitmap, err error) {
	before := time.Now()
	strategy := b.Strategy()
	defer helpers.AnnotateSlowQueryLogSlowest(ctx, docBitmapSlowLogKey, before, func() map[string]any {
		return map[string]any{
			"prop":     pv.prop,
			"operator": pv.operator,
			"value":    pv.value[:min(len(pv.value), filters.LogMaxStringChars)],
			"count":    bm.count(),
			"failed":   err != nil,
			"strategy": strategy,
		}
	})

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
	case lsmkv.StrategyGeoPoints:
		bm, err = s.docBitmapGeo(ctx, b, pv)
	default:
		return docBitmap{}, fmt.Errorf("property '%s' is neither filterable nor searchable nor rangeable", pv.prop)
	}

	return bm, err
}

func (s *Searcher) docBitmapInvertedRoaringSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newUninitializedDocBitmap()
	isEmpty := true
	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)
	var readFn ReadFn = func(k []byte, docIDs *sroar.Bitmap, release func()) (bool, error) {
		if isEmpty {
			out.docIDs = docIDs
			out.release = release
			isEmpty = false
		} else {
			out.docIDs.OrConc(docIDs, mergeConc)
			release()
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	var rr *RowReaderRoaringSet
	if pv.nested.isNested {
		rr = NewRowReaderRoaringSetWithPrefix(b, pv.value, pv.operator, false, invnested.PathPrefix(pv.nested.relPath))
	} else {
		rr = NewRowReaderRoaringSet(b, pv.value, pv.operator, false)
	}
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	out.isDenyList = rr.isDenyList
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
	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)
	var readFn ReadFn = func(k []byte, ids *sroar.Bitmap, release func()) (bool, error) {
		if isEmpty {
			out.docIDs = ids
			out.release = release
			isEmpty = false
		} else {
			out.docIDs.OrConc(ids, mergeConc)
			release()
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReader(b, pv.value, pv.operator, false)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	out.isDenyList = rr.isDenyList
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
	mergeConc := concurrency.BudgetFromCtxCapped(ctx, concurrency.SROAR_MERGE)
	var readFn ReadFn = func(k []byte, ids *sroar.Bitmap, release func()) (bool, error) {
		if isEmpty {
			out.docIDs = ids
			out.release = release
			isEmpty = false
		} else {
			out.docIDs.OrConc(ids, mergeConc)
			release()
		}

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	rr := NewRowReaderFrequency(b, pv.value, pv.operator, false, s.shardVersion)
	if err := rr.Read(ctx, readFn); err != nil {
		return out, fmt.Errorf("read row: %w", err)
	}

	out.isDenyList = rr.isDenyList
	if isEmpty {
		return newDocBitmap(), nil
	}
	return out, nil
}

// docBitmapGeo serves a withinGeoRange filter from the property's geopoints
// bucket, which answers it exactly.
func (s *Searcher) docBitmapGeo(ctx context.Context, b *lsmkv.Bucket, pv *propValuePair) (docBitmap, error) {
	if pv.operator != filters.OperatorWithinGeoRange || pv.valueGeoRange == nil {
		return docBitmap{}, fmt.Errorf("operator %s is not supported on geo prop %q", pv.operator.Name(), pv.prop)
	}
	if pv.valueGeoRange.GeoCoordinates == nil || pv.valueGeoRange.Latitude == nil || pv.valueGeoRange.Longitude == nil {
		return docBitmap{}, fmt.Errorf("geo range on prop %q: latitude and longitude must be set", pv.prop)
	}

	res, err := b.GeoPointsWithinRange(ctx, *pv.valueGeoRange.Latitude, *pv.valueGeoRange.Longitude,
		pv.valueGeoRange.Distance)
	if err != nil {
		return docBitmap{}, fmt.Errorf("geo index range search on prop %q: %w", pv.prop, err)
	}

	// the bitmap is freshly built, nothing to release
	out := newUninitializedDocBitmap()
	out.docIDs = res
	out.release = func() {}
	return out, nil
}
