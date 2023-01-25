//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"context"
	"encoding/binary"

	"github.com/dgraph-io/sroar"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
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

	// bucket with strategy map serves docIds used to build bitmap
	// and frequencies, which are ignored for filtering
	if pv.hasFrequency {
		return s.docBitmapInvertedMap(ctx, b, limit, pv)
	}

	// bucket with strategy roaring set serves bitmaps directly
	if b.Strategy() == lsmkv.StrategyRoaringSet {
		return s.docBitmapInvertedRoaringSet(ctx, b, limit, pv)
	}

	// bucket with strategy set serves docIds used to build bitmap
	return s.docBitmapInvertedSet(ctx, b, limit, pv)
}

func (s *Searcher) docBitmapInvertedRoaringSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newDocBitmap()
	hashBucket, err := s.getHashBucket(pv)

	if err != nil {
		return out, err
	}

	rr := NewRowReaderRoaringSet(b, pv.value, pv.operator, false)
	var hashes [][]byte
	var readFn RoaringSetReadFn = func(k []byte, docIDs *sroar.Bitmap) (bool, error) {
		out.docIDs.Or(docIDs)

		currHash, err := hashBucket.Get(k)
		if err != nil {
			return false, errors.Wrap(err, "get hash")
		}
		// currHash is only safe to access for the lifetime of the RowReader, once
		// that has finished, a compaction could happen and remove the underlying
		// memory that the slice points to. Since the hashes will be used to merge
		// filters - which happens after the RowReader has completed - this can lead
		// to segfault crashes. Now is the time to safely copy it, creating a new
		// and immutable slice.
		hashes = append(hashes, copyBytes(currHash))

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	if err := rr.Read(ctx, readFn); err != nil {
		return out, errors.Wrap(err, "read row")
	}

	out.checksum = combineChecksums(hashes, pv.operator)
	return out, nil
}

func (s *Searcher) docBitmapInvertedSet(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newDocBitmap()
	hashBucket, err := s.getHashBucket(pv)

	if err != nil {
		return out, err
	}

	rr := NewRowReader(b, pv.value, pv.operator, false)
	var hashes [][]byte
	var readFn ReadFn = func(k []byte, ids [][]byte) (bool, error) {
		for _, asBytes := range ids {
			out.docIDs.Set(binary.LittleEndian.Uint64(asBytes))
		}

		currHash, err := hashBucket.Get(k)
		if err != nil {
			return false, errors.Wrap(err, "get hash")
		}
		// currHash is only safe to access for the lifetime of the RowReader, once
		// that has finished, a compaction could happen and remove the underlying
		// memory that the slice points to. Since the hashes will be used to merge
		// filters - which happens after the RowReader has completed - this can lead
		// to segfault crashes. Now is the time to safely copy it, creating a new
		// and immutable slice.
		hashes = append(hashes, copyBytes(currHash))

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	if err := rr.Read(ctx, readFn); err != nil {
		return out, errors.Wrap(err, "read row")
	}

	out.checksum = combineChecksums(hashes, pv.operator)
	return out, nil
}

func (s *Searcher) docBitmapInvertedMap(ctx context.Context, b *lsmkv.Bucket,
	limit int, pv *propValuePair,
) (docBitmap, error) {
	out := newDocBitmap()
	hashBucket, err := s.getHashBucket(pv)

	if err != nil {
		return out, err
	}

	rr := NewRowReaderFrequency(b, pv.value, pv.operator, false, s.shardVersion)
	var hashes [][]byte
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

		currHash, err := hashBucket.Get(k)
		if err != nil {
			return false, errors.Wrap(err, "get hash")
		}
		// currHash is only safe to access for the lifetime of the RowReader, once
		// that has finished, a compaction could happen and remove the underlying
		// memory that the slice points to. Since the hashes will be used to merge
		// filters - which happens after the RowReader has completed - this can lead
		// to segfault crashes. Now is the time to safely copy it, creating a new
		// and immutable slice.
		hashes = append(hashes, copyBytes(currHash))

		if limit > 0 && out.docIDs.GetCardinality() >= limit {
			return false, nil
		}
		return true, nil
	}

	if err := rr.Read(ctx, readFn); err != nil {
		return out, errors.Wrap(err, "read row")
	}

	out.checksum = combineChecksums(hashes, pv.operator)
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

	// we can not use the checksum in the same fashion as with the inverted
	// index, i.e. it can not prevent a search as the underlying index does not
	// have any understanding of checksums which could prevent such a read.
	// However, there is more use in the checksum: It can also be used in merging
	// searches (e.g. cond1 AND cond2). The merging operation itself is expensive
	// and cachable, therefore there is a lot of value in calculating and
	// returning a checksum - even for geoProps.
	checksum, err := docPointerChecksum(res)
	if err != nil {
		return out, errors.Wrap(err, "calculate checksum")
	}

	out.checksum = checksum
	return out, nil
}

// TODO move to some helper/utils?
func (s *Searcher) getHashBucket(pv *propValuePair) (*lsmkv.Bucket, error) {
	propName := pv.prop
	if pv.operator == filters.OperatorIsNull {
		propName += filters.InternalNullIndex
	}

	hashBucket := s.store.Bucket(helpers.HashBucketFromPropNameLSM(propName))
	if hashBucket == nil {
		return nil, errors.Errorf("no hash bucket for prop '%s' found", propName)
	}
	return hashBucket, nil
}
