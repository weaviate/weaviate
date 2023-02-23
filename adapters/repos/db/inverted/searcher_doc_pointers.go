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

// import (
// 	"context"
// 	"encoding/binary"

// 	"github.com/pkg/errors"
// 	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
// 	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
// 	"github.com/weaviate/weaviate/entities/filters"
// )

// func (s *Searcher) docPointers(prop string, b *lsmkv.Bucket, limit int,
// 	pv *propValuePair, tolerateDuplicates bool,
// ) (docPointers, error) {
// 	if pv.operator == filters.OperatorWithinGeoRange {
// 		// geo props cannot be served by the inverted index and they require an
// 		// external index. So, instead of trying to serve this chunk of the filter
// 		// request internally, we can pass it to an external geo index
// 		return s.docPointersGeo(pv)
// 	} else {
// 		// all other operators perform operations on the inverted index which we
// 		// can serve directly
// 		return s.docPointersInverted(prop, b, limit, pv, tolerateDuplicates)
// 	}
// }

// func (s *Searcher) docPointersInverted(prop string, b *lsmkv.Bucket, limit int,
// 	pv *propValuePair, tolerateDuplicates bool,
// ) (docPointers, error) {
// 	if pv.hasFrequency {
// 		return s.docPointersInvertedFrequency(prop, b, limit, pv, tolerateDuplicates)
// 	}

// 	return s.docPointersInvertedNoFrequency(prop, b, limit, pv, tolerateDuplicates)
// }

// func (s *Searcher) docPointersInvertedNoFrequency(prop string, b *lsmkv.Bucket, limit int,
// 	pv *propValuePair, tolerateDuplicates bool,
// ) (docPointers, error) {
// 	rr := NewRowReader(b, pv.value, pv.operator, false)

// 	var pointers docPointers
// 	var hashes [][]byte

// 	if err := rr.Read(context.TODO(), func(k []byte, ids [][]byte) (bool, error) {
// 		currentDocIDs := make([]uint64, len(ids))
// 		for i, asBytes := range ids {
// 			currentDocIDs[i] = binary.LittleEndian.Uint64(asBytes)
// 		}

// 		pointers.count += uint64(len(ids))
// 		pointers.docIDs = append(pointers.docIDs, currentDocIDs...)

// 		propName := pv.prop
// 		if pv.operator == filters.OperatorIsNull {
// 			propName += filters.InternalNullIndex
// 		}

// 		hashBucket := s.store.Bucket(helpers.HashBucketFromPropNameLSM(propName))
// 		if hashBucket == nil {
// 			return false, errors.Errorf("no hash bucket for prop '%s' found", propName)
// 		}

// 		currHash, err := hashBucket.Get(k)
// 		if err != nil {
// 			return false, errors.Wrap(err, "get hash")
// 		}

// 		// currHash is only safe to access for the lifetime of the RowReader, once
// 		// that has finished, a compaction could happen and remove the underlying
// 		// memory that the slice points to. Since the hashes will be used to merge
// 		// filters - which happens after the RowReader has completed - this can lead
// 		// to segfault crashes. Now is the time to safely copy it, creating a new
// 		// and immutable slice.
// 		hashes = append(hashes, copyBytes(currHash))
// 		if limit > 0 && pointers.count >= uint64(limit) {
// 			return false, nil
// 		}

// 		return true, nil
// 	}); err != nil {
// 		return pointers, errors.Wrap(err, "read row")
// 	}

// 	pointers.checksum = combineChecksums(hashes, pv.operator)
// 	if !tolerateDuplicates {
// 		pointers.removeDuplicates()
// 	}

// 	return pointers, nil
// }

// func (s *Searcher) docPointersInvertedFrequency(prop string, b *lsmkv.Bucket, limit int,
// 	pv *propValuePair, tolerateDuplicates bool,
// ) (docPointers, error) {
// 	rr := NewRowReaderFrequency(b, pv.value, pv.operator, false, s.shardVersion)

// 	var pointers docPointers
// 	var hashes [][]byte

// 	if err := rr.Read(context.TODO(), func(k []byte, pairs []lsmkv.MapPair) (bool, error) {
// 		currentDocIDs := make([]uint64, len(pairs))
// 		// beforePairs := time.Now()
// 		for i, pair := range pairs {
// 			// this entry has a frequency, but that's only used for bm25, not for
// 			// pure filtering, so we can ignore it here
// 			if s.shardVersion < 2 {
// 				currentDocIDs[i] = binary.LittleEndian.Uint64(pair.Key)
// 			} else {
// 				currentDocIDs[i] = binary.BigEndian.Uint64(pair.Key)
// 			}
// 		}
// 		// fmt.Printf("loop through pairs took %s\n", time.Since(beforePairs))

// 		pointers.count += uint64(len(pairs))
// 		if len(pointers.docIDs) > 0 {
// 			pointers.docIDs = append(pointers.docIDs, currentDocIDs...)
// 		} else {
// 			pointers.docIDs = currentDocIDs
// 		}

// 		hashBucket := s.store.Bucket(helpers.HashBucketFromPropNameLSM(pv.prop))
// 		if hashBucket == nil {
// 			return false, errors.Errorf("no hash bucket for prop '%s' found", pv.prop)
// 		}

// 		// use retrieved k instead of pv.value - they are typically the same, but
// 		// not on a like operator with wildcard where we only had a partial match
// 		currHash, err := hashBucket.Get(k)
// 		if err != nil {
// 			return false, errors.Wrap(err, "get hash")
// 		}

// 		// currHash is only safe to access for the lifetime of the RowReader, once
// 		// that has finished, a compaction could happen and remove the underlying
// 		// memory that the slice points to. Since the hashes will be used to merge
// 		// filters - which happens after the RowReader has completed - this can lead
// 		// to segfault crashes. Now is the time to safely copy it, creating a new
// 		// and immutable slice.
// 		hashes = append(hashes, copyBytes(currHash))
// 		if limit > 0 && pointers.count >= uint64(limit) {
// 			return false, nil
// 		}

// 		return true, nil
// 	}); err != nil {
// 		return pointers, errors.Wrap(err, "read row")
// 	}

// 	pointers.checksum = combineChecksums(hashes, pv.operator)

// 	if !tolerateDuplicates {
// 		pointers.removeDuplicates()
// 	}
// 	return pointers, nil
// }

// func (s *Searcher) docPointersGeo(pv *propValuePair) (docPointers, error) {
// 	propIndex, ok := s.propIndices.ByProp(pv.prop)
// 	out := docPointers{}
// 	if !ok {
// 		return out, nil
// 	}

// 	ctx := context.TODO() // TODO: pass through instead of spawning new
// 	res, err := propIndex.GeoIndex.WithinRange(ctx, *pv.valueGeoRange)
// 	if err != nil {
// 		return out, errors.Wrapf(err, "geo index range search on prop %q", pv.prop)
// 	}

// 	out.docIDs = res
// 	out.count = uint64(len(res))

// 	// we can not use the checksum in the same fashion as with the inverted
// 	// index, i.e. it can not prevent a search as the underlying index does not
// 	// have any understanding of checksums which could prevent such a read.
// 	// However, there is more use in the checksum: It can also be used in merging
// 	// searches (e.g. cond1 AND cond2). The merging operation itself is expensive
// 	// and cachable, therefore there is a lot of value in calculating and
// 	// returning a checksum - even for geoProps.
// 	chksum, err := docPointerChecksum(res)
// 	if err != nil {
// 		return out, errors.Wrap(err, "calculate checksum")
// 	}
// 	out.checksum = chksum

// 	return out, nil
// }

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
