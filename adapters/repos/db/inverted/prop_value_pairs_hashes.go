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

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
)

func (pv *propValuePair) cacheable() bool {
	switch pv.operator {
	case filters.OperatorEqual, filters.OperatorAnd, filters.OperatorOr,
		filters.OperatorNotEqual, filters.OperatorLike:
		// do not cache nested queries with an extreme amount of operands, such as
		// ref-filter queries. For those queries, just checking the large amount of
		// hashes has a very signifcant cost - even if they all turn out to be
		// cache misses
		if len(pv.children) >= 10000 {
			return false
		}
		for _, child := range pv.children {
			if !child.cacheable() {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (pv *propValuePair) fetchHashes(s *Searcher) error {
	if pv.operator.OnValue() {
		bucketName := helpers.HashBucketFromPropNameLSM(pv.prop)

		b := s.store.Bucket(bucketName)
		if b == nil && pv.operator != filters.OperatorWithinGeoRange {
			return errors.Errorf("hash bucket for prop %s not found - is it indexed?", pv.prop)
		}

		var hash []byte
		var err error
		if pv.operator == filters.OperatorEqual {
			hash, err = b.Get(pv.value)
			if err != nil {
				return err
			}
		} else {
			hash, err = pv.hashForNonEqualOp(s.store, b, s.shardVersion)
			if err != nil {
				return err
			}

		}

		pv.docIDs.checksum = hash
	} else {
		checksums := make([][]byte, len(pv.children))
		for i, child := range pv.children {
			if err := child.fetchHashes(s); err != nil {
				return errors.Wrap(err, "child filter")
			}

			checksums[i] = child.docIDs.checksum
		}

		pv.docIDs.checksum = combineChecksums(checksums, pv.operator)
	}

	return nil
}

func (pv *propValuePair) hashForNonEqualOp(store *lsmkv.Store,
	hashBucket *lsmkv.Bucket, shardVersion uint16,
) ([]byte, error) {
	if pv.isFilterable {
		bucketName := helpers.BucketFromPropNameLSM(pv.prop)
		bucket := store.Bucket(bucketName)

		if bucket == nil && pv.operator != filters.OperatorWithinGeoRange {
			return nil, errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		if bucket.Strategy() == lsmkv.StrategyRoaringSet {
			return pv.hashForNonEqualOpWithoutFrequencyRoaringSet(bucket, hashBucket)
		}

		return pv.hashForNonEqualOpWithoutFrequencySet(bucket, hashBucket)
	}

	if pv.isSearchable {
		bucketName := helpers.BucketSearchableFromPropNameLSM(pv.prop)
		bucket := store.Bucket(bucketName)

		if bucket == nil && pv.operator != filters.OperatorWithinGeoRange {
			return nil, errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		return pv.hashForNonEqualOpWithFrequency(bucket, hashBucket, shardVersion)
	}

	return nil, errors.New("property is neither filterable nor searchable")
}

func (pv *propValuePair) hashForNonEqualOpWithoutFrequencySet(propBucket,
	hashBucket *lsmkv.Bucket,
) ([]byte, error) {
	rr := NewRowReader(propBucket, pv.value, pv.operator, true)

	var keys [][]byte
	if err := rr.Read(context.TODO(), func(k []byte, ids [][]byte) (bool, error) {
		keys = append(keys, k)
		return true, nil
	}); err != nil {
		return nil, errors.Wrap(err, "read row")
	}

	hashes := make([][]byte, len(keys))
	for i, key := range keys {
		h, err := hashBucket.Get(key)
		if err != nil {
			return nil, errors.Wrapf(err, "get hash for key %v", key)
		}
		hashes[i] = h
	}

	return combineChecksums(hashes, pv.operator), nil
}

func (pv *propValuePair) hashForNonEqualOpWithoutFrequencyRoaringSet(propBucket,
	hashBucket *lsmkv.Bucket,
) ([]byte, error) {
	rr := NewRowReaderRoaringSet(propBucket, pv.value, pv.operator, true)

	var keys [][]byte
	var readFn RoaringSetReadFn = func(k []byte, _ *sroar.Bitmap) (bool, error) {
		keys = append(keys, k)
		return true, nil
	}

	if err := rr.Read(context.TODO(), readFn); err != nil {
		return nil, errors.Wrap(err, "read row")
	}

	hashes := make([][]byte, len(keys))
	for i, key := range keys {
		h, err := hashBucket.Get(key)
		if err != nil {
			return nil, errors.Wrapf(err, "get hash for key %v", key)
		}
		hashes[i] = h
	}

	return combineChecksums(hashes, pv.operator), nil
}

func (pv *propValuePair) hashForNonEqualOpWithFrequency(propBucket,
	hashBucket *lsmkv.Bucket, shardVersion uint16,
) ([]byte, error) {
	rr := NewRowReaderFrequency(propBucket, pv.value, pv.operator, true, shardVersion)

	var keys [][]byte
	if err := rr.Read(context.TODO(), func(k []byte, ids []lsmkv.MapPair) (bool, error) {
		keys = append(keys, k)
		return true, nil
	}); err != nil {
		return nil, errors.Wrap(err, "read row")
	}

	hashes := make([][]byte, len(keys))
	for i, key := range keys {
		h, err := hashBucket.Get(key)
		if err != nil {
			return nil, errors.Wrapf(err, "get hash for key %v", key)
		}
		hashes[i] = h
	}

	return combineChecksums(hashes, pv.operator), nil
}
