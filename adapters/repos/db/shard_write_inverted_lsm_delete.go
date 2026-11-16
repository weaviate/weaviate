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
	"encoding/binary"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

func (w *invertedWrite) deleteProps() error {
	s, docID, st := w.s, w.status.oldDocID, w.st
	for i, prop := range w.toDel {
		sl := w.delSlots[i]
		if sl.filterable != noSlot {
			bucket := w.bucket(sl.filterable)
			for _, item := range prop.Items {
				if err := s.deleteFromPropertySetBucket(bucket, docID, item.Data); err != nil {
					return errors.Wrapf(err, "delete item '%s' from index",
						string(item.Data))
				}
			}
		}

		if sl.searchable != noSlot {
			bucket := w.bucket(sl.searchable)
			for _, item := range prop.Items {
				if err := s.deleteInvertedIndexItemWithFrequencyLSM(bucket, item,
					docID); err != nil {
					return errors.Wrapf(err, "delete item '%s' from index",
						string(item.Data))
				}
			}
		}

		if sl.rangeable != noSlot {
			bucket := w.bucket(sl.rangeable)
			for _, item := range prop.Items {
				if err := s.deleteFromPropertyRangeBucket(bucket, docID, item.Data); err != nil {
					return errors.Wrapf(err, "delete item '%s' from index",
						string(item.Data))
				}
			}
		}

		// Suppress the inline callback for props under migration; their
		// target-schema delete double-write is handled by the migration pass.
		if _, migrating := st.scope.props[prop.Name]; !migrating {
			if err := s.fireDeleteFromPropertyValueIndex(st.del, docID, &prop); err != nil {
				return err
			}
		}

		if sl.length != noSlot {
			if err := s.deleteFromPropertyLengthIndex(w.bucket(sl.length), prop.Name, docID, prop.Length); err != nil {
				return errors.Wrap(err, "add indexed property length")
			}
		}
		if sl.null != noSlot {
			if err := s.deleteFromPropertyNullIndex(w.bucket(sl.null), prop.Name, docID, prop.Length == 0); err != nil {
				return errors.Wrap(err, "add indexed null state")
			}
		}
	}

	for i, nilProperty := range w.nilDel {
		if sl := w.nilDelSlots[i]; sl.length != noSlot {
			if err := s.deleteFromPropertyLengthIndex(w.bucket(sl.length), nilProperty.Name, docID, 0); err != nil {
				return errors.Wrap(err, "add indexed property length")
			}
		}
		if sl := w.nilDelSlots[i]; sl.null != noSlot {
			if err := s.deleteFromPropertyNullIndex(w.bucket(sl.null), nilProperty.Name, docID, true); err != nil {
				return errors.Wrap(err, "add indexed null state")
			}
		}
	}

	return nil
}

func (s *Shard) deleteInvertedIndexItemWithFrequencyLSM(bucket *lsmkv.Bucket,
	item inverted.Countable, docID uint64,
) error {
	lsmkv.MustBeExpectedStrategy(bucket.Strategy(), lsmkv.StrategyMapCollection, lsmkv.StrategyInverted)

	if bucket.Strategy() == lsmkv.StrategyInverted {
		return bucket.InvertedDeleteDoc(item.Data, s.searchableDocID(docID))
	}

	docIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(docIDBytes, s.searchableDocID(docID))

	return bucket.MapDeleteKey(item.Data, docIDBytes)
}

func (s *Shard) deleteFromPropertyLengthIndex(bucketLength *lsmkv.Bucket, propName string, docID uint64, length int) error {
	key, err := bucketKeyPropertyLength(length)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' length", propName)
	}
	if err := s.deleteFromPropertySetBucket(bucketLength, docID, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' length bucket", propName)
	}
	return nil
}

func (s *Shard) deleteFromPropertyNullIndex(bucketNull *lsmkv.Bucket, propName string, docID uint64, isNull bool) error {
	key, err := bucketKeyPropertyNull(isNull)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' null", propName)
	}
	if err := s.deleteFromPropertySetBucket(bucketNull, docID, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' null bucket", propName)
	}
	return nil
}

func (s *Shard) deleteFromPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	lsmkv.MustBeExpectedStrategy(bucket.Strategy(), lsmkv.StrategySetCollection, lsmkv.StrategyRoaringSet)

	if bucket.Strategy() == lsmkv.StrategySetCollection {
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)

		return bucket.SetDeleteSingle(key, docIDBytes)
	}

	return bucket.RoaringSetRemoveOne(key, docID)
}

func (s *Shard) deleteFromPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	lsmkv.MustBeExpectedStrategy(bucket.Strategy(), lsmkv.StrategyRoaringSetRange)

	if len(key) != 8 {
		return fmt.Errorf("shard: invalid value length %d, should be 8 bytes", len(key))
	}

	return bucket.RoaringSetRangeRemove(binary.BigEndian.Uint64(key), docID)
}
