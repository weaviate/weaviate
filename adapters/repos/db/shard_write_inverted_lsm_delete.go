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
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// deleteFromInvertedIndicesLSM removes docID's postings for the given props.
// Every bucket written to is reported into touched (nil-safe), so a delete's
// durability barrier can sync exactly those WALs.
func (s *Shard) deleteFromInvertedIndicesLSM(props []inverted.Property, nilProps []inverted.NilProperty,
	docID uint64, st *propValueIndexState, touched *touchedBuckets,
) error {
	for _, prop := range props {
		if prop.HasFilterableIndex {
			bucketName := helpers.BucketFromPropNameLSM(prop.Name)
			bucket := s.store.Bucket(bucketName)
			if bucket == nil {
				return fmt.Errorf("no bucket for prop '%s' found", prop.Name)
			}
			if len(prop.Items) > 0 {
				touched.add(bucketName)
			}

			for _, item := range prop.Items {
				if err := s.deleteFromPropertySetBucket(bucket, docID, item.Data); err != nil {
					return errors.Wrapf(err, "delete item '%s' from index",
						string(item.Data))
				}
			}
		}

		if prop.HasSearchableIndex {
			bucketName := helpers.BucketSearchableFromPropNameLSM(prop.Name)
			bucket := s.store.Bucket(bucketName)
			if bucket == nil {
				return fmt.Errorf("no bucket searchable for prop '%s' found", prop.Name)
			}
			if len(prop.Items) > 0 {
				touched.add(bucketName)
			}

			for _, item := range prop.Items {
				if err := s.deleteInvertedIndexItemWithFrequencyLSM(bucket, item,
					docID); err != nil {
					return errors.Wrapf(err, "delete item '%s' from index",
						string(item.Data))
				}
			}
		}

		if prop.HasRangeableIndex {
			bucketName := helpers.BucketRangeableFromPropNameLSM(prop.Name)
			bucket := s.store.Bucket(bucketName)
			if bucket == nil {
				return fmt.Errorf("no bucket rangeable for prop %q found", prop.Name)
			}
			if len(prop.Items) > 0 {
				touched.add(bucketName)
			}
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
			if err := s.fireDeleteFromPropertyValueIndex(st, docID, &prop); err != nil {
				return err
			}
		}

		// add non-nil properties to the null-state inverted index, but skip internal properties (__meta_count, _id etc)
		if isMetaCountProperty(prop) || isInternalProperty(prop) {
			continue
		}

		// properties where defining a length does not make sense (floats etc.) have a negative entry as length
		if s.index.invertedIndexConfig.IndexPropertyLength && prop.Length >= 0 {
			if err := s.deleteFromPropertyLengthIndex(prop.Name, docID, prop.Length, touched); err != nil {
				return errors.Wrap(err, "add indexed property length")
			}
		}

		if s.index.invertedIndexConfig.IndexNullState {
			if err := s.deleteFromPropertyNullIndex(prop.Name, docID, prop.Length == 0, touched); err != nil {
				return errors.Wrap(err, "add indexed null state")
			}
		}
	}

	// remove nil properties from the nullstate and property length inverted index
	for _, nilProperty := range nilProps {
		if s.index.invertedIndexConfig.IndexPropertyLength && nilProperty.AddToPropertyLength {
			if err := s.deleteFromPropertyLengthIndex(nilProperty.Name, docID, 0, touched); err != nil {
				return errors.Wrap(err, "add indexed property length")
			}
		}

		if s.index.invertedIndexConfig.IndexNullState {
			if err := s.deleteFromPropertyNullIndex(nilProperty.Name, docID, true, touched); err != nil {
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

	docIDBytes := make([]byte, 8)
	// Shard Index version 2 requires BigEndian for sorting, if the shard was
	// built prior assume it uses LittleEndian
	if s.versioner.Version() < 2 {
		binary.LittleEndian.PutUint64(docIDBytes, docID)
	} else {
		binary.BigEndian.PutUint64(docIDBytes, docID)
	}

	return bucket.MapDeleteKey(item.Data, docIDBytes)
}

func (s *Shard) deleteFromPropertyLengthIndex(propName string, docID uint64, length int, touched *touchedBuckets) error {
	bucketName := helpers.BucketFromPropNameLengthLSM(propName)
	bucketLength := s.store.Bucket(bucketName)
	if bucketLength == nil {
		return errors.Errorf("no bucket for prop '%s' length found", propName)
	}

	key, err := bucketKeyPropertyLength(length)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' length", propName)
	}
	if err := s.deleteFromPropertySetBucket(bucketLength, docID, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' length bucket", propName)
	}
	touched.add(bucketName)
	return nil
}

func (s *Shard) deleteFromPropertyNullIndex(propName string, docID uint64, isNull bool, touched *touchedBuckets) error {
	bucketName := helpers.BucketFromPropNameNullLSM(propName)
	bucketNull := s.store.Bucket(bucketName)
	if bucketNull == nil {
		return errors.Errorf("no bucket for prop '%s' null found", propName)
	}

	key, err := bucketKeyPropertyNull(isNull)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' null", propName)
	}
	if err := s.deleteFromPropertySetBucket(bucketNull, docID, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' null bucket", propName)
	}
	touched.add(bucketName)
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
