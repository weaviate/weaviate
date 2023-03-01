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

package db

import (
	"encoding/binary"
	"math"

	"github.com/weaviate/weaviate/entities/filters"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

func (s *Shard) extendInvertedIndicesLSM(props []inverted.Property, nilProps []nilProp,
	docID uint64,
) error {
	for _, prop := range props {
		if err := s.addToPropertyValueIndex(docID, prop); err != nil {
			return err
		}

		// add non-nil properties to the null-state inverted index, but skip internal properties (__meta_count, _id etc)
		if isMetaCountProperty(prop) || isInternalProperty(prop) {
			continue
		}

		// properties where defining a length does not make sense (floats etc.) have a negative entry as length
		if s.index.invertedIndexConfig.IndexPropertyLength && prop.Length >= 0 {
			if err := s.addToPropertyLengthIndex(prop.Name, docID, prop.Length); err != nil {
				return errors.Wrap(err, "add indexed property length")
			}
		}

		if s.index.invertedIndexConfig.IndexNullState {
			if err := s.addToPropertyNullIndex(prop.Name, docID, prop.Length == 0); err != nil {
				return errors.Wrap(err, "add indexed null state")
			}
		}
	}

	// add nil properties to the nullstate and property length inverted index
	for _, nilProperty := range nilProps {
		if s.index.invertedIndexConfig.IndexPropertyLength && nilProperty.AddToPropertyLength {
			if err := s.addToPropertyLengthIndex(nilProperty.Name, docID, 0); err != nil {
				return errors.Wrap(err, "add indexed property length")
			}
		}

		if s.index.invertedIndexConfig.IndexNullState {
			if err := s.addToPropertyNullIndex(nilProperty.Name, docID, true); err != nil {
				return errors.Wrap(err, "add indexed null state")
			}
		}
	}

	return nil
}

func (s *Shard) addToPropertyValueIndex(docID uint64, property inverted.Property) error {
	bucketValue := s.store.Bucket(helpers.BucketFromPropNameLSM(property.Name))
	if bucketValue == nil {
		return errors.Errorf("no bucket for prop '%s' found", property.Name)
	}

	hashBucketValue := s.store.Bucket(helpers.HashBucketFromPropNameLSM(property.Name))
	if hashBucketValue == nil {
		return errors.Errorf("no hash bucket for prop '%s' found", property.Name)
	}

	if property.HasFrequency {
		propLen := float32(len(property.Items))
		for _, item := range property.Items {
			key := item.Data
			if err := s.addToPropertyHashBucket(hashBucketValue, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value hash bucket", property.Name)
			}
			pair := s.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
			if err := s.addToPropertyMapBucket(bucketValue, pair, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
			}
		}
	} else {
		for _, item := range property.Items {
			key := item.Data
			if err := s.addToPropertyHashBucket(hashBucketValue, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value hash bucket", property.Name)
			}
			if err := s.addToPropertySetBucket(bucketValue, docID, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
			}
		}
	}
	return nil
}

func (s *Shard) addToPropertyLengthIndex(propName string, docID uint64, length int) error {
	bucketLength := s.store.Bucket(helpers.BucketFromPropNameLengthLSM(propName))
	if bucketLength == nil {
		return errors.Errorf("no bucket for prop '%s' length found", propName)
	}

	hashBucketLength := s.store.Bucket(helpers.HashBucketFromPropNameLengthLSM(propName))
	if hashBucketLength == nil {
		return errors.Errorf("no hash bucket for prop '%s' length found", propName)
	}

	key, err := s.keyPropertyLength(length)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' length", propName)
	}
	if err := s.addToPropertyHashBucket(hashBucketLength, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' length hash bucket", propName)
	}
	if err := s.addToPropertySetBucket(bucketLength, docID, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' length bucket", propName)
	}
	return nil
}

func (s *Shard) addToPropertyNullIndex(propName string, docID uint64, isNull bool) error {
	bucketNull := s.store.Bucket(helpers.BucketFromPropNameNullLSM(propName))
	if bucketNull == nil {
		return errors.Errorf("no bucket for prop '%s' null found", propName)
	}

	hashBucketNull := s.store.Bucket(helpers.HashBucketFromPropNameNullLSM(propName))
	if hashBucketNull == nil {
		return errors.Errorf("no hash bucket for prop '%s' null found", propName)
	}

	key, err := s.keyPropertyNull(isNull)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' null", propName)
	}
	if err := s.addToPropertyHashBucket(hashBucketNull, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' null hash bucket", propName)
	}
	if err := s.addToPropertySetBucket(bucketNull, docID, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' null bucket", propName)
	}
	return nil
}

func (s *Shard) pairPropertyWithFrequency(docID uint64, freq, propLen float32) lsmkv.MapPair {
	// 8 bytes for doc id, 4 bytes for frequency, 4 bytes for prop term length
	buf := make([]byte, 16)

	// Shard Index version 2 requires BigEndian for sorting, if the shard was
	// built prior assume it uses LittleEndian
	if s.versioner.Version() < 2 {
		binary.LittleEndian.PutUint64(buf[0:8], docID)
	} else {
		binary.BigEndian.PutUint64(buf[0:8], docID)
	}
	binary.LittleEndian.PutUint32(buf[8:12], math.Float32bits(freq))
	binary.LittleEndian.PutUint32(buf[12:16], math.Float32bits(propLen))

	return lsmkv.MapPair{
		Key:   buf[:8],
		Value: buf[8:],
	}
}

func (s *Shard) keyPropertyLength(length int) ([]byte, error) {
	return inverted.LexicographicallySortableInt64(int64(length))
}

func (s *Shard) keyPropertyNull(isNull bool) ([]byte, error) {
	if isNull {
		return []byte{uint8(filters.InternalNullState)}, nil
	}
	return []byte{uint8(filters.InternalNotNullState)}, nil
}

func (s *Shard) addToPropertyHashBucket(hashBucket *lsmkv.Bucket, key []byte) error {
	lsmkv.CheckExpectedStrategy(hashBucket.Strategy(), lsmkv.StrategyReplace)

	hash, err := s.generateRowHash()
	if err != nil {
		return err
	}

	if err := hashBucket.Put(key, hash); err != nil {
		return err
	}

	return nil
}

func (s *Shard) addToPropertyMapBucket(bucket *lsmkv.Bucket, pair lsmkv.MapPair, key []byte) error {
	lsmkv.CheckExpectedStrategy(bucket.Strategy(), lsmkv.StrategyMapCollection)

	return bucket.MapSet(key, pair)
}

func (s *Shard) addToPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	lsmkv.CheckExpectedStrategy(bucket.Strategy(), lsmkv.StrategySetCollection, lsmkv.StrategyRoaringSet)

	if bucket.Strategy() == lsmkv.StrategySetCollection {
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)

		return bucket.SetAdd(key, [][]byte{docIDBytes})
	}

	return bucket.RoaringSetAddOne(key, docID)
}

func (s *Shard) batchExtendInvertedIndexItemsLSMNoFrequency(b, hashBucket *lsmkv.Bucket,
	item inverted.MergeItem,
) error {
	if b.Strategy() != lsmkv.StrategySetCollection && b.Strategy() != lsmkv.StrategyRoaringSet {
		panic("prop has no frequency, but bucket does not have 'Set' nor 'RoaringSet' strategy")
	}

	hash, err := s.generateRowHash()
	if err != nil {
		return err
	}

	if err := hashBucket.Put(item.Data, hash); err != nil {
		return err
	}

	if b.Strategy() == lsmkv.StrategyRoaringSet {
		docIDs := make([]uint64, len(item.DocIDs))
		for i, idTuple := range item.DocIDs {
			docIDs[i] = idTuple.DocID
		}
		return b.RoaringSetAddList(item.Data, docIDs)
	}

	docIDs := make([][]byte, len(item.DocIDs))
	for i, idTuple := range item.DocIDs {
		docIDs[i] = make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDs[i], idTuple.DocID)
	}

	return b.SetAdd(item.Data, docIDs)
}

// the row hash isn't actually a hash at this point, it is just a random
// sequence of bytes. The important thing is that every new write into this row
// replaces the hash as the read cacher will make a decision based on the hash
// if it should read the row again from cache. So changing the "hash" (by
// replacing it with other random bytes) is essentially just a signal to the
// read-time cacher to invalidate its entry
func (s *Shard) generateRowHash() ([]byte, error) {
	return s.randomSource.Make(8)
}

func (s *Shard) addPropLengths(props []inverted.Property) error {
	for _, prop := range props {
		if !prop.HasFrequency {
			continue
		}

		if err := s.propLengths.TrackProperty(prop.Name, float32(len(prop.Items))); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shard) extendDimensionTrackerLSM(
	count int, docID uint64,
) error {
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return errors.Errorf("no bucket dimensions")
	}

	// 4 bytes for dim count (row key), 8 bytes for doc id (map key), 0 bytes for
	// map value
	buf := make([]byte, 12)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(count))
	binary.LittleEndian.PutUint64(buf[4:12], docID)

	pair := lsmkv.MapPair{
		Key:   buf[4:12],
		Value: buf[12:12],
	}

	return b.MapSet(buf[0:4], pair)
}

// Key (dimensionality) | Value Doc IDs
// 128 | 1,2,4,5,17
// 128 | 1,2,4,5,17, Tombstone 4,

func (s *Shard) removeDimensionsLSM(
	count int, docID uint64,
) error {
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return errors.Errorf("no bucket dimensions")
	}

	// 4 bytes for dim count (row key), 8 bytes for doc id (map key), 0 bytes for
	// map value
	buf := make([]byte, 12)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(count))
	binary.LittleEndian.PutUint64(buf[4:12], docID)

	pair := lsmkv.MapPair{
		Key:       buf[4:12],
		Value:     buf[12:12],
		Tombstone: true,
	}

	return b.MapSet(buf[0:4], pair)
}

func isMetaCountProperty(property inverted.Property) bool {
	return len(property.Name) > 12 && property.Name[len(property.Name)-12:] == "__meta_count"
}

func isInternalProperty(property inverted.Property) bool {
	return property.Name[0] == '_'
}
