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

package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/errorcompounder"
)

func (s *Shard) extendInvertedIndicesLSM(props []inverted.Property, nilProps []inverted.NilProperty,
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
	if property.HasFilterableIndex {
		bucketValue := s.store.Bucket(helpers.BucketFromPropNameLSM(property.Name))
		if bucketValue == nil {
			return errors.Errorf("no bucket for prop '%s' found", property.Name)
		}

		for _, item := range property.Items {
			key := item.Data
			if err := s.addToPropertySetBucket(bucketValue, docID, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
			}
		}
	}

	if property.HasSearchableIndex {
		bucketValue := s.store.Bucket(helpers.BucketSearchableFromPropNameLSM(property.Name))
		if bucketValue == nil {
			return errors.Errorf("no bucket searchable for prop '%s' found", property.Name)
		}
		propLen := float32(0)

		if bucketValue.Strategy() == lsmkv.StrategyInverted {
			// Iterating over all items to calculate the property length, which is the sum of all term frequencies
			for _, item := range property.Items {
				propLen += item.TermFrequency
			}
		} else {
			// This is the old way of calculating the property length, which counts terms that show up multiple times only once,
			// which is not standard for BM25
			propLen = float32(len(property.Items))
		}
		for _, item := range property.Items {
			key := item.Data
			pair := s.pairPropertyWithFrequency(docID, item.TermFrequency, propLen)
			if err := s.addToPropertyMapBucket(bucketValue, pair, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
			}
		}
	}

	if property.HasRangeableIndex {
		bucketValue := s.store.Bucket(helpers.BucketRangeableFromPropNameLSM(property.Name))
		if bucketValue == nil {
			return errors.Errorf("no bucket rangeable for prop '%s' found", property.Name)
		}

		for _, item := range property.Items {
			key := item.Data
			if err := s.addToPropertyRangeBucket(bucketValue, docID, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
			}
		}
	}

	if err := s.onAddToPropertyValueIndex(docID, &property); err != nil {
		return err
	}

	return nil
}

func (s *Shard) addToPropertyLengthIndex(propName string, docID uint64, length int) error {
	bucketLength := s.store.Bucket(helpers.BucketFromPropNameLengthLSM(propName))
	if bucketLength == nil {
		return errors.Errorf("no bucket for prop '%s' length found", propName)
	}

	key, err := bucketKeyPropertyLength(length)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' length", propName)
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

	key, err := bucketKeyPropertyNull(isNull)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' null", propName)
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

func (s *Shard) addToPropertyMapBucket(bucket *lsmkv.Bucket, pair lsmkv.MapPair, key []byte) error {
	lsmkv.MustBeExpectedStrategy(bucket.Strategy(), lsmkv.StrategyMapCollection, lsmkv.StrategyInverted)

	return bucket.MapSet(key, pair)
}

func (s *Shard) addToPropertySetBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	lsmkv.MustBeExpectedStrategy(bucket.Strategy(), lsmkv.StrategySetCollection, lsmkv.StrategyRoaringSet)

	if bucket.Strategy() == lsmkv.StrategySetCollection {
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)

		return bucket.SetAdd(key, [][]byte{docIDBytes})
	}

	return bucket.RoaringSetAddOne(key, docID)
}

func (s *Shard) addToPropertyRangeBucket(bucket *lsmkv.Bucket, docID uint64, key []byte) error {
	lsmkv.MustBeExpectedStrategy(bucket.Strategy(), lsmkv.StrategyRoaringSetRange)

	if len(key) != 8 {
		return fmt.Errorf("shard: invalid value length %d, should be 8 bytes", len(key))
	}

	return bucket.RoaringSetRangeAdd(binary.BigEndian.Uint64(key), docID)
}

func (s *Shard) batchExtendInvertedIndexItemsLSMNoFrequency(b *lsmkv.Bucket,
	item inverted.MergeItem,
) error {
	if b.Strategy() != lsmkv.StrategySetCollection && b.Strategy() != lsmkv.StrategyRoaringSet {
		panic("prop has no frequency, but bucket does not have 'Set' nor 'RoaringSet' strategy")
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

func (s *Shard) SetPropertyLengths(props []inverted.Property) error {
	for _, prop := range props {
		if !prop.HasSearchableIndex {
			continue
		}

		if err := s.GetPropertyLengthTracker().TrackProperty(prop.Name, float32(len(prop.Items))); err != nil {
			return err
		}

	}

	return nil
}

func (s *Shard) subtractPropLengths(props []inverted.Property) error {
	for _, prop := range props {
		if !prop.HasSearchableIndex {
			continue
		}

		if err := s.GetPropertyLengthTracker().UnTrackProperty(prop.Name, float32(len(prop.Items))); err != nil {
			return err
		}

	}

	return nil
}

func (s *Shard) extendDimensionTrackerLSM(
	dimLength int, docID uint64, targetVector string,
) error {
	return s.addToDimensionBucket(dimLength, docID, targetVector, false)
}

var uniqueCounter atomic.Uint64

// GenerateUniqueString generates a random string of the specified length
func GenerateUniqueString(length int) (string, error) {
	uniqueCounter.Add(1)
	return fmt.Sprintf("%v", uniqueCounter.Load()), nil
}

// Empty the dimensions bucket, quickly and efficiently
func (s *Shard) resetDimensionsLSM() error {
	// Load the current one, or an empty one if it doesn't exist
	err := s.store.CreateOrLoadBucket(context.Background(),
		helpers.DimensionsBucketLSM,
		s.memtableDirtyConfig(),
		lsmkv.WithStrategy(lsmkv.StrategyMapCollection),
		lsmkv.WithPread(s.index.Config.AvoidMMap),
		lsmkv.WithAllocChecker(s.index.allocChecker),
		lsmkv.WithMaxSegmentSize(s.index.Config.MaxSegmentSize),
		lsmkv.WithMinMMapSize(s.index.Config.MinMMapSize),
		lsmkv.WithMinWalThreshold(s.index.Config.MaxReuseWalSize),
		s.segmentCleanupConfig(),
	)
	if err != nil {
		return fmt.Errorf("create dimensions bucket: %w", err)
	}

	// Fetch the actual bucket
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return errors.Errorf("resetDimensionsLSM: no bucket dimensions")
	}

	// Create random bucket name
	name, err := GenerateUniqueString(32)
	if err != nil {
		return errors.Wrap(err, "generate unique bucket name")
	}

	// Create a new bucket with the unique name
	err = s.createDimensionsBucket(context.Background(), name)
	if err != nil {
		return errors.Wrap(err, "create temporary dimensions bucket")
	}

	// Replace the old bucket with the new one
	err = s.store.ReplaceBuckets(context.Background(), helpers.DimensionsBucketLSM, name)
	if err != nil {
		return errors.Wrap(err, "replace dimensions bucket")
	}

	return nil
}

// Key (target vector name and dimensionality) | Value Doc IDs
// targetVector,128 | 1,2,4,5,17
// targetVector,128 | 1,2,4,5,17, Tombstone 4,
func (s *Shard) removeDimensionsLSM(
	dimLength int, docID uint64, targetVector string,
) error {
	return s.addToDimensionBucket(dimLength, docID, targetVector, true)
}

func (s *Shard) addToDimensionBucket(
	dimLength int, docID uint64, vecName string, tombstone bool,
) error {
	err := s.addDimensionsProperty(context.Background())
	if err != nil {
		return errors.Wrap(err, "add dimensions property")
	}
	b := s.store.Bucket(helpers.DimensionsBucketLSM)
	if b == nil {
		return errors.Errorf("add dimension bucket: no bucket dimensions")
	}

	tv := []byte(vecName)
	// 8 bytes for doc id (map key)
	// 4 bytes for dim count (row key)
	// len(vecName) bytes for vector name (prefix of row key)
	buf := make([]byte, 12+len(tv))
	binary.LittleEndian.PutUint64(buf[:8], docID)
	binary.LittleEndian.PutUint32(buf[8+len(tv):], uint32(dimLength))
	copy(buf[8:], tv)

	return b.MapSet(buf[8:], lsmkv.MapPair{
		Key:       buf[:8],
		Value:     []byte{},
		Tombstone: tombstone,
	})
}

func (s *Shard) onAddToPropertyValueIndex(docID uint64, property *inverted.Property) error {
	ec := errorcompounder.New()
	for i := range s.callbacksAddToPropertyValueIndex {
		ec.Add(s.callbacksAddToPropertyValueIndex[i](s, docID, property))
	}
	return ec.ToError()
}

func isMetaCountProperty(property inverted.Property) bool {
	return len(property.Name) > 12 && property.Name[len(property.Name)-12:] == "__meta_count"
}

func isInternalProperty(property inverted.Property) bool {
	return property.Name[0] == '_'
}
