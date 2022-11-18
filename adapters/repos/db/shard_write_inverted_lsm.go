//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"encoding/binary"
	"math"

	"github.com/semi-technologies/weaviate/entities/filters"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
)

func (s *Shard) extendInvertedIndicesLSM(props []inverted.Property, nilProps []nilProp,
	docID uint64,
) error {
	for _, prop := range props {
		b := s.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name))
		if b == nil {
			return errors.Errorf("no bucket for prop '%s' found", prop.Name)
		}

		hashBucket := s.store.Bucket(helpers.HashBucketFromPropNameLSM(prop.Name))
		if hashBucket == nil {
			return errors.Errorf("no hash bucket for prop '%s' found", prop.Name)
		}

		if prop.HasFrequency {
			for _, item := range prop.Items {
				if err := s.extendInvertedIndexItemWithFrequencyLSM(b, hashBucket, item,
					docID, item.TermFrequency, float32(len(prop.Items))); err != nil {
					return errors.Wrapf(err, "extend index with item '%s'",
						string(item.Data))
				}
			}
		} else {
			for _, item := range prop.Items {
				if err := s.extendInvertedIndexItemLSM(b, hashBucket, item, docID); err != nil {
					return errors.Wrapf(err, "extend index with item '%s'",
						string(item.Data))
				}
			}
		}

		// add non-nil properties to the null-state inverted index, but skip internal properties (__meta_count, _id etc)
		if (len(prop.Name) > 12 && prop.Name[len(prop.Name)-12:] == "__meta_count") ||
			prop.Name[0] == '_' {
			continue
		}

		// properties where defining a length does not make sense (floats etc.) have a negative entry as length
		if s.index.invertedIndexConfig.IndexPropertyLength && prop.Length >= 0 {
			if err := s.addIndexedPropertyLengthToProps(docID, prop.Name, prop.Length); err != nil {
				return errors.Wrap(err, "add indexed property length")
			}
		}

		if s.index.invertedIndexConfig.IndexNullState {
			if err := s.addIndexedNullStateToProps(docID, prop.Name, prop.Length == 0); err != nil {
				return errors.Wrap(err, "add indexed null state")
			}
		}
	}

	// add nil properties to the nullstate and property length inverted index
	for _, nilProperty := range nilProps {
		if s.index.invertedIndexConfig.IndexNullState {
			if err := s.addIndexedNullStateToProps(docID, nilProperty.Name, true); err != nil {
				return errors.Wrap(err, "add indexed null state")
			}
		}

		if s.index.invertedIndexConfig.IndexPropertyLength && nilProperty.AddToPropertyLength {
			if err := s.addIndexedPropertyLengthToProps(docID, nilProperty.Name, 0); err != nil {
				return errors.Wrap(err, "add indexed property length")
			}
		}

	}

	return nil
}

func (s *Shard) addIndexedPropertyLengthToProps(docID uint64, propName string, length int) error {
	bLength := s.store.Bucket(helpers.BucketFromPropNameLSM(propName + filters.InternalPropertyLength))
	if bLength == nil {
		return errors.Errorf("no bucket prop '%s' found", propName+filters.InternalPropertyLength)
	}

	if bLength.Strategy() != lsmkv.StrategySetCollection {
		panic("prop has no frequency, but bucket does not have 'Set' strategy")
	}

	hashBucketPropertyLength := s.store.Bucket(helpers.HashBucketFromPropNameLSM(propName + filters.InternalPropertyLength))
	if hashBucketPropertyLength == nil {
		return errors.Errorf("no hash bucket for prop '%s' found", propName+filters.InternalPropertyLength)
	}

	hash, err := s.generateRowHash()
	if err != nil {
		return err
	}

	key, err := inverted.LexicographicallySortableInt64(int64(length))
	if err != nil {
		return err
	}

	if err := hashBucketPropertyLength.Put(key, hash); err != nil {
		return err
	}
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)

	return bLength.SetAdd(key, [][]byte{docIDBytes})
}

func (s *Shard) addIndexedNullStateToProps(docID uint64, propName string, isNil bool) error {
	bNullState := s.store.Bucket(helpers.BucketFromPropNameLSM(propName + filters.InternalNullIndex))
	if bNullState == nil {
		return errors.Errorf("no bucket for nil prop '%s' found", propName+filters.InternalNullIndex)
	}

	hashBucketNullState := s.store.Bucket(helpers.HashBucketFromPropNameLSM(propName + filters.InternalNullIndex))
	if hashBucketNullState == nil {
		return errors.Errorf("no nil-hash bucket for prop '%s' found", propName+filters.InternalNullIndex)
	}

	if bNullState.Strategy() != lsmkv.StrategySetCollection {
		panic("prop has no frequency, but bucket does not have 'Set' strategy")
	}

	hash, err := s.generateRowHash()
	if err != nil {
		return err
	}

	var key uint8
	if isNil {
		key = uint8(filters.InternalNullState)
	} else {
		key = uint8(filters.InternalNotNullState)
	}
	if err := hashBucketNullState.Put([]byte{key}, hash); err != nil {
		return err
	}
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)

	return bNullState.SetAdd([]byte{key}, [][]byte{docIDBytes})
}

func (s *Shard) extendInvertedIndexItemWithFrequencyLSM(b, hashBucket *lsmkv.Bucket,
	item inverted.Countable, docID uint64, frequency float32, propLen float32,
) error {
	if b.Strategy() != lsmkv.StrategyMapCollection {
		panic("prop has frequency, but bucket does not have 'Map' strategy")
	}

	hash, err := s.generateRowHash()
	if err != nil {
		return err
	}

	if err := hashBucket.Put(item.Data, hash); err != nil {
		return err
	}

	// 8 bytes for doc id, 4 bytes for frequency, 4 bytes for prop term length
	buf := make([]byte, 16)

	// Shard Index version 2 requires BigEndian for sorting, if the shard was
	// built prior assume it uses LittleEndian
	if s.versioner.Version() < 2 {
		binary.LittleEndian.PutUint64(buf[0:8], docID)
	} else {
		binary.BigEndian.PutUint64(buf[0:8], docID)
	}
	binary.LittleEndian.PutUint32(buf[8:12], math.Float32bits(item.TermFrequency))
	binary.LittleEndian.PutUint32(buf[12:16], math.Float32bits(propLen))

	pair := lsmkv.MapPair{
		Key:   buf[:8],
		Value: buf[8:],
	}

	return b.MapSet(item.Data, pair)
}

func (s *Shard) extendInvertedIndexItemLSM(b, hashBucket *lsmkv.Bucket,
	item inverted.Countable, docID uint64,
) error {
	if b.Strategy() != lsmkv.StrategySetCollection {
		panic("prop has no frequency, but bucket does not have 'Set' strategy")
	}

	hash, err := s.generateRowHash()
	if err != nil {
		return err
	}

	if err := hashBucket.Put(item.Data, hash); err != nil {
		return err
	}

	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)

	return b.SetAdd(item.Data, [][]byte{docIDBytes})
}

func (s *Shard) batchExtendInvertedIndexItemsLSMNoFrequency(b, hashBucket *lsmkv.Bucket,
	item inverted.MergeItem,
) error {
	if b.Strategy() != lsmkv.StrategySetCollection {
		panic("prop has no frequency, but bucket does not have 'Set' strategy")
	}

	hash, err := s.generateRowHash()
	if err != nil {
		return err
	}

	if err := hashBucket.Put(item.Data, hash); err != nil {
		return err
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

		s.propLengths.TrackProperty(prop.Name, float32(len(prop.Items)))
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
