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
	"math"
	"math/bits"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/schema"
)

func (w *invertedWrite) addProps() error {
	s, docID := w.s, w.status.docID
	for i, prop := range w.toAdd {
		sl := w.addSlots[i]
		if err := w.addToPropertyValueIndex(docID, prop, sl); err != nil {
			return err
		}
		if sl.length != noSlot {
			if err := s.addToPropertyLengthIndex(w.bucket(sl.length), prop.Name, docID, prop.Length); err != nil {
				return errors.Wrap(err, "add indexed property length")
			}
		}
		if sl.null != noSlot {
			if err := s.addToPropertyNullIndex(w.bucket(sl.null), prop.Name, docID, prop.Length == 0); err != nil {
				return errors.Wrap(err, "add indexed null state")
			}
		}
	}

	for i, nilProperty := range w.nilAdd {
		if sl := w.nilAddSlots[i]; sl.length != noSlot {
			if err := s.addToPropertyLengthIndex(w.bucket(sl.length), nilProperty.Name, docID, 0); err != nil {
				return errors.Wrap(err, "add indexed property length")
			}
		}
		if sl := w.nilAddSlots[i]; sl.null != noSlot {
			if err := s.addToPropertyNullIndex(w.bucket(sl.null), nilProperty.Name, docID, true); err != nil {
				return errors.Wrap(err, "add indexed null state")
			}
		}
	}

	return nil
}

func (w *invertedWrite) addToPropertyValueIndex(docID uint64, property inverted.Property, sl propSlots) error {
	s := w.s
	if sl.filterable != noSlot {
		bucketValue := w.bucket(sl.filterable)
		for _, item := range property.Items {
			key := item.Data
			if err := s.addToPropertySetBucket(bucketValue, docID, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
			}
		}
	}

	if sl.searchable != noSlot {
		bucketValue := w.bucket(sl.searchable)
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
			if err := s.addToPropertyMapBucket(bucketValue, docID, key, item.TermFrequency, propLen); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
			}
		}
	}

	if sl.rangeable != noSlot {
		bucketValue := w.bucket(sl.rangeable)
		for _, item := range property.Items {
			key := item.Data
			if err := s.addToPropertyRangeBucket(bucketValue, docID, key); err != nil {
				return errors.Wrapf(err, "failed adding to prop '%s' value bucket", property.Name)
			}
		}
	}

	// Scope props skip st.add; invertedWrite.apply fires them through fireMirror under the TARGET overlay.
	if _, migrating := w.st.scope.props[property.Name]; !migrating {
		if err := s.fireAddToPropertyValueIndex(w.st.add, docID, &property); err != nil {
			return err
		}
	}

	return nil
}

func (s *Shard) addToPropertyLengthIndex(bucketLength *lsmkv.Bucket, propName string, docID uint64, length int) error {
	key, err := bucketKeyPropertyLength(length)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' length", propName)
	}
	if err := s.addToPropertySetBucket(bucketLength, docID, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' length bucket", propName)
	}
	return nil
}

func (s *Shard) addToPropertyNullIndex(bucketNull *lsmkv.Bucket, propName string, docID uint64, isNull bool) error {
	key, err := bucketKeyPropertyNull(isNull)
	if err != nil {
		return errors.Wrapf(err, "failed creating key for prop '%s' null", propName)
	}
	if err := s.addToPropertySetBucket(bucketNull, docID, key); err != nil {
		return errors.Wrapf(err, "failed adding to prop '%s' null bucket", propName)
	}
	return nil
}

// searchableDocID is the doc ID as a searchable bucket's key reads big-endian.
// Shards below index version 2 wrote those keys little-endian, so reverse.
func (s *Shard) searchableDocID(docID uint64) uint64 {
	if s.versioner.Version() < 2 {
		return bits.ReverseBytes64(docID)
	}
	return docID
}

func (s *Shard) pairPropertyWithFrequency(docID uint64, freq, propLen float32) lsmkv.MapPair {
	// 8 bytes for doc id, 4 bytes for frequency, 4 bytes for prop term length
	buf := make([]byte, 16)

	binary.BigEndian.PutUint64(buf[0:8], s.searchableDocID(docID))
	binary.LittleEndian.PutUint32(buf[8:12], math.Float32bits(freq))
	binary.LittleEndian.PutUint32(buf[12:16], math.Float32bits(propLen))

	return lsmkv.MapPair{
		Key:   buf[:8],
		Value: buf[8:],
	}
}

func (s *Shard) addToPropertyMapBucket(bucket *lsmkv.Bucket, docID uint64, key []byte, tf, propLen float32) error {
	lsmkv.MustBeExpectedStrategy(bucket.Strategy(), lsmkv.StrategyMapCollection, lsmkv.StrategyInverted)

	if bucket.Strategy() == lsmkv.StrategyInverted {
		return bucket.InvertedSet(key, s.searchableDocID(docID), tf, propLen)
	}

	return bucket.MapSet(key, s.pairPropertyWithFrequency(docID, tf, propLen))
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
		if !prop.HasSearchableIndex || prop.OverlaySearchable {
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
		if !prop.HasSearchableIndex || prop.OverlaySearchable {
			continue
		}

		if err := s.GetPropertyLengthTracker().UnTrackProperty(prop.Name, float32(len(prop.Items))); err != nil {
			return err
		}

	}

	return nil
}

func isMetaCountProperty(property inverted.Property) bool {
	return len(property.Name) > len(schema.InternalMetaCountSuffix) &&
		strings.HasSuffix(property.Name, schema.InternalMetaCountSuffix)
}

func isInternalProperty(property inverted.Property) bool {
	return property.Name[0] == '_'
}
