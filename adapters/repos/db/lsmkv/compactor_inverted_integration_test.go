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

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

func NewMapPairFromDocIdAndTf(docId uint64, tf float32, propLength float32, isTombstone bool) MapPair {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, docId)

	value := make([]byte, 8)
	binary.LittleEndian.PutUint32(value[0:4], math.Float32bits(tf))
	binary.LittleEndian.PutUint32(value[4:8], math.Float32bits(propLength))

	return MapPair{
		Key:       key,
		Value:     value,
		Tombstone: isTombstone,
	}
}

func (kv MapPair) UpdateTf(tf float32, propLength float32) {
	kv.Value = make([]byte, 8)
	binary.LittleEndian.PutUint32(kv.Value[0:4], math.Float32bits(tf))
	binary.LittleEndian.PutUint32(kv.Value[4:8], math.Float32bits(propLength))
}

type kv struct {
	key    []byte
	values []MapPair
}

func validateMapPairListVsBlockMaxSearch(ctx context.Context, bucket *Bucket, expectedMultiKey []kv) error {
	for _, termPair := range expectedMultiKey {
		expected := termPair.values
		mapKey := termPair.key
		// get more results, as there may be more results than expected on the result heap
		// during intermediate steps of insertions
		N := len(expected) * 10
		bm25config := schema.BM25Config{
			K1: 1.2,
			B:  0.75,
		}
		avgPropLen := 1.0
		queries := []string{string(mapKey)}
		duplicateTextBoosts := make([]int, 1)
		diskTerms, _, release, err := bucket.CreateDiskTerm(float64(N), nil, queries, "", 1, duplicateTextBoosts, bm25config, ctx)

		defer func() {
			release()
		}()

		expectedSet := make(map[uint64][]*terms.DocPointerWithScore, len(expected))
		for _, diskTerm := range diskTerms {
			topKHeap := DoBlockMaxWand(N, diskTerm, avgPropLen, true, 1, 1)
			if err != nil {
				return fmt.Errorf("failed to create disk term: %w", err)
			}
			for topKHeap.Len() > 0 {
				item := topKHeap.Pop()
				expectedSet[item.ID] = item.Value
			}
		}

		for _, val := range expected {
			docId := binary.BigEndian.Uint64(val.Key)
			if val.Tombstone {
				continue
			}
			freq := math.Float32frombits(binary.LittleEndian.Uint32(val.Value[0:4]))
			if _, ok := expectedSet[docId]; !ok {
				return fmt.Errorf("expected docId %v not found in topKHeap: %v", docId, expectedSet)
			}
			if expectedSet[docId][0].Frequency != freq {
				return fmt.Errorf("expected frequency %v but got %v", freq, expectedSet[docId][0].Frequency)
			}

		}
	}

	return nil
}

func createTerm(bucket *Bucket, N float64, filterDocIds helpers.AllowList, query string, queryTermIndex int, propertyBoost float32, duplicateTextBoost int, ctx context.Context, bm25Config schema.BM25Config, logger logrus.FieldLogger) (*terms.Term, error) {
	termResult := terms.NewTerm(query, queryTermIndex, float32(1.0), bm25Config)

	allMsAndProps := make([][]terms.DocPointerWithScore, 1)

	m, err := bucket.DocPointerWithScoreList(ctx, []byte(query), 1)
	if err != nil {
		return nil, err
	}

	allMsAndProps[0] = m

	largestN := 0
	// remove empty results from allMsAndProps
	nonEmptyMsAndProps := make([][]terms.DocPointerWithScore, 0, len(allMsAndProps))
	for _, m := range allMsAndProps {
		if len(m) > 0 {
			nonEmptyMsAndProps = append(nonEmptyMsAndProps, m)
		}
		if len(m) > largestN {
			largestN = len(m)
		}
	}
	allMsAndProps = nonEmptyMsAndProps

	if len(nonEmptyMsAndProps) == 0 {
		return nil, nil
	}

	if len(nonEmptyMsAndProps) == 1 {
		termResult.Data = allMsAndProps[0]
		n := float64(len(termResult.Data))
		termResult.SetIdf(math.Log(float64(1)+(N-float64(n)+0.5)/(float64(n)+0.5)) * float64(duplicateTextBoost))
		termResult.SetPosPointer(0)
		termResult.SetIdPointer(termResult.Data[0].Id)
		return termResult, nil
	}
	indices := make([]int, len(allMsAndProps))
	var docMapPairs []terms.DocPointerWithScore = nil

	// The indices are needed to combining the results of different properties
	// They were previously used to keep track of additional explanations TF and prop len,
	// but this is now done when adding terms to the heap in the getTopKHeap function
	var docMapPairsIndices map[uint64]int = nil
	for {
		i := -1
		minId := uint64(0)
		for ti, mAndProps := range allMsAndProps {
			if indices[ti] >= len(mAndProps) {
				continue
			}
			ki := mAndProps[indices[ti]].Id
			if i == -1 || ki < minId {
				i = ti
				minId = ki
			}
		}

		if i == -1 {
			break
		}

		m := allMsAndProps[i]
		k := indices[i]
		val := m[indices[i]]

		indices[i]++

		// only create maps/slices if we know how many entries there are
		if docMapPairs == nil {
			docMapPairs = make([]terms.DocPointerWithScore, 0, largestN)
			docMapPairsIndices = make(map[uint64]int, largestN)

			docMapPairs = append(docMapPairs, val)
			docMapPairsIndices[val.Id] = k
		} else {
			key := val.Id
			ind, ok := docMapPairsIndices[key]
			if ok {
				if ind >= len(docMapPairs) {
					// the index is not valid anymore, but the key is still in the map
					logger.Warnf("Skipping pair in BM25: Index %d is out of range for key %d, length %d.", ind, key, len(docMapPairs))
					continue
				}
				if ind < len(docMapPairs) && docMapPairs[ind].Id != key {
					logger.Warnf("Skipping pair in BM25: id at %d in doc map pairs, %d, differs from current key, %d", ind, docMapPairs[ind].Id, key)
					continue
				}

				docMapPairs[ind].PropLength += val.PropLength
				docMapPairs[ind].Frequency += val.Frequency
			} else {
				docMapPairs = append(docMapPairs, val)
				docMapPairsIndices[val.Id] = len(docMapPairs) - 1 // current last entry
			}

		}
	}
	if docMapPairs == nil {
		return nil, nil
	}
	termResult.Data = docMapPairs

	n := float64(len(docMapPairs))
	termResult.SetIdf(math.Log(float64(1)+(N-n+0.5)/(n+0.5)) * float64(duplicateTextBoost))

	// catch special case where there are no results and would panic termResult.data[0].id
	// related to #4125
	if len(termResult.Data) == 0 {
		return nil, nil
	}

	termResult.SetPosPointer(0)
	termResult.SetIdPointer(termResult.Data[0].Id)
	return termResult, nil
}

func validateMapPairListVsWandSearch(ctx context.Context, bucket *Bucket, expectedMultiKey []kv) error {
	for _, termPair := range expectedMultiKey {
		expected := termPair.values
		mapKey := termPair.key
		// get more results, as there may be more results than expected on the result heap
		// during intermediate steps of insertions
		N := len(expected) * 10
		bm25config := schema.BM25Config{
			K1: 1.2,
			B:  0.75,
		}
		avgPropLen := 1.0
		duplicateTextBoosts := make(map[string]float32)
		duplicateTextBoosts[string(mapKey)] = 1.0
		term, err := createTerm(bucket, float64(N), nil, string(mapKey), 0, 1.0, 1, ctx, bm25config, bucket.logger)

		countNonTombstones := 0
		for _, val := range expected {
			if !val.Tombstone {
				countNonTombstones++
			}
		}

		// nothing but tombstones and nothing retrieved
		if term == nil && err == nil && countNonTombstones == 0 {
			continue
		} else if term == nil && err == nil {
			return fmt.Errorf("expected term to be non-nil")
		} else if err != nil {
			return fmt.Errorf("failed to create term: %w", err)
		}

		expectedSet := make(map[uint64][]*terms.DocPointerWithScore, len(expected))
		terms := &terms.Terms{
			T:     []terms.TermInterface{term},
			Count: 1,
		}

		topKHeap := DoWand(N, terms, avgPropLen, true, 1)

		for topKHeap.Len() > 0 {
			item := topKHeap.Pop()
			expectedSet[item.ID] = item.Value
		}

		for _, val := range expected {
			docId := binary.BigEndian.Uint64(val.Key)
			if val.Tombstone {
				continue
			}
			freq := math.Float32frombits(binary.LittleEndian.Uint32(val.Value[0:4]))
			if _, ok := expectedSet[docId]; !ok {
				return fmt.Errorf("expected docId %v not found in topKHeap: %v", docId, expectedSet)
			}
			if expectedSet[docId][0].Frequency != freq {
				return fmt.Errorf("expected frequency %v but got %v", freq, expectedSet[docId][0].Frequency)
			}

		}
	}

	return nil
}

// compare expected and actual, but actual can have more keys than expected
func partialCompare(expected, actual kv) error {
	a := 0
	for _, e := range expected.values {
		if e.Tombstone {
			continue
		}
		// if the key in actual is smaller than the key in expected, move to the next key in actual
		for bytes.Compare(actual.values[a].Key, e.Key) < 0 {
			a++
			if a >= len(actual.values) {
				docId := binary.BigEndian.Uint64(e.Key)
				return fmt.Errorf("expected key %v (docId: %v) not found in actual values", e.Key, docId)
			}
		}

		if !bytes.Equal(actual.values[a].Key, e.Key) {
			docId := binary.BigEndian.Uint64(e.Key)
			return fmt.Errorf("expected key %v (docId: %v) not found in actual values", e.Key, docId)
		}
		if !bytes.Equal(actual.values[a].Value, e.Value) {
			return fmt.Errorf("expected value %v, got %v", e.Value, actual.values[a].Value)
		}
	}

	return nil
}

func compactionInvertedStrategy(ctx context.Context, t *testing.T, opts []BucketOption,
	expectedMinSize, expectedMaxSize int64,
) {
	size := 100

	addedDocIds := make(map[uint64]struct{})
	removedDocIds := make(map[uint64]struct{})

	// this segment is not part of the merge, but might still play a role in
	// overall results. For example if one of the later segments has a tombstone
	// for it
	var previous1 []kv
	var previous2 []kv

	var segment1 []kv
	var segment2 []kv
	var expected []kv
	var bucket *Bucket

	dirName := t.TempDir()

	t.Run("create test data", func(t *testing.T) {
		// The test data is split into 4 scenarios evenly:
		//
		// 0.) created in the first segment, never touched again
		// 1.) created in the first segment, appended to it in the second
		// 2.) created in the first segment, first element updated in the second
		// 3.) created in the first segment, second element updated in the second
		// 4.) created in the first segment, first element deleted in the second
		// 5.) created in the first segment, second element deleted in the second
		// 6.) not present in the first segment, created in the second
		// 7.) present in an unrelated previous segment, deleted in the first
		// 8.) present in an unrelated previous segment, deleted in the second
		// 9.) present in an unrelated previous segment, never touched again
		for i := 0; i < size; i++ {
			rowKey := []byte(fmt.Sprintf("row-%03d", i))

			docId1 := uint64(i)
			docId2 := uint64(i + 10000)

			pair1 := NewMapPairFromDocIdAndTf(docId1, float32(i+1), float32(i+2), false)
			pair2 := NewMapPairFromDocIdAndTf(docId2, float32(i+1), float32(i+2), false)

			pairs := []MapPair{pair1, pair2}

			switch i % 10 {
			case 0:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs[:1],
				})

				addedDocIds[docId1] = struct{}{}
				// leave this element untouched in the second segment
				expected = append(expected, kv{
					key:    rowKey,
					values: pairs[:1],
				})
			case 1:
				// add to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs[:1],
				})

				// add extra pair in the second segment
				segment2 = append(segment2, kv{
					key:    rowKey,
					values: pairs[1:2],
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

				expected = append(expected, kv{
					key:    rowKey,
					values: pairs,
				})
			case 2:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// update first key in the second segment
				updated := pair1
				updated.UpdateTf(float32(i*1000), float32(i*1000))

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair2, updated},
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

			case 3:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// update first key in the second segment
				updated := pair2
				updated.UpdateTf(float32(i*1000), float32(i*1000))

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair1, updated},
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

			case 4:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// delete first key in the second segment
				updated := pair1
				updated.Value = nil
				updated.Tombstone = true

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair2},
				})

				removedDocIds[docId1] = struct{}{}
			case 5:
				// add both to segment 1
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: pairs,
				})

				// delete second key in the second segment
				updated := pair2
				updated.Value = nil
				updated.Tombstone = true

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{updated},
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: []MapPair{pair1},
				})

				removedDocIds[docId2] = struct{}{}

			case 6:
				// do not add to segment 2

				// only add to segment 2 (first entry)
				segment2 = append(segment2, kv{
					key:    rowKey,
					values: pairs,
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: pairs,
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}

			case 7:
				// only part of a previous segment, which is not part of the merge
				previous1 = append(previous1, kv{
					key:    rowKey,
					values: pairs[:1],
				})
				previous2 = append(previous2, kv{
					key:    rowKey,
					values: pairs[1:],
				})

				// delete in segment 1
				deleted1 := pair1
				deleted1.Value = nil
				deleted1.Tombstone = true

				deleted2 := pair2
				deleted2.Value = nil
				deleted2.Tombstone = true

				segment1 = append(segment1, kv{
					key:    rowKey,
					values: []MapPair{deleted1},
				})
				segment1 = append(segment1, kv{
					key:    rowKey,
					values: []MapPair{deleted2},
				})

				removedDocIds[docId1] = struct{}{}
				removedDocIds[docId2] = struct{}{}

				// should not have any values in expected at all, not even a key

			case 8:
				// only part of a previous segment, which is not part of the merge
				previous1 = append(previous1, kv{
					key:    rowKey,
					values: pairs[:1],
				})
				previous2 = append(previous2, kv{
					key:    rowKey,
					values: pairs[1:],
				})

				// delete in segment 1
				deleted1 := pair1
				deleted1.Value = nil
				deleted1.Tombstone = true

				deleted2 := pair2
				deleted2.Value = nil
				deleted2.Tombstone = true

				removedDocIds[docId1] = struct{}{}
				removedDocIds[docId2] = struct{}{}

				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{deleted1},
				})
				segment2 = append(segment2, kv{
					key:    rowKey,
					values: []MapPair{deleted2},
				})

				// should not have any values in expected at all, not even a key

			case 9:
				// only part of a previous segment
				previous1 = append(previous1, kv{
					key:    rowKey,
					values: pairs[:1],
				})
				previous2 = append(previous2, kv{
					key:    rowKey,
					values: pairs[1:],
				})

				expected = append(expected, kv{
					key:    rowKey,
					values: pairs,
				})

				addedDocIds[docId1] = struct{}{}
				addedDocIds[docId2] = struct{}{}
			}
		}
	})

	t.Run("shuffle the import order for each segment", func(t *testing.T) {
		// this is to make sure we don't accidentally rely on the import order
		rand.Shuffle(len(segment1), func(i, j int) {
			segment1[i], segment1[j] = segment1[j], segment1[i]
		})
		rand.Shuffle(len(segment2), func(i, j int) {
			segment2[i], segment2[j] = segment2[j], segment2[i]
		})
	})

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("import previous1 segments", func(t *testing.T) {
		for _, kvs := range previous1 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, pair)

				require.Nil(t, err)
			}
		}
	})

	t.Run("verify previous1 before flush", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()
		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			if len(kvs) > 0 {
				retrieved = append(retrieved, kv{
					key:    k,
					values: kvs,
				})
			}

		}
		for i := range previous1 {
			assert.Nil(t, partialCompare(previous1[i], retrieved[i]))
		}
		assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, previous1))
		assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, previous1))
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("import previous2 segments", func(t *testing.T) {
		for _, kvs := range previous2 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, pair)
				require.Nil(t, err)
			}
		}
	})

	t.Run("verify previous2 before flush", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()
		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			if len(kvs) > 0 {
				retrieved = append(retrieved, kv{
					key:    k,
					values: kvs,
				})
			}

		}
		for i := range previous2 {
			assert.Nil(t, partialCompare(previous2[i], retrieved[i]))
		}
		assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, previous2))
		assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, previous2))
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("import segment 1", func(t *testing.T) {
		for _, kvs := range segment1 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, pair)
				require.Nil(t, err)
			}
		}
	})

	t.Run("verify segment1 before flush", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()
		i := 0
		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			if len(kvs) > 0 {
				retrieved = append(retrieved, kv{
					key:    k,
					values: kvs,
				})
			}
			if bytes.Equal(segment1[i].key, k) {
				assert.Nil(t, partialCompare(segment1[i], retrieved[len(retrieved)-1]))
				i++
			}
		}
		assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, segment1))
		assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, segment1))
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("verify segment1 after flush", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()
		i := 0
		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			if len(kvs) > 0 {
				retrieved = append(retrieved, kv{
					key:    k,
					values: kvs,
				})
			}
			if bytes.Equal(segment1[i].key, k) {
				assert.Nil(t, partialCompare(segment1[i], retrieved[len(retrieved)-1]))
				i++
			}
		}
		assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, segment1))
		assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, segment1))
	})

	t.Run("import segment 2", func(t *testing.T) {
		for _, kvs := range segment2 {
			for _, pair := range kvs.values {
				err := bucket.MapSet(kvs.key, pair)
				require.Nil(t, err)
			}
		}
	})

	t.Run("verify segment2 before flush", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()
		i := 0
		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			if len(kvs) > 0 {
				retrieved = append(retrieved, kv{
					key:    k,
					values: kvs,
				})
				if bytes.Equal(segment2[i].key, k) {
					assert.Nil(t, partialCompare(segment2[i], retrieved[len(retrieved)-1]))
					i++
				}
			}

		}
		assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, segment2))
		assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, segment2))
	})

	t.Run("flush to disk", func(t *testing.T) {
		require.Nil(t, bucket.FlushAndSwitch())
	})

	t.Run("verify segment2 after flush", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()
		i := 0
		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			if len(kvs) > 0 {
				retrieved = append(retrieved, kv{
					key:    k,
					values: kvs,
				})
				if bytes.Equal(segment2[i].key, k) {
					assert.Nil(t, partialCompare(segment2[i], retrieved[len(retrieved)-1]))
					i++
				}
			}

		}
		assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, segment2))
		assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, segment2))
	})

	t.Run("within control make sure map keys are sorted", func(t *testing.T) {
		for i := range expected {
			sort.Slice(expected[i].values, func(a, b int) bool {
				return bytes.Compare(expected[i].values[a].Key, expected[i].values[b].Key) < 0
			})
		}
	})

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()
		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			if len(kvs) > 0 {
				retrieved = append(retrieved, kv{
					key:    k,
					values: kvs,
				})
			}

		}
		for i := range expected {
			assert.Nil(t, partialCompare(expected[i], retrieved[i]))
		}
		assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, expected))
		assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, expected))
	})

	t.Run("compact until no longer eligible", func(t *testing.T) {
		i := 0
		var compacted bool
		var err error
		for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
			i++
			t.Run("verify control during compaction", func(t *testing.T) {
				var retrieved []kv

				c := bucket.MapCursor()
				defer c.Close()

				for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

					kvs, err := bucket.MapList(ctx, k)

					assert.Nil(t, err)

					if len(kvs) > 0 {
						retrieved = append(retrieved, kv{
							key:    k,
							values: kvs,
						})
					}
				}

				assert.Equal(t, expected, retrieved)
				assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, expected))
				assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, expected))
			})
		}
		require.Nil(t, err)
	})

	t.Run("verify control after compaction using a cursor", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()

		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			if len(kvs) > 0 {
				retrieved = append(retrieved, kv{
					key:    k,
					values: kvs,
				})
			}
		}

		assert.Equal(t, expected, retrieved)
		assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, expected))
		assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, expected))
		assertSingleSegmentOfSize(t, bucket, expectedMinSize, expectedMaxSize)
	})

	t.Run("verify control using individual get (MapList) operations",
		func(t *testing.T) {
			// Previously the only verification was done using the cursor. That
			// guaranteed that all pairs are present in the payload, but it did not
			// guarantee the integrity of the index (DiskTree) which is used to access
			// _individual_ keys. Corrupting this index is exactly what happened in
			// https://github.com/weaviate/weaviate/issues/3517
			for _, pair := range expected {
				kvs, err := bucket.MapList(ctx, pair.key)
				require.NoError(t, err)

				assert.Equal(t, pair.values, kvs)
				assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, []kv{pair}))
				assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, []kv{pair}))
			}
		})
}

func compactionInvertedStrategy_RemoveUnnecessary(ctx context.Context, t *testing.T, opts []BucketOption) {
	// in this test each segment reverses the action of the previous segment so
	// that in the end a lot of information is present in the individual segments
	// which is no longer needed. We then verify that after all compaction this
	// information is gone, thus freeing up disk space
	size := 100

	key := []byte("my-key")

	var bucket *Bucket
	dirName := t.TempDir()

	t.Run("init bucket", func(t *testing.T) {
		b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyInverted))
		require.Nil(t, err)

		// so big it effectively never triggers as part of this test
		b.SetMemtableThreshold(1e9)

		bucket = b
	})

	t.Run("write segments", func(t *testing.T) {
		for i := 0; i < size; i++ {
			if i != 0 {
				// we can only update an existing value if this isn't the first write
				pair := NewMapPairFromDocIdAndTf(uint64(i-1), float32(i), float32(i), false)
				err := bucket.MapSet(key, pair)
				require.Nil(t, err)
			}

			if i > 1 {
				// we can only delete two back an existing value if this isn't the
				// first or second write
				pair := NewMapPairFromDocIdAndTf(uint64(i-2), float32(i), float32(i), true)
				err := bucket.MapSet(key, pair)
				require.Nil(t, err)
			}

			pair := NewMapPairFromDocIdAndTf(uint64(i), float32(i), float32(i), false)
			err := bucket.MapSet(key, pair)
			require.Nil(t, err)
			require.Nil(t, bucket.FlushAndSwitch())
		}
	})

	expectedPair := NewMapPairFromDocIdAndTf(uint64(size-2), float32(size-1), float32(size-1), false)

	expectedPair2 := NewMapPairFromDocIdAndTf(uint64(size-1), float32(size-1), float32(size-1), false)

	expected := []kv{
		{
			key: key,
			values: []MapPair{
				expectedPair,
				expectedPair2,
			},
		},
	}

	t.Run("verify control before compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()

		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			retrieved = append(retrieved, kv{
				key:    k,
				values: kvs,
			})
		}

		for i := range expected {
			assert.Nil(t, partialCompare(expected[i], retrieved[i]))
		}
		assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, expected))
		assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, expected))
	})

	t.Run("compact until no longer eligible", func(t *testing.T) {
		var compacted bool
		var err error
		for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
			t.Run("verify control during compaction", func(t *testing.T) {
				var retrieved []kv

				c := bucket.MapCursor()
				defer c.Close()

				for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {

					kvs, err := bucket.MapList(ctx, k)

					assert.Nil(t, err)

					if len(kvs) > 0 {
						retrieved = append(retrieved, kv{
							key:    k,
							values: kvs,
						})
					}
				}

				assert.Equal(t, expected, retrieved)
				assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, expected))
				assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, expected))
			})
		}
		require.Nil(t, err)
	})

	t.Run("verify control after compaction", func(t *testing.T) {
		var retrieved []kv

		c := bucket.MapCursor()
		defer c.Close()

		for k, _ := c.First(ctx); k != nil; k, _ = c.Next(ctx) {
			kvs, err := bucket.MapList(ctx, k)

			assert.Nil(t, err)

			retrieved = append(retrieved, kv{
				key:    k,
				values: kvs,
			})
		}

		for i := range expected {
			assert.Nil(t, partialCompare(expected[i], retrieved[i]))
		}
		assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, expected))
		assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, expected))
	})

	t.Run("verify control using individual get (MapList) operations",
		func(t *testing.T) {
			// Previously the only verification was done using the cursor. That
			// guaranteed that all pairs are present in the payload, but it did not
			// guarantee the integrity of the index (DiskTree) which is used to access
			// _individual_ keys. Corrupting this index is exactly what happened in
			// https://github.com/weaviate/weaviate/issues/3517
			for _, pair := range expected {
				kvs, err := bucket.MapList(ctx, pair.key)
				require.NoError(t, err)

				assert.Nil(t, partialCompare(pair, kv{
					key:    pair.key,
					values: kvs,
				}))
				assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, []kv{pair}))
				assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, []kv{pair}))
			}
		})
}

func compactionInvertedStrategy_FrequentPutDeleteOperations(ctx context.Context, t *testing.T, opts []BucketOption) {
	// In this test we are testing that the compaction works well for map collection
	maxSize := 10

	key := []byte("my-key")
	mapKey := make([]byte, 8)
	binary.BigEndian.PutUint64(mapKey, 0)

	for size := 4; size < maxSize; size++ {
		t.Run(fmt.Sprintf("compact %v segments", size), func(t *testing.T) {
			var bucket *Bucket
			dirName := t.TempDir()

			t.Run("init bucket", func(t *testing.T) {
				b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
				require.Nil(t, err)

				// so big it effectively never triggers as part of this test
				b.SetMemtableThreshold(1e9)

				bucket = b
			})

			t.Run("write segments", func(t *testing.T) {
				for i := 0; i < size; i++ {
					pair := NewMapPairFromDocIdAndTf(0, float32(i), float32(i), false)

					err := bucket.MapSet(key, pair)
					require.Nil(t, err)

					if size == 5 || size == 6 {
						// delete all
						err = bucket.MapDeleteKey(key, mapKey)
						require.Nil(t, err)
					} else if i != size-1 {
						// don't delete at the end
						err := bucket.MapDeleteKey(key, mapKey)
						require.Nil(t, err)
					}

					t.Run("check entries before flush", func(t *testing.T) {
						pair2 := kv{
							key:    key,
							values: []MapPair{pair},
						}

						res, err := bucket.MapList(ctx, key)

						if size == 5 || size == 6 || i != size-1 {
							assert.Empty(t, res)
							pair2.values = []MapPair{}
							assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, []kv{pair2}))
							assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, []kv{pair2}))
						} else {
							assert.Len(t, res, 1)

							assert.Nil(t, err)
							assert.Nil(t, partialCompare(pair2, kv{
								key:    key,
								values: res,
							}))
							assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, []kv{pair2}))
							assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, []kv{pair2}))
						}
					})

					require.Nil(t, bucket.FlushAndSwitch())
				}
			})

			t.Run("check entries before compaction", func(t *testing.T) {
				res, err := bucket.MapList(ctx, key)
				assert.Nil(t, err)

				pair2 := kv{
					key:    key,
					values: []MapPair{NewMapPairFromDocIdAndTf(0, float32(size-1), float32(size-1), false)},
				}

				if size == 5 || size == 6 {
					assert.Empty(t, res)
					pair2.values = []MapPair{}
					assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, []kv{pair2}))
					assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, []kv{pair2}))
				} else {
					assert.Len(t, res, 1)
					assert.Equal(t, false, res[0].Tombstone)
				}
				pair := kv{
					key:    key,
					values: res,
				}

				assert.Nil(t, partialCompare(pair, kv{
					key:    key,
					values: res,
				}))
				assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, []kv{pair}))
				assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, []kv{pair}))
			})

			t.Run("compact until no longer eligible", func(t *testing.T) {
				var compacted bool
				var err error
				for compacted, err = bucket.disk.compactOnce(); err == nil && compacted; compacted, err = bucket.disk.compactOnce() {
					t.Run("check entries during compaction", func(t *testing.T) {
						res, err := bucket.MapList(ctx, key)
						assert.Nil(t, err)

						pair2 := kv{
							key:    key,
							values: []MapPair{NewMapPairFromDocIdAndTf(0, float32(size-1), float32(size-1), false)},
						}

						if size == 5 || size == 6 {
							assert.Empty(t, res)
							pair2.values = []MapPair{}
							assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, []kv{pair2}))
							assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, []kv{pair2}))
						} else {
							assert.Len(t, res, 1)
							assert.Equal(t, false, res[0].Tombstone)
						}

						pair := kv{
							key:    key,
							values: res,
						}

						assert.Nil(t, partialCompare(pair, kv{
							key:    key,
							values: res,
						}))
						assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, []kv{pair}))
						assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, []kv{pair}))
					})
				}
				require.Nil(t, err)
			})

			t.Run("check entries after compaction", func(t *testing.T) {
				res, err := bucket.MapList(ctx, key)
				assert.Nil(t, err)
				if size == 5 || size == 6 {
					assert.Empty(t, res)
				} else {
					assert.Len(t, res, 1)
					assert.Equal(t, false, res[0].Tombstone)
				}
				pair := kv{
					key:    key,
					values: res,
				}

				assert.Nil(t, partialCompare(pair, kv{
					key:    key,
					values: res,
				}))
				assert.Nil(t, validateMapPairListVsBlockMaxSearch(ctx, bucket, []kv{pair}))
				assert.Nil(t, validateMapPairListVsWandSearch(ctx, bucket, []kv{pair}))
			})
		})
	}
}
