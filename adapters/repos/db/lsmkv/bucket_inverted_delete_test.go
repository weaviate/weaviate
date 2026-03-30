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

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

func validateMapPairListVsBlockMaxSearchFull(ctx context.Context, bucket *Bucket, expectedMultiKey []kv) error {
	view := bucket.GetConsistentView()
	defer view.release()
	return validateMapPairListVsBlockMaxSearchFromViewFull(ctx, bucket, view, expectedMultiKey)
}

func validateMapPairListVsBlockMaxSearchFromViewFull(ctx context.Context, bucket *Bucket, view BucketConsistentView, expectedMultiKey []kv) error {
	// partialCompare := false
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
		duplicateTextBoosts[0] = 1
		diskTerms, _, _, err := bucket.createDiskTermFromCV(ctx, view, float64(N), nil, queries, "", 1, duplicateTextBoosts, bm25config)
		if err != nil {
			return fmt.Errorf("failed to create disk term: %w", err)
		}

		found := make(map[uint64][]*terms.DocPointerWithScore, len(expected))
		for _, diskTerm := range diskTerms {
			topKHeap, err := DoBlockMaxWand(ctx, N, diskTerm, avgPropLen, true, 1, 1, bucket.logger)
			if err != nil {
				return fmt.Errorf("failed to execute DoBlockMaxWand for diskTerm %v: %w", diskTerm, err)
			}
			for topKHeap.Len() > 0 {
				item := topKHeap.Pop()
				found[item.ID] = item.Value
			}
		}
		//err = compareKeywordSearchResultSets(expected, found, partialCompare)
		//if err != nil {
		//	return err
		//}
	}

	return nil
}

func validateMapPairListVsWandSearchFull(ctx context.Context, bucket *Bucket, expectedMultiKey []kv) error {
	// partialCompare := false
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

		found := make(map[uint64][]*terms.DocPointerWithScore, len(expected))
		terms := &terms.Terms{
			T:     []terms.TermInterface{term},
			Count: 1,
		}

		topKHeap := DoWand(ctx, N, terms, avgPropLen, true, 1, logger)

		for topKHeap.Len() > 0 {
			item := topKHeap.Pop()
			found[item.ID] = item.Value
		}

		//err = compareKeywordSearchResultSets(expected, found, partialCompare)
		//if err != nil {
		//	return err
		//}
	}

	return nil
}

func TestTombstonesInverted_Compaction(t *testing.T) {
	// size can be increased to 100000 for longer local tests
	sizes := []int{1000}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			runTombstoneInvertedCompactionTest(t, size)
		})
	}
}

func runTombstoneInvertedCompactionTest(t *testing.T, size int) {
	type testCase struct {
		name                  string
		mutateOnOdd           func(bucket *Bucket, key []byte, i int) error
		bulkDeleteEvery100    func(bucket *Bucket, key []byte, i int) error
		buildExpected         func(key []byte, i int) kv
		sleepDuringValidation bool
	}
	tests := []testCase{
		{
			name: "DeleteAndCompaction",
			mutateOnOdd: func(bucket *Bucket, key []byte, i int) error {
				// delete the prior odd entry
				pair := NewMapPairFromDocIdAndTf(uint64(i-2), float32(1), float32(1), true)
				if err := bucket.MapSet(key, pair); err != nil {
					return err
				}
				return bucket.MapDeleteKey(key, pair.Key)
			},
			bulkDeleteEvery100: func(bucket *Bucket, key []byte, i int) error {
				// delete all even entries in the prior 100
				for j := i - 100; j < i; j += 2 {
					pair := NewMapPairFromDocIdAndTf(uint64(j), float32(1), float32(1), true)
					if err := bucket.MapSet(key, pair); err != nil {
						return err
					}
					if err := bucket.MapDeleteKey(key, pair.Key); err != nil {
						return err
					}
				}
				return nil
			},
			buildExpected: func(key []byte, i int) kv {
				expected := kv{key: key, values: []MapPair{}}
				min := (i / 100 * 100) - 1
				for j := min; j <= i; j++ {
					if (j >= 0 && j%2 == 0) || j == i-1 {
						expected.values = append(expected.values, NewMapPairFromDocIdAndTf(uint64(j), float32(1), float32(1), false))
					}
				}
				return expected
			},
			sleepDuringValidation: true,
		},
		{
			name: "UpdateAndCompaction",
			mutateOnOdd: func(bucket *Bucket, key []byte, i int) error {
				// update the prior odd entry (skip when it would cross a 100-boundary)
				if (i-2)%100 == 99 {
					return nil
				}
				pair := NewMapPairFromDocIdAndTf(uint64(i-2), float32(2), float32(2), false)
				if err := bucket.MapDeleteKey(key, pair.Key); err != nil {
					return err
				}
				return bucket.MapSet(key, pair)
			},
			bulkDeleteEvery100: func(bucket *Bucket, key []byte, i int) error {
				// delete all entries in the prior 100
				for j := i - 100; j < i; j++ {
					pair := NewMapPairFromDocIdAndTf(uint64(j), float32(1), float32(1), true)
					if err := bucket.MapSet(key, pair); err != nil {
						return err
					}
					if err := bucket.MapDeleteKey(key, pair.Key); err != nil {
						return err
					}
				}
				return nil
			},
			buildExpected: func(key []byte, i int) kv {
				expected := kv{key: key, values: []MapPair{}}
				min := (i / 100 * 100)
				for j := min; j <= i; j++ {
					if (j >= 0) || j == i-1 {
						if j%2 == 0 || j == i-1 {
							expected.values = append(expected.values, NewMapPairFromDocIdAndTf(uint64(j), float32(1), float32(1), false))
						} else {
							expected.values = append(expected.values, NewMapPairFromDocIdAndTf(uint64(j), float32(2), float32(2), false))
						}
					}
				}
				return expected
			},
			sleepDuringValidation: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			key := []byte("my-key")

			var bucket *Bucket
			dirName := t.TempDir()
			allErrors := map[string]struct{}{}

			t.Run("init bucket", func(t *testing.T) {
				b, err := NewBucketCreator().NewBucket(ctx, dirName, dirName, nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyInverted))
				require.Nil(t, err)

				// so big it effectively never triggers as part of this test
				b.SetMemtableThreshold(1e9)
				bucket = b
			})

			eg := enterrors.NewErrorGroupWrapper(nullLogger())
			eg.SetLimit(3)
			done := make(chan struct{})

			t.Run("write segments", func(t *testing.T) {
				eg.Go(func() error {
					defer close(done)
					lastTime := time.Now()
					errorCount := 0
					for i := range size {
						pair := NewMapPairFromDocIdAndTf(uint64(i), float32(1), float32(1), false)
						err := bucket.MapSet(key, pair)
						require.Nil(t, err)

						if i%2 != 0 && i > 1 {
							err := tc.mutateOnOdd(bucket, key, i)
							require.Nil(t, err)
						}

						if i%100 == 0 && i >= 100 {
							err := tc.bulkDeleteEvery100(bucket, key, i)
							require.Nil(t, err)
							view := bucket.GetConsistentView()
							t.Logf("verifying at entry %d/%d %d  %s", i, size, len(view.Disk), time.Since(lastTime))
							lastTime = time.Now()
							view.release()
						}

						if i != 0 && i%10 == 0 {
							expected := tc.buildExpected(key, i)

							err1 := validateMapPairListVsBlockMaxSearchFull(ctx, bucket, []kv{expected})
							err2 := validateMapPairListVsWandSearchFull(ctx, bucket, []kv{expected})

							kvs, err := bucket.MapList(ctx, key)
							require.NoError(t, err)

							check1 := assert.ObjectsAreEqual(expected, kv{
								key:    key,
								values: kvs,
							})

							if !check1 || err1 != nil || err2 != nil {
								errorCount++

								errKey := fmt.Sprintf("MapList:%v|BMW:%v|WAND:%v", check1, err1, err2)
								if _, exists := allErrors[errKey]; !exists {
									allErrors[errKey] = struct{}{}
								} else {
									continue
								}

								t.Logf("validation failed at %d/%d\n   - MapList: %v\n   - BMW: %v\n   - WAND %v", i, size, check1, err1, err2)
								if tomb, err := bucket.active.ReadOnlyTombstones(); err == nil {
									tombA := tomb.ToArray()
									t.Logf("active tombstones %v", tombA)
								}
								if bucket.flushing != nil {
									if tomb, err := bucket.flushing.ReadOnlyTombstones(); err == nil {
										tombF := tomb.ToArray()
										t.Logf("flushing tombstones %v", tombF)
									}
								}
								docIds := make([]uint64, 0)
								if err1 != nil && strings.Contains(err1.Error(), "[") && strings.Contains(err1.Error(), "]") {
									t.Logf("%s\n", strings.Split(strings.Split(strings.Split(err1.Error(), "[")[1], "]")[0], " "))
									for id := range strings.SplitSeq(strings.Split(strings.Split(err1.Error(), "[")[1], "]")[0], " ") {
										if id == "" {
											continue
										}
										docId := uint64(0)
										fmt.Sscanf(id, "%d", &docId)
										docIds = append(docIds, docId)
									}
								}
								view := bucket.GetConsistentView()
								t.Logf("segment count %d\n", len(view.Disk))
								for k := len(view.Disk) - 1; k >= 1; k-- {
									if tomb, err := view.Disk[k].ReadOnlyTombstones(); err == nil {
										pl, err := view.Disk[k-1].getPropertyLengths()
										require.Nil(t, err)
										for _, docId := range docIds {
											if !tomb.Contains(docId) {
												if _, ok := pl[docId]; ok {
													t.Logf("segment %d %d (%s) doesn't contain tombstone for docId %d", k, view.Disk[k].getLevel(), strings.Split(view.Disk[k].getPath(), "/")[len(strings.Split(view.Disk[k].getPath(), "/"))-1], docId)
													break
												}
											}
										}
									}
								}
								view.release()
							} else {
								errorCount = 0
							}
						}
					}

					for errKey := range allErrors {
						t.Logf("encountered error: %s", errKey)
					}
					if len(allErrors) > 0 {
						return fmt.Errorf("encountered %d unique validation errors", len(allErrors))
					}
					return nil
				})

				eg.Go(func() error {
					k := 0
					for {
						select {
						case <-done:
							return nil
						default:
							require.Nil(t, bucket.FlushAndSwitch())
							k++
						}
					}
				})

				eg.Go(func() error {
					k := 0
					for {
						select {
						case <-done:
							return nil
						default:
							ok, err := bucket.disk.compactOnce()
							require.Nil(t, err)
							if ok {
								k++
							}
						}
					}
				})
			})

			eg.Wait()
		})
	}
}
