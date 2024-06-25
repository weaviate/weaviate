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

package inverted

import (
	"context"
	"strings"

	"github.com/weaviate/sroar"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

// global var to store wand times
/*
var (
	wandDiskTimes     []float64 = make([]float64, 5)
	wandDiskCounter   int       = 0
	wandDiskLastClass string    = ""
	wandDiskStats     []float64 = make([]float64, 1)
)
*/

func (b *BM25Searcher) wandDisk(
	ctx context.Context, filterDocIds helpers.AllowList, class *models.Class, params searchparams.KeywordRanking, limit int, useWandDiskForced bool,
) ([]*storobj.Object, []float32, error) {
	/*
		if wandDiskLastClass == "" {
			wandDiskLastClass = string(class.Class)
		}
		if wandDiskLastClass != string(class.Class) {
			fmt.Printf("DISK,%v", wandDiskLastClass)
			sum := 0.
			for i := 0; i < len(wandDiskTimes); i++ {
				fmt.Printf(",%8.2f", wandDiskTimes[i]/float64(wandDiskCounter))
				sum += wandDiskTimes[i] / float64(wandDiskCounter)
			}
			fmt.Printf(",%8.2f", sum)
			//for i := 0; i < len(wandDiskStats); i++ {
			//	fmt.Printf(",%8.2f", wandDiskStats[i]/float64(wandDiskCounter))
			//}
			fmt.Printf("\n")
			wandDiskTimes = make([]float64, len(wandDiskTimes))
			wandDiskStats = make([]float64, len(wandDiskStats))
			wandDiskCounter = 0
			wandDiskLastClass = string(class.Class)
		}

		wandDiskCounter++
		wandTimesId := 0

		// start timer
		startTime := float64(time.Now().UnixNano()) / 1e6
	*/

	N := float64(b.store.Bucket(helpers.ObjectsBucketLSM).Count())

	// stopword filtering for word tokenization
	queryTermsByTokenization, duplicateBoostsByTokenization, propNamesByTokenization, propertyBoosts, averagePropLength, err := b.extractTermInformation(class, params)
	if err != nil {
		return nil, nil, err
	}

	return b.wandDiskScoring(queryTermsByTokenization, duplicateBoostsByTokenization, propNamesByTokenization, propertyBoosts, averagePropLength, N, filterDocIds, params, limit)
}

func (b *BM25Searcher) wandDiskScoring(queryTermsByTokenization map[string][]string, duplicateBoostsByTokenization map[string][]int, propNamesByTokenization map[string][]string, propertyBoosts map[string]float32, averagePropLength float64, N float64, filterDocIds helpers.AllowList, params searchparams.KeywordRanking, limit int) ([]*storobj.Object, []float32, error) {
	memTables, err := b.store.GetMemtablesForTerms(propNamesByTokenization, queryTermsByTokenization)
	if err != nil {
		return nil, nil, err
	}

	segments, err := b.store.GetSegmentsForTerms(propNamesByTokenization, queryTermsByTokenization)
	if err != nil {
		return nil, nil, err
	}

	tombstones := make(map[string][]*sroar.Bitmap, len(propNamesByTokenization))

	resultCount := 0
	for propName, propSegments := range segments {
		tombstones[propName] = make([]*sroar.Bitmap, 0)
		for _, segment := range propSegments {
			segment.CompactionMutex.RLock()
			defer segment.CompactionMutex.RUnlock()
			tombstone, err := segment.GetTombstones()
			if err != nil {
				return nil, nil, err
			}
			tombstones[propName] = append(tombstones[propName], tombstone)
			resultCount += 1
		}
	}
	for propName, propMemTables := range memTables {
		if _, ok := tombstones[propName]; !ok {
			tombstones[propName] = make([]*sroar.Bitmap, 0)
		}
		for _, memTable := range propMemTables {

			tombstone, err := memTable.GetTombstones()
			if err != nil {
				return nil, nil, err
			}
			tombstones[propName] = append(tombstones[propName], tombstone)
			resultCount += 1
		}
	}

	propertySizes, _, err := b.store.GetAllSegmentStats(segments, memTables, propNamesByTokenization, queryTermsByTokenization)
	if err != nil {
		return nil, nil, err
	}
	// segments, memTables, propertySizes, _, _, _ := b.store.GetAllSegmentsForTerms(propNamesByTokenization, queryTermsByTokenization)

	allObjects := make([][]*storobj.Object, resultCount)
	allScores := make([][]float32, resultCount)

	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU)

	currentBucket := 0
	for propName, segments := range segments {
		for segmentIndex, segment := range segments {
			segment := segment
			propName := propName
			myCurrentBucket := currentBucket
			currentBucket++
			eg.Go(func() (err error) {
				if segment.Closing {
					b.logger.Infof("segment closing\n")
					return nil
				}

				allTombstones := sroar.NewBitmap()
				for _, tombstone := range tombstones[propName][segmentIndex+1:] {
					allTombstones.Or(tombstone)
				}

				blockList := helpers.NewAllowListFromBitmap(allTombstones)

				allTerms := make([]terms.Term, 0, len(queryTermsByTokenization[models.PropertyTokenizationWord]))
				for i, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
					// pass i to the closure
					i := i
					term := term
					duplicateTextBoost := duplicateBoostsByTokenization[models.PropertyTokenizationWord][i]

					singleTerms, err := segment.WandTerm([]byte(term), i, N, float64(duplicateTextBoost), float64(propertyBoosts[propName]), propertySizes[propName][term], filterDocIds, blockList)
					if err == nil {
						allTerms = append(allTerms, singleTerms)
					}
				}

				flatTerms := allTerms
				// wandDiskStats[0] += float64(len(queryTermsByTokenization[models.PropertyTokenizationWord]))

				resultsOriginalOrder := make([]terms.Term, len(flatTerms))
				copy(resultsOriginalOrder, flatTerms)

				topKHeap := b.getTopKHeap(limit, flatTerms, averagePropLength, params.AdditionalExplanations)
				objects, scores, err := b.getTopKObjects(topKHeap, resultsOriginalOrder, params.AdditionalExplanations)

				allObjects[myCurrentBucket] = objects
				allScores[myCurrentBucket] = scores

				if err != nil {
					return err
				}
				return nil
			})
		}
	}

	for propName, memTables := range memTables {
		for memTableIndex, memTable := range memTables {
			memTable := memTable
			myCurrentBucket := currentBucket
			currentBucket++
			eg.Go(func() (err error) {
				allTerms := make([]terms.Term, 0, len(queryTermsByTokenization[models.PropertyTokenizationWord]))
				for i, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
					// pass i to the closure
					i := i
					term := term
					duplicateTextBoost := duplicateBoostsByTokenization[models.PropertyTokenizationWord][i]
					n := float64(propertySizes[propName][term])

					blockList := helpers.NewAllowList()
					if len(tombstones[propName]) > memTableIndex+1 {
						blockList = helpers.NewAllowListFromBitmap(tombstones[propName][memTableIndex+1])
					}

					singleTerms, err := memTable.CreateTerm(N, n, filterDocIds, blockList, term, i, propName, propertyBoosts, duplicateTextBoost, params.AdditionalExplanations)
					if err == nil {
						allTerms = append(allTerms, singleTerms)
					}
				}

				flatTerms := allTerms
				// wandDiskStats[0] += float64(len(queryTermsByTokenization[models.PropertyTokenizationWord]))

				resultsOriginalOrder := make([]terms.Term, len(flatTerms))
				copy(resultsOriginalOrder, flatTerms)

				topKHeap := b.getTopKHeap(limit, flatTerms, averagePropLength, params.AdditionalExplanations)
				objects, scores, err := b.getTopKObjects(topKHeap, resultsOriginalOrder, params.AdditionalExplanations)

				allObjects[myCurrentBucket] = objects
				allScores[myCurrentBucket] = scores

				if err != nil {
					return err
				}
				return nil
			})
		}
	}

	err = eg.Wait()
	if err != nil {
		return nil, nil, err
	}

	// merge the results from the different buckets
	objects, scores := b.rankMultiBucket(allObjects, allScores, limit)

	return objects, scores, nil
}

func (b *BM25Searcher) rankMultiBucket(allObjects [][]*storobj.Object, allScores [][]float32, limit int) ([]*storobj.Object, []float32) {
	if len(allObjects) == 1 {
		return allObjects[0], allScores[0]
	}

	// allObjects and allScores are ordered by reverse score already
	// we need to merge them and keep the top K

	// merge allObjects and allScores
	mergedObjects := make([]*storobj.Object, limit)
	mergedScores := make([]float32, limit)
	mergedPos := limit - 1

	bucketPosition := make([]int, len(allObjects))

	for i := range bucketPosition {
		bucketPosition[i] = len(allObjects[i]) - 1
	}

	// iterate by bucket, bet the one with the highest score and add it to the merged list
	for {
		// find the best score
		bestScore := float32(-1)
		bestScoreIndex := -1
		lowestDocID := ""

		for i := range allObjects {
			if bucketPosition[i] >= 0 {
				if allScores[i][bucketPosition[i]] > bestScore {
					bestScore = allScores[i][bucketPosition[i]]
					bestScoreIndex = i
					lowestDocID = allObjects[i][bucketPosition[i]].ID().String()
				} else if allScores[i][bucketPosition[i]] == bestScore {
					uuid2 := allObjects[i][bucketPosition[i]].ID().String()
					res := strings.Compare(uuid2, lowestDocID)
					if res < 0 {
						bestScoreIndex = i
						lowestDocID = uuid2
					}
				}
			}
		}

		// if we found a score, add it to the merged list
		if bestScoreIndex != -1 {
			mergedObjects[mergedPos] = allObjects[bestScoreIndex][bucketPosition[bestScoreIndex]]
			mergedScores[mergedPos] = allScores[bestScoreIndex][bucketPosition[bestScoreIndex]]
			bucketPosition[bestScoreIndex]--
			mergedPos--
		}

		// if we didn't find any score, we are done
		if bestScoreIndex == -1 || mergedPos < 0 {
			break
		}
	}

	// if the merged list is smaller than the limit, we need to remove the empty slots
	if (limit - mergedPos) > 0 {
		mergedObjects = mergedObjects[mergedPos+1:]
		mergedScores = mergedScores[mergedPos+1:]
	}

	return mergedObjects, mergedScores
}

/*
func (b *BM25Searcher) rankMultiBucketWithDuplicates(allObjects [][]*storobj.Object, allScores [][]float32, limit int) ([]*storobj.Object, []float32) {
	if len(allObjects) == 1 {
		return allObjects[0], allScores[0]
	}


	// for i := range allObjects {
	// 	for j := range allObjects[i] {
	// 		fmt.Printf("DISK,%v,%v,%v,%v\n", i, j, allObjects[i][j].ID(), allScores[i][j])
	// 	}
	// }


	// allObjects and allScores are ordered by reverse score already
	// we need to merge them and keep the top K

	// merge allObjects and allScores
	mergedObjects := make([]*storobj.Object, limit)
	mergedScores := make([]float32, limit)
	mergedPos := limit - 1

	bucketPosition := make([]int, len(allObjects))

	for i := range bucketPosition {
		bucketPosition[i] = len(allObjects[i]) - 1
	}
	usedIds := make(map[string]int, len(allObjects))
	usedIdsWho := make(map[string]int, len(allObjects))

	// iterate by bucket, bet the one with the highest score and add it to the merged list
	for {
		// find the best score
		bestScore := float32(-1)
		bestScoreIndex := -1
		lowestDocID := ""
		i := 0
		for i < len(allObjects) {
			if bucketPosition[i] >= 0 {

				// increase the bucket position to avoid infinite loop
				val, ok := usedIds[allObjects[i][bucketPosition[i]].ID().String()]
				if !ok {
					val = mergedPos
				}

				if ok {
					who := usedIdsWho[allObjects[i][bucketPosition[i]].ID().String()]
					if who < i {
						mergedObjects[val] = allObjects[i][bucketPosition[i]]
						mergedScores[val] = allScores[i][bucketPosition[i]]
						usedIdsWho[allObjects[i][bucketPosition[i]].ID().String()] = i
					}
					bucketPosition[i]--
					continue
				}

				if allScores[i][bucketPosition[i]] > bestScore {
					bestScore = allScores[i][bucketPosition[i]]
					bestScoreIndex = i
					lowestDocID = allObjects[i][bucketPosition[i]].ID().String()
				} else if allScores[i][bucketPosition[i]] == bestScore {
					uuid2 := allObjects[i][bucketPosition[i]].ID().String()
					res := strings.Compare(uuid2, lowestDocID)
					if res < 0 {
						bestScoreIndex = i
						lowestDocID = uuid2
					}
				} else if allObjects[i][bucketPosition[i]].ID().String() == lowestDocID {
					bucketPosition[i]--
					bestScoreIndex = i
				}

			}
			i++
		}

		// if we found a score, add it to the merged list
		if bestScoreIndex != -1 && bucketPosition[bestScoreIndex] != -1 {
			mergedObjects[mergedPos] = allObjects[bestScoreIndex][bucketPosition[bestScoreIndex]]
			mergedScores[mergedPos] = allScores[bestScoreIndex][bucketPosition[bestScoreIndex]]
			usedIds[mergedObjects[mergedPos].ID().String()] = mergedPos
			usedIdsWho[mergedObjects[mergedPos].ID().String()] = bestScoreIndex
			bucketPosition[bestScoreIndex]--
			mergedPos--
		}

		// if we didn't find any score, we are done
		if bestScoreIndex == -1 || mergedPos < 0 {
			break
		}
	}

	// if the merged list is smaller than the limit, we need to remove the empty slots
	if (limit - mergedPos) > 0 {
		mergedObjects = mergedObjects[mergedPos+1:]
		mergedScores = mergedScores[mergedPos+1:]
	}

	return mergedObjects, mergedScores
}
*/

// Memtable
