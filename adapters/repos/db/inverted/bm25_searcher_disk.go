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

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
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

func (b *BM25Searcher) wandDiskMem(
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

	// wandDiskTimes[wandTimesId] += float64(time.Now().UnixNano())/1e6 - startTime
	// wandTimesId++
	// startTime = float64(time.Now().UnixNano()) / 1e6

	hasMultipleProperties := len(params.Properties) > 1

	if hasMultipleProperties && !useWandDiskForced {
		b.logger.Debug("BM25 search: multiple properties requested, falling back to memory search")
		return b.wandMemScoring(queryTermsByTokenization, duplicateBoostsByTokenization, propNamesByTokenization, propertyBoosts, averagePropLength, N, filterDocIds, params, limit)
	}

	_, _, _, hasTombstones, _, err := b.store.GetAllSegmentsForTerms(propNamesByTokenization, queryTermsByTokenization)
	if err != nil {
		return nil, nil, err
	}

	if hasTombstones && !useWandDiskForced {
		b.logger.Debug("BM25 search: found tombstones in inverted index, falling back to memory search")
		return b.wandMemScoring(queryTermsByTokenization, duplicateBoostsByTokenization, propNamesByTokenization, propertyBoosts, averagePropLength, N, filterDocIds, params, limit)
	}

	if hasTombstones {
		b.logger.Debug("BM25 search: found tombstones in inverted index, using disk search as useWandDiskForced is set to true")
	} else if hasMultipleProperties {
		b.logger.Debug("BM25 search: multiple properties requested, using disk search as useWandDiskForced is set to true")
	}

	// wandDiskTimes[wandTimesId] += float64(time.Now().UnixNano())/1e6 - startTime
	// wandTimesId++
	// startTime = float64(time.Now().UnixNano()) / 1e6
	return b.wandDiskScoring(queryTermsByTokenization, duplicateBoostsByTokenization, propNamesByTokenization, propertyBoosts, averagePropLength, N, filterDocIds, params, limit)
}

func (b *BM25Searcher) wandDiskScoring(queryTermsByTokenization map[string][]string, duplicateBoostsByTokenization map[string][]int, propNamesByTokenization map[string][]string, propertyBoosts map[string]float32, averagePropLength float64, N float64, filterDocIds helpers.AllowList, params searchparams.KeywordRanking, limit int) ([]*storobj.Object, []float32, error) {
	allSegments, memTables, propertySizes, _, _, _ := b.store.GetAllSegmentsForTerms(propNamesByTokenization, queryTermsByTokenization)

	allObjects := make([][]*storobj.Object, len(allSegments)+len(memTables))
	allScores := make([][]float32, len(allSegments)+len(memTables))

	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU)

	currentBucket := 0
	for segment, propName := range allSegments {
		segment := segment
		propName := propName
		myCurrentBucket := currentBucket
		currentBucket++
		eg.Go(func() (err error) {
			terms := make([]Term, 0, len(queryTermsByTokenization[models.PropertyTokenizationWord]))
			for i, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
				// pass i to the closure
				i := i
				term := term
				duplicateTextBoost := duplicateBoostsByTokenization[models.PropertyTokenizationWord][i]

				singleTerms, err := segment.WandTerm([]byte(term), N, float64(duplicateTextBoost), float64(propertyBoosts[propName]), propertySizes[propName][term], filterDocIds)
				if err == nil {
					terms = append(terms, singleTerms)
				}
			}

			flatTerms := terms
			// wandDiskStats[0] += float64(len(queryTermsByTokenization[models.PropertyTokenizationWord]))

			resultsOriginalOrder := make([]Term, len(flatTerms))
			copy(resultsOriginalOrder, flatTerms)

			topKHeap := b.getTopKHeap(limit, flatTerms, averagePropLength)
			indices := make([]map[uint64]int, 0)
			objects, scores, err := b.getTopKObjects(topKHeap, resultsOriginalOrder, indices, params.AdditionalExplanations)

			allObjects[myCurrentBucket] = objects
			allScores[myCurrentBucket] = scores

			if err != nil {
				return err
			}
			return nil
		})
	}

	for memTable, propName := range memTables {
		memTable := memTable
		myCurrentBucket := currentBucket
		currentBucket++
		eg.Go(func() (err error) {
			terms := make([]Term, 0, len(queryTermsByTokenization[models.PropertyTokenizationWord]))
			for i, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
				// pass i to the closure
				i := i
				term := term
				duplicateTextBoost := duplicateBoostsByTokenization[models.PropertyTokenizationWord][i]
				n := float64(propertySizes[propName][term])
				singleTerms, _, err := memTable.CreateTerm(N, n, filterDocIds, term, propName, propertyBoosts, duplicateTextBoost, params.AdditionalExplanations)
				if err == nil {
					terms = append(terms, singleTerms)
				}
			}

			flatTerms := terms
			// wandDiskStats[0] += float64(len(queryTermsByTokenization[models.PropertyTokenizationWord]))

			resultsOriginalOrder := make([]Term, len(flatTerms))
			copy(resultsOriginalOrder, flatTerms)

			topKHeap := b.getTopKHeap(limit, flatTerms, averagePropLength)
			indices := make([]map[uint64]int, 0)
			objects, scores, err := b.getTopKObjects(topKHeap, resultsOriginalOrder, indices, params.AdditionalExplanations)

			allObjects[myCurrentBucket] = objects
			allScores[myCurrentBucket] = scores

			if err != nil {
				return err
			}
			return nil
		})
	}

	err := eg.Wait()
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
