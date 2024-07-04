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
	"sort"
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
	propertySegCount := make(map[string]int, len(propNamesByTokenization))
	propertyMemCount := make(map[string]int, len(propNamesByTokenization))

	resultCount := 0
	for propName, propSegments := range segments {
		tombstones[propName] = make([]*sroar.Bitmap, 0)
		propertySegCount[propName] = len(propSegments)
		for _, segmentTok := range propSegments {
			segmentTok.Segment.CompactionMutex.RLock()
			defer segmentTok.Segment.CompactionMutex.RUnlock()
			tombstone, err := segmentTok.Segment.GetTombstones()
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
		propertyMemCount[propName] = len(propMemTables)

		for _, memTable := range propMemTables {

			tombstone, err := memTable.Memtable.GetTombstones()
			if err != nil {
				return nil, nil, err
			}
			tombstones[propName] = append(tombstones[propName], tombstone)
			resultCount += 1
		}
	}

	allObjects := make(map[string][][]*storobj.Object, len(tombstones))
	allScores := make(map[string][][]float32, len(tombstones))

	// pre-allocation of the slices to get the results and in the correct order
	for propName := range tombstones {
		count := 0
		if _, ok := propertySegCount[propName]; ok {
			count += propertySegCount[propName]
		}
		if _, ok := propertyMemCount[propName]; ok {
			count += propertyMemCount[propName]
		}
		allObjects[propName] = make([][]*storobj.Object, count)
		allScores[propName] = make([][]float32, count)
	}

	propertySizes, _, err := b.store.GetAllSegmentStats(segments, memTables, propNamesByTokenization, queryTermsByTokenization)
	if err != nil {
		return nil, nil, err
	}
	// segments, memTables, propertySizes, _, _, _ := b.store.GetAllSegmentsForTerms(propNamesByTokenization, queryTermsByTokenization)

	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU)

	currentBucket := 0
	for propName, segments := range segments {
		for segmentIndex, segmentAndTokenization := range segments {
			segment := segmentAndTokenization.Segment
			tokenization := segmentAndTokenization.Tokenization
			propName := propName
			// myCurrentBucket := currentBucket
			currentBucket++
			eg.Go(func() (err error) {
				propName := propName
				tokenization := tokenization
				segmentIndex := segmentIndex

				if segment.Closing {
					b.logger.Infof("segment closing\n")
					return nil
				}

				allTombstones := sroar.NewBitmap()
				for _, tombstone := range tombstones[propName][segmentIndex+1:] {
					allTombstones.Or(tombstone)
				}

				blockList := helpers.NewAllowListFromBitmap(allTombstones)

				allTerms := make([]terms.Term, 0, len(queryTermsByTokenization[tokenization]))
				for i, term := range queryTermsByTokenization[tokenization] {
					// pass i to the closure
					i := i
					term := term
					duplicateTextBoost := duplicateBoostsByTokenization[tokenization][i]

					singleTerms, err := segment.WandTerm([]byte(term), i, N, float64(duplicateTextBoost), float64(propertyBoosts[propName]), propertySizes[propName][term], filterDocIds, blockList)
					if err == nil {
						allTerms = append(allTerms, singleTerms)
					}
				}

				flatTerms := allTerms
				// wandDiskStats[0] += float64(len(queryTermsByTokenization[tokenization]))

				resultsOriginalOrder := make([]terms.Term, len(flatTerms))
				copy(resultsOriginalOrder, flatTerms)

				topKHeap := b.getTopKHeap(limit, flatTerms, averagePropLength, params.AdditionalExplanations)
				objects, scores, err := b.getTopKObjects(topKHeap, resultsOriginalOrder, params.AdditionalExplanations)

				allObjects[propName][segmentIndex] = objects
				allScores[propName][segmentIndex] = scores

				if err != nil {
					return err
				}
				return nil
			})
		}
	}

	for propName, memTables := range memTables {
		for memTableIndex, memtableAndTokenization := range memTables {
			propName := propName
			memTable := memtableAndTokenization.Memtable
			tokenization := memtableAndTokenization.Tokenization
			memTableIndex += len(segments[propName])
			// myCurrentBucket := currentBucket
			currentBucket++
			eg.Go(func() (err error) {
				propName := propName
				allTerms := make([]terms.Term, 0, len(queryTermsByTokenization[tokenization]))
				for i, term := range queryTermsByTokenization[tokenization] {
					// pass i to the closure
					i := i
					term := term

					duplicateTextBoost := duplicateBoostsByTokenization[tokenization][i]
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
				// wandDiskStats[0] += float64(len(queryTermsByTokenization[tokenization]))

				resultsOriginalOrder := make([]terms.Term, len(flatTerms))
				copy(resultsOriginalOrder, flatTerms)

				topKHeap := b.getTopKHeap(limit, flatTerms, averagePropLength, params.AdditionalExplanations)
				objects, scores, err := b.getTopKObjects(topKHeap, resultsOriginalOrder, params.AdditionalExplanations)

				allObjects[propName][memTableIndex] = objects
				allScores[propName][memTableIndex] = scores

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

/*
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
*/
func (b *BM25Searcher) rankMultiBucket2(allObjects [][]*storobj.Object, allScores [][]float32, limit int) ([]*storobj.Object, []float32) {
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

func (b *BM25Searcher) rankMultiBucket(allObjects map[string][][]*storobj.Object, allScores map[string][][]float32, limit int) ([]*storobj.Object, []float32) {
	multiObjects := make(map[string][]*storobj.Object, len(allObjects))
	multiScores := make(map[string][]float32, len(allObjects))
	for propName := range allObjects {
		objects, scores := b.rankMultiBucketSingleProp(allObjects[propName], allScores[propName], limit)
		multiObjects[propName] = objects
		multiScores[propName] = scores
	}

	return b.mergeMultiProperty(multiObjects, multiScores, limit)
}

func (b *BM25Searcher) rankMultiBucketSingleProp(allObjects [][]*storobj.Object, allScores [][]float32, limit int) ([]*storobj.Object, []float32) {
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
	//if (limit - mergedPos) > 0 {
	//	mergedObjects = mergedObjects[mergedPos+1:]
	//	mergedScores = mergedScores[mergedPos+1:]
	//}

	return mergedObjects, mergedScores
}

func (b *BM25Searcher) mergeMultiProperty(multiObjects map[string][]*storobj.Object, multiScores map[string][]float32, limit int) ([]*storobj.Object, []float32) {
	docs := make(map[string]float32, len(multiObjects)*limit)
	docsToObjects := make(map[string]*storobj.Object, len(multiObjects)*limit)

	for propName, objects := range multiObjects {
		scores := multiScores[propName]
		for i, obj := range objects {
			docs[obj.ID().String()] += scores[i]
			docsToObjects[obj.ID().String()] = obj
		}
	}
	// sort the documents by score
	return b.sortByScore(docs, docsToObjects, limit)
}

// Custom type to hold the indices for sorting
type byFirstArray struct {
	firstArray  []float32
	secondArray []*storobj.Object
	indices     []int
}

func (b *byFirstArray) Init(firstArray []float32, secondArray []*storobj.Object) {
	// Create a slice of indices
	b.indices = make([]int, len(firstArray))
	for i := range b.indices {
		b.indices[i] = i
	}
	b.firstArray = firstArray
	b.secondArray = secondArray
}

// Implement sort.Interface for byFirstArray
func (b byFirstArray) Len() int {
	return len(b.indices)
}

func (b byFirstArray) Swap(i, j int) {
	b.indices[i], b.indices[j] = b.indices[j], b.indices[i]
}

func (b byFirstArray) Less(i, j int) bool {
	if b.firstArray[b.indices[i]] == b.firstArray[b.indices[j]] {
		return b.secondArray[b.indices[i]].ID().String() > b.secondArray[b.indices[j]].ID().String()
	}
	return b.firstArray[b.indices[i]] < b.firstArray[b.indices[j]]
}

func (b byFirstArray) GetSorted(limit int) ([]*storobj.Object, []float32) {
	sortedFirst := make([]float32, len(b.indices))
	sortedSecond := make([]*storobj.Object, len(b.indices))
	for i, index := range b.indices {
		sortedFirst[i] = b.firstArray[index]
		sortedSecond[i] = b.secondArray[index]

		if i == limit-1 {
			break
		}
	}
	if len(sortedFirst) < limit {
		limit = len(sortedFirst)
	}
	return sortedSecond[:limit], sortedFirst[:limit]
}

func (b *BM25Searcher) sortByScore(scoredObjects map[string]float32, mappedObjects map[string]*storobj.Object, limit int) ([]*storobj.Object, []float32) {
	// sort the documents by score
	objects := make([]*storobj.Object, 0, len(scoredObjects))
	scores := make([]float32, 0, len(scoredObjects))

	for obj, score := range scoredObjects {
		objects = append(objects, mappedObjects[obj])
		scores = append(scores, score)
	}

	// sort the objects by score
	bfa := byFirstArray{}
	bfa.Init(scores, objects)

	sort.Sort(bfa)

	return bfa.GetSorted(limit)
}
