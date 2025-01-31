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
	"slices"
	"sort"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

// var metrics = lsmkv.BlockMetrics{}

func (b *BM25Searcher) createBlockTerm(N float64, filterDocIds helpers.AllowList, query []string, propName string, propertyBoost float32, duplicateTextBoosts []int, averagePropLength float64, config schema.BM25Config, ctx context.Context) ([][]*lsmkv.SegmentBlockMax, *sync.RWMutex, error) {
	bucket := b.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
	return bucket.CreateDiskTerm(N, filterDocIds, query, propName, propertyBoost, duplicateTextBoosts, averagePropLength, config, ctx)
}

func (b *BM25Searcher) wandBlock(
	ctx context.Context, filterDocIds helpers.AllowList, class *models.Class, params searchparams.KeywordRanking, limit int, additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	allBucketsAreInverted, N, propNamesByTokenization, queryTermsByTokenization, duplicateBoostsByTokenization, propertyBoosts, averagePropLength, err := b.generateQueryTermsAndStats(class, params)
	if err != nil {
		return nil, nil, err
	}

	if !allBucketsAreInverted {
		return b.wand(ctx, filterDocIds, class, params, limit, additional)
	}

	allResults := make([][][]*lsmkv.SegmentBlockMax, 0, len(params.Properties))
	termCounts := make([][]string, 0, len(params.Properties))

	// These locks are the segmentCompactions locks for the searched properties
	// The old search process locked the compactions and read the full postings list into memory.
	// We don't do that anymore, as the goal of BlockMaxWAND is to avoid reading the full postings list into memory.
	// The locks are needed here instead of at DoBlockMaxWand only, as we separate term creation from the actual search.
	// TODO: We should consider if we can remove these locks and only lock at DoBlockMaxWand
	locks := make(map[string]*sync.RWMutex, len(params.Properties))

	defer func() {
		for _, lock := range locks {
			if lock != nil {
				lock.RUnlock()
			}
		}
	}()

	for _, tokenization := range helpers.Tokenizations {
		propNames := propNamesByTokenization[tokenization]
		if len(propNames) > 0 {
			queryTerms, duplicateBoosts := queryTermsByTokenization[tokenization], duplicateBoostsByTokenization[tokenization]
			for _, propName := range propNames {
				results, lock, err := b.createBlockTerm(N, filterDocIds, queryTerms, propName, propertyBoosts[propName], duplicateBoosts, averagePropLength, b.config, ctx)
				if err != nil {
					if lock != nil {
						lock.RUnlock()
					}
					return nil, nil, err
				}
				if lock != nil {
					locks[propName] = lock
				}
				allResults = append(allResults, results)
				termCounts = append(termCounts, queryTerms)
			}

		}
	}

	// all results. Sum up the length of the results from all terms to get an upper bound of how many results there are
	internalLimit := limit
	if limit == 0 {
		for _, perProperty := range allResults {
			for _, perSegment := range perProperty {
				for _, perTerm := range perSegment {
					if perTerm != nil {
						limit += perTerm.Count()
					}
				}
			}
		}
		internalLimit = limit

	} else if len(allResults) > 1 { // we only need to increase the limit if there are multiple properties
		// TODO: the limit is increased by 10 to make sure candidates that are on the edge of the limit are not missed for multi-property search
		// the proper fix is to either make sure that the limit is always high enough, or force a rerank of the top results from all properties
		internalLimit = limit + 10
	}

	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU)

	allObjects := make([][][]*storobj.Object, len(allResults))
	allScores := make([][][]float32, len(allResults))
	for i, perProperty := range allResults {
		allObjects[i] = make([][]*storobj.Object, len(perProperty))
		allScores[i] = make([][]float32, len(perProperty))
		// per segment
		for j := range perProperty {

			i := i
			j := j

			if len(allResults[i][j]) == 0 {
				continue
			}
			eg.Go(func() (err error) {
				topKHeap := lsmkv.DoBlockMaxWand(internalLimit, allResults[i][j], averagePropLength, params.AdditionalExplanations, len(termCounts[i]))
				objects, scores, err := b.getTopKObjects(topKHeap, params.AdditionalExplanations, termCounts[i], additional)

				allObjects[i][j] = objects
				allScores[i][j] = scores
				if err != nil {
					return err
				}
				return nil
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	objects, scores := b.combineResults(allObjects, allScores, limit)

	return objects, scores, nil
}

func (b *BM25Searcher) combineResults(allObjects [][][]*storobj.Object, allScores [][][]float32, limit int) ([]*storobj.Object, []float32) {
	// combine all results
	combinedObjects := make([]*storobj.Object, 0, limit*len(allObjects))
	combinedScores := make([]float32, 0, limit*len(allObjects))

	// combine all results
	for i := range allObjects {
		singlePropObjects := slices.Concat(allObjects[i]...)
		singlePropScores := slices.Concat(allScores[i]...)
		// Choose the highest score for each object if it appears in multiple segments
		combinedObjectsProp, combinedScoresProp := b.combineResultsForMultiProp(singlePropObjects, singlePropScores, func(a, b float32) float32 { return b })
		combinedObjects = append(combinedObjects, combinedObjectsProp...)
		combinedScores = append(combinedScores, combinedScoresProp...)
	}

	// Choose the sum of the scores for each object if it appears in multiple properties
	combinedObjects, combinedScores = b.combineResultsForMultiProp(combinedObjects, combinedScores, func(a, b float32) float32 { return a + b })

	combinedObjects, combinedScores = b.sortResultsByScore(combinedObjects, combinedScores)

	if len(combinedObjects) <= limit {
		return combinedObjects, combinedScores
	}

	return combinedObjects[len(combinedObjects)-limit:], combinedScores[len(combinedObjects)-limit:]
}

type aggregate func(float32, float32) float32

func (b *BM25Searcher) combineResultsForMultiProp(allObjects []*storobj.Object, allScores []float32, aggregateFn aggregate) ([]*storobj.Object, []float32) {
	// if ids are the same, sum the scores
	combinedObjects := make(map[string]*storobj.Object)
	combinedScores := make(map[string]float32)

	for i, obj := range allObjects {
		id := string(obj.ID())
		if _, ok := combinedObjects[id]; !ok {
			combinedObjects[id] = obj
			combinedScores[id] = allScores[i]
		} else {
			combinedObjects[id] = combineObjects(combinedObjects[id], obj)
			combinedScores[id] = aggregateFn(combinedScores[id], allScores[i])
		}
	}

	// sort the combined results
	combinedObjectsSlice := make([]*storobj.Object, 0, len(combinedObjects))
	combinedScoresSlice := make([]float32, 0, len(combinedObjects))

	for id, obj := range combinedObjects {
		combinedObjectsSlice = append(combinedObjectsSlice, obj)
		combinedScoresSlice = append(combinedScoresSlice, combinedScores[id])
	}

	return combinedObjectsSlice, combinedScoresSlice
}

func (b *BM25Searcher) sortResultsByScore(objects []*storobj.Object, scores []float32) ([]*storobj.Object, []float32) {
	sorter := &scoreSorter{
		objects: objects,
		scores:  scores,
	}
	sort.Sort(sorter)
	return sorter.objects, sorter.scores
}

type scoreSorter struct {
	objects []*storobj.Object
	scores  []float32
}

func (s *scoreSorter) Len() int {
	return len(s.objects)
}

func (s *scoreSorter) Less(i, j int) bool {
	if s.scores[i] == s.scores[j] {
		return s.objects[i].ID() > s.objects[j].ID()
	}
	return s.scores[i] < s.scores[j]
}

func (s *scoreSorter) Swap(i, j int) {
	s.objects[i], s.objects[j] = s.objects[j], s.objects[i]
	s.scores[i], s.scores[j] = s.scores[j], s.scores[i]
}

func combineObjects(a, b *storobj.Object) *storobj.Object {
	for k, v := range b.Object.Additional {
		a.Object.Additional[k] = v
	}
	return a
}
