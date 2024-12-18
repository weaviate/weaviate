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
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
)

// var metrics = lsmkv.BlockMetrics{}

func (b *BM25Searcher) createBlockTerm(N float64, filterDocIds helpers.AllowList, query []string, propName string, propertyBoost float32, duplicateTextBoosts []int, averagePropLength float64, config schema.BM25Config, ctx context.Context) ([][]terms.TermInterface, *sync.RWMutex, error) {
	bucket := b.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
	desiredStrategy := bucket.GetDesiredStrategy()
	if desiredStrategy == lsmkv.StrategyInverted {
		return bucket.CreateDiskTerm(N, filterDocIds, query, propName, propertyBoost, duplicateTextBoosts, averagePropLength, config, ctx)
	} else if desiredStrategy == lsmkv.StrategyMapCollection {
		term := make([]terms.TermInterface, 0, len(query))
		for i, queryTerm := range query {
			propertyBoosts := make(map[string]float32)
			propertyBoosts[propName] = propertyBoost
			t, err := b.createTerm(N, filterDocIds, queryTerm, i, []string{propName}, propertyBoosts, duplicateTextBoosts[i], ctx)
			if err != nil {
				return nil, nil, err
			}
			if t != nil {
				term = append(term, t)
			}
		}
		return [][]terms.TermInterface{term}, nil, nil
	} else {
		return nil, nil, fmt.Errorf("unsupported strategy %s", desiredStrategy)
	}
}

func (b *BM25Searcher) wandBlock(
	ctx context.Context, filterDocIds helpers.AllowList, class *models.Class, params searchparams.KeywordRanking, limit int, additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	N, propNamesByTokenization, queryTermsByTokenization, duplicateBoostsByTokenization, propertyBoosts, averagePropLength, err := b.generateQueryTermsAndStats(class, params)
	if err != nil {
		return nil, nil, err
	}

	allResults := make([][][]terms.TermInterface, 0, len(params.Properties))
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

	allIds := make([][][]uint64, len(allResults))
	allScores := make([][][]float32, len(allResults))
	allExplanation := make([][][][]*terms.DocPointerWithScore, len(allResults))
	for i, perProperty := range allResults {
		allIds[i] = make([][]uint64, len(perProperty))
		allScores[i] = make([][]float32, len(perProperty))
		allExplanation[i] = make([][][]*terms.DocPointerWithScore, len(perProperty))
		// per segment
		for j := range perProperty {

			i := i
			j := j

			if len(allResults[i][j]) == 0 {
				continue
			}

			combinedTerms := &terms.Terms{
				T:     allResults[i][j],
				Count: len(termCounts[i]),
			}

			eg.Go(func() (err error) {
				topKHeap := terms.DoBlockMaxWand(internalLimit, combinedTerms, averagePropLength, params.AdditionalExplanations)
				ids, scores, explanations, err := b.getTopKIds(topKHeap)

				allIds[i][j] = ids
				allScores[i][j] = scores
				allExplanation[i][j] = explanations
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

	objects, scores := b.combineResults(allIds, allScores, allExplanation, additional, limit)

	return objects, scores, nil
}

func (b *BM25Searcher) combineResults(allIds [][][]uint64, allScores [][][]float32, allExplanation [][][][]*terms.DocPointerWithScore, additional additional.Properties, limit int) ([]*storobj.Object, []float32) {
	// combine all results
	combinedIds := make([]uint64, 0, limit*len(allIds))
	combinedScores := make([]float32, 0, limit*len(allIds))
	combinedExplanations := make([][]*terms.DocPointerWithScore, 0, limit*len(allIds))

	// combine all results
	for i := range allIds {
		singlePropIds := slices.Concat(allIds[i]...)
		singlePropScores := slices.Concat(allScores[i]...)
		singlePropExplanation := slices.Concat(allExplanation[i]...)
		// Choose the highest score for each object if it appears in multiple segments
		combinedIdsProp, combinedScoresProp, combinedExplanationProp := b.combineResultsForMultiProp(singlePropIds, singlePropScores, singlePropExplanation, func(a, b float32) float32 { return b })
		combinedIds = append(combinedIds, combinedIdsProp...)
		combinedScores = append(combinedScores, combinedScoresProp...)
		combinedExplanations = append(combinedExplanations, combinedExplanationProp...)
	}

	// Choose the sum of the scores for each object if it appears in multiple properties
	combinedIds, combinedScores, _ = b.combineResultsForMultiProp(combinedIds, combinedScores, combinedExplanations, func(a, b float32) float32 { return a + b })

	combinedIds, combinedScores = b.sortResultsByScore(combinedIds, combinedScores)

	// min between limit and len(combinedIds)
	limit = int(math.Min(float64(limit), float64(len(combinedIds))))

	combinedObjects, combinedScores, err := b.getObjectsAndScores(combinedIds[limit:], combinedScores[limit:], additional)
	if err != nil {
		return nil, nil
	}

	if len(combinedIds) <= limit {
		return combinedObjects, combinedScores
	}

	return combinedObjects[len(combinedObjects)-limit:], combinedScores[len(combinedObjects)-limit:]
}

type aggregate func(float32, float32) float32

func (b *BM25Searcher) combineResultsForMultiProp(allObjects []uint64, allScores []float32, allExplanation [][]*terms.DocPointerWithScore, aggregateFn aggregate) ([]uint64, []float32, [][]*terms.DocPointerWithScore) {
	// if ids are the same, sum the scores
	combinedScores := make(map[uint64]float32)

	for i, obj := range allObjects {
		id := obj
		if _, ok := combinedScores[id]; !ok {
			combinedScores[id] = allScores[i]
		} else {
			combinedScores[id] = aggregateFn(combinedScores[id], allScores[i])
		}
	}

	objects := make([]uint64, 0, len(combinedScores))
	scores := make([]float32, 0, len(combinedScores))
	exp := make([][]*terms.DocPointerWithScore, 0, len(combinedScores))
	for id, score := range combinedScores {
		objects = append(objects, id)
		scores = append(scores, score)
	}
	return objects, scores, exp
}

func (b *BM25Searcher) sortResultsByScore(objects []uint64, scores []float32) ([]uint64, []float32) {
	sorter := &scoreSorter{
		objects: objects,
		scores:  scores,
	}
	sort.Sort(sorter)
	return sorter.objects, sorter.scores
}

func (b *BM25Searcher) getObjectsAndScores(ids []uint64, scores []float32, additionalProps additional.Properties) ([]*storobj.Object, []float32, error) {
	objectsBucket := b.store.Bucket(helpers.ObjectsBucketLSM)

	objs, err := storobj.ObjectsByDocID(objectsBucket, ids, additionalProps, nil, b.logger)
	if err != nil {
		return objs, nil, errors.Errorf("objects loading")
	}

	if len(objs) != len(scores) {
		idsTmp := make([]uint64, len(objs))
		j := 0
		for i := range scores {
			if j >= len(objs) {
				break
			}
			if objs[j].DocID != ids[i] {
				continue
			}
			scores[j] = scores[i]
			idsTmp[j] = ids[i]
			j++
		}
		scores = scores[:j]
		// explanations = explanations[:j]
		// ids = idsTmp[:j]
	}

	//WIP
	/*
		if len(explanations) == len(scores) {
			for k := range objs {
				// add score explanation
				if objs[k].AdditionalProperties() == nil {
					objs[k].Object.Additional = make(map[string]interface{})
				}
				for j, result := range explanations[k] {
					if result == nil {
						continue
					}
					queryTerm := terms[j]
					objs[k].Object.Additional["BM25F_"+queryTerm+"_frequency"] = result.Frequency
					objs[k].Object.Additional["BM25F_"+queryTerm+"_propLength"] = result.PropLength
				}
			}
		}
	*/

	return objs, scores, nil
}

type scoreSorter struct {
	objects []uint64
	scores  []float32
}

func (s *scoreSorter) Len() int {
	return len(s.objects)
}

func (s *scoreSorter) Less(i, j int) bool {
	if s.scores[i] == s.scores[j] {
		return s.objects[i] < s.objects[j]
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
