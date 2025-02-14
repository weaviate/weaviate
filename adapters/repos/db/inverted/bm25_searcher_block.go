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
	"math"
	"slices"
	"sort"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

// var metrics = lsmkv.BlockMetrics{}

func (b *BM25Searcher) createBlockTerm(N float64, filterDocIds helpers.AllowList, query []string, propName string, propertyBoost float32, duplicateTextBoosts []int, averagePropLength float64, config schema.BM25Config, ctx context.Context) ([][]*lsmkv.SegmentBlockMax, func(), error) {
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
	releaseCallbacks := make(map[string]func(), len(params.Properties))

	defer func() {
		for _, release := range releaseCallbacks {
			release()
		}
	}()

	for _, tokenization := range helpers.Tokenizations {
		propNames := propNamesByTokenization[tokenization]
		if len(propNames) > 0 {
			queryTerms, duplicateBoosts := queryTermsByTokenization[tokenization], duplicateBoostsByTokenization[tokenization]
			for _, propName := range propNames {
				results, release, err := b.createBlockTerm(N, filterDocIds, queryTerms, propName, propertyBoosts[propName], duplicateBoosts, averagePropLength, b.config, ctx)
				if err != nil {
					return nil, nil, err
				}

				if release != nil {
					releaseCallbacks[propName] = release
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
			eg.Go(func() (err error) {
				topKHeap := lsmkv.DoBlockMaxWand(internalLimit, allResults[i][j], averagePropLength, params.AdditionalExplanations, len(termCounts[i]))
				ids, scores, explanations, err := b.getTopKIds(topKHeap)
				if err != nil {
					return err
				}

				allIds[i][j] = ids
				allScores[i][j] = scores
				if len(explanations) > 0 {
					allExplanation[i][j] = explanations
				}

				return nil
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	objects, scores := b.combineResults(allIds, allScores, allExplanation, termCounts, additional, limit)
	return objects, scores, nil
}

func (b *BM25Searcher) combineResults(allIds [][][]uint64, allScores [][][]float32, allExplanation [][][][]*terms.DocPointerWithScore, queryTerms [][]string, additional additional.Properties, limit int) ([]*storobj.Object, []float32) {
	// combine all results
	combinedIds := make([]uint64, 0, limit*len(allIds))
	combinedScores := make([]float32, 0, limit*len(allIds))
	combinedExplanations := make([][]*terms.DocPointerWithScore, 0, limit*len(allIds))
	combinedTerms := make([]string, 0, limit*len(allIds))

	// combine all results
	for i := range allIds {
		singlePropIds := slices.Concat(allIds[i]...)
		singlePropScores := slices.Concat(allScores[i]...)
		singlePropExplanation := slices.Concat(allExplanation[i]...)
		// Choose the highest score for each object if it appears in multiple segments
		combinedIdsProp, combinedScoresProp, combinedExplanationProp := b.combineResultsForMultiProp(singlePropIds, singlePropScores, singlePropExplanation, func(a, b float32) float32 { return b }, true)
		combinedIds = append(combinedIds, combinedIdsProp...)
		combinedScores = append(combinedScores, combinedScoresProp...)
		combinedExplanations = append(combinedExplanations, combinedExplanationProp...)
		combinedTerms = append(combinedTerms, queryTerms[i]...)
	}

	// Choose the sum of the scores for each object if it appears in multiple properties
	combinedIds, combinedScores, combinedExplanations = b.combineResultsForMultiProp(combinedIds, combinedScores, combinedExplanations, func(a, b float32) float32 { return a + b }, false)

	combinedIds, combinedScores, combinedExplanations = b.sortResultsByScore(combinedIds, combinedScores, combinedExplanations)

	limit = int(math.Min(float64(limit), float64(len(combinedIds))))

	combinedObjects, combinedScores, err := b.getObjectsAndScores(combinedIds, combinedScores, combinedExplanations, combinedTerms, additional, limit)
	if err != nil {
		return nil, nil
	}
	return combinedObjects, combinedScores
}

type aggregate func(float32, float32) float32

func (b *BM25Searcher) combineResultsForMultiProp(allIds []uint64, allScores []float32, allExplanation [][]*terms.DocPointerWithScore, aggregateFn aggregate, singleProp bool) ([]uint64, []float32, [][]*terms.DocPointerWithScore) {
	// if ids are the same, sum the scores
	combinedScores := make(map[uint64]float32)
	combinedExplanations := make(map[uint64][]*terms.DocPointerWithScore)

	for i, obj := range allIds {
		id := obj
		if _, ok := combinedScores[id]; !ok {
			combinedScores[id] = allScores[i]
			if len(allExplanation) > 0 {
				combinedExplanations[id] = allExplanation[i]
			}
		} else {
			combinedScores[id] = aggregateFn(combinedScores[id], allScores[i])
			if len(allExplanation) > 0 {
				if singleProp {
					combinedExplanations[id] = allExplanation[i]
				} else {
					combinedExplanations[id] = append(combinedExplanations[id], allExplanation[i]...)
				}
			}

		}
	}

	ids := make([]uint64, 0, len(combinedScores))
	scores := make([]float32, 0, len(combinedScores))
	exp := make([][]*terms.DocPointerWithScore, 0, len(combinedScores))
	for id, score := range combinedScores {
		ids = append(ids, id)
		scores = append(scores, score)
		if allExplanation != nil {
			exp = append(exp, combinedExplanations[id])
		}
	}
	return ids, scores, exp
}

func (b *BM25Searcher) sortResultsByScore(ids []uint64, scores []float32, explanations [][]*terms.DocPointerWithScore) ([]uint64, []float32, [][]*terms.DocPointerWithScore) {
	sorter := &scoreSorter{
		ids:          ids,
		scores:       scores,
		explanations: explanations,
	}
	sort.Sort(sorter)
	return sorter.ids, sorter.scores, sorter.explanations
}

func (b *BM25Searcher) getObjectsAndScores(ids []uint64, scores []float32, explanations [][]*terms.DocPointerWithScore, queryTerms []string, additionalProps additional.Properties, limit int) ([]*storobj.Object, []float32, error) {
	// reverse arrays to start with the highest score
	slices.Reverse(ids)
	slices.Reverse(scores)
	if explanations != nil {
		slices.Reverse(explanations)
	}

	objs := make([]*storobj.Object, 0, limit)
	scoresResult := make([]float32, 0, limit)
	explanationsResults := make([][]*terms.DocPointerWithScore, 0, limit)

	objectsBucket := b.store.Bucket(helpers.ObjectsBucketLSM)

	startAt := 0
	endAt := limit
	// try to get docs up to the limit
	// if there are not enough docs, get limit more docs until we've exhausted the list of ids
	for len(objs) < limit && startAt < len(ids) {
		objsBatch, err := storobj.ObjectsByDocID(objectsBucket, ids[startAt:endAt], additionalProps, nil, b.logger)
		if err != nil {
			return objs, nil, errors.Errorf("objects loading")
		}
		for i, obj := range objsBatch {
			if obj == nil {
				continue
			}
			if obj.DocID != ids[startAt+i] {
				continue
			}
			objs = append(objs, obj)
			scoresResult = append(scoresResult, scores[startAt+i])
			if explanations != nil {
				explanationsResults = append(explanationsResults, explanations[startAt+i])
			}
		}
		startAt = endAt
		endAt = int(math.Min(float64(endAt+limit), float64(len(ids))))
	}

	if explanationsResults != nil && len(explanationsResults) == len(scoresResult) {
		for k := range objs {
			// add score explanation
			if objs[k].AdditionalProperties() == nil {
				objs[k].Object.Additional = make(map[string]interface{})
			}
			for j, result := range explanationsResults[k] {
				if result == nil {
					continue
				}
				queryTerm := queryTerms[j]
				objs[k].Object.Additional["BM25F_"+queryTerm+"_frequency"] = result.Frequency
				objs[k].Object.Additional["BM25F_"+queryTerm+"_propLength"] = result.PropLength
			}
		}
	}

	// reverse back the arrays to the expected order
	slices.Reverse(objs)
	slices.Reverse(scoresResult)

	return objs, scoresResult, nil
}

type scoreSorter struct {
	ids          []uint64
	scores       []float32
	explanations [][]*terms.DocPointerWithScore
}

func (s *scoreSorter) Len() int {
	return len(s.ids)
}

func (s *scoreSorter) Less(i, j int) bool {
	if s.scores[i] == s.scores[j] {
		return s.ids[i] > s.ids[j]
	}
	return s.scores[i] < s.scores[j]
}

func (s *scoreSorter) Swap(i, j int) {
	s.ids[i], s.ids[j] = s.ids[j], s.ids[i]
	s.scores[i], s.scores[j] = s.scores[j], s.scores[i]
	if s.explanations != nil {
		s.explanations[i], s.explanations[j] = s.explanations[j], s.explanations[i]
	}
}
