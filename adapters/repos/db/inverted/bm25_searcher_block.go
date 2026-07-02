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

package inverted

import (
	"context"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/entities/additional"
	entcfg "github.com/weaviate/weaviate/entities/config"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

// var metrics = lsmkv.BlockMetrics{}

func (b *BM25Searcher) createBlockTerm(N float64, filterDocIds helpers.AllowList, query []string, propName string, propertyBoost float32, duplicateTextBoosts []int, config schema.BM25Config, ctx context.Context) ([][]*lsmkv.SegmentBlockMax, map[string]uint64, func(), error) {
	bucket := b.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
	return bucket.CreateDiskTerm(N, filterDocIds, query, propName, propertyBoost, duplicateTextBoosts, config, ctx)
}

func (b *BM25Searcher) wandBlock(
	ctx context.Context, filterDocIds helpers.AllowList, class *models.Class, params searchparams.KeywordRanking, limit int, additional additional.Properties,
) ([]*storobj.Object, []float32, bool, error) {
	start := time.Now()
	defer func() {
		if !entcfg.Enabled(os.Getenv("DISABLE_RECOVERY_ON_PANIC")) {
			if r := recover(); r != nil {
				b.logger.Errorf("Recovered from panic in wandBlock: %v", r)
				enterrors.PrintStack(b.logger)
			}
		}
	}()

	// if the filter is empty, we can skip the search
	// as no documents will match it
	if filterDocIds != nil && filterDocIds.IsEmpty() {
		return []*storobj.Object{}, []float32{}, false, nil
	}

	allBucketsAreInverted, N, propNamesByTokenization, queryTermsByTokenization, duplicateBoostsByTokenization, propertyBoosts, averagePropLength, err := b.generateQueryTermsAndStats(ctx, class, params)
	if err != nil {
		return nil, nil, false, err
	}

	// fallback to the old search process if not all buckets are inverted
	if !allBucketsAreInverted {
		objects, scores, err := b.wand(ctx, filterDocIds, class, params, limit, additional)
		return objects, scores, true, err
	}

	allResults := make([][][]*lsmkv.SegmentBlockMax, 0, len(params.Properties))
	termCounts := make([][]string, 0, len(params.Properties))
	minimumOrTokensMatchByProperty := make([]int, 0, len(params.Properties))

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

	tokenizationTime := time.Since(start)
	helpers.AnnotateSlowQueryLog(ctx, "kwd_1_tok_time", tokenizationTime)
	start = time.Now()
	for tokenization, propNames := range propNamesByTokenization {
		if len(propNames) > 0 {
			lenAllResults := len(allResults)
			queryTerms, duplicateBoosts := queryTermsByTokenization[tokenization], duplicateBoostsByTokenization[tokenization]
			duplicateBoostsByTerm := make(map[string]int, len(duplicateBoosts))
			for i, term := range queryTerms {
				duplicateBoostsByTerm[term] = duplicateBoosts[i]
			}
			globalIdfCounts := make(map[string]uint64, len(queryTerms))
			nonZeroTerms := make(map[string]uint64, len(queryTerms))
			for _, propName := range propNames {
				results, idfCounts, release, err := b.createBlockTerm(N, filterDocIds, queryTerms, propName, propertyBoosts[propName], duplicateBoosts, b.config, ctx)
				if err != nil {
					return nil, nil, false, err
				}

				if release != nil {
					releaseCallbacks[propName] = release
				}

				allResults = append(allResults, results)
				termCounts = append(termCounts, queryTerms)

				minimumOrTokensMatch := params.MinimumOrTokensMatch
				if params.SearchOperator == common_filters.SearchOperatorAnd {
					minimumOrTokensMatch = len(queryTerms)
				}

				minimumOrTokensMatchByProperty = append(minimumOrTokensMatchByProperty, minimumOrTokensMatch)
				for _, term := range queryTerms {
					globalIdfCounts[term] += idfCounts[term]
					if idfCounts[term] > 0 {
						nonZeroTerms[term]++
					}
				}
			}
			globalIdfs := make(map[string]float64, len(queryTerms))
			for term := range globalIdfCounts {
				if nonZeroTerms[term] == 0 {
					continue
				}
				n := globalIdfCounts[term] / nonZeroTerms[term]

				globalIdfs[term] = math.Log(float64(1)+(N-float64(n)+0.5)/(float64(n)+0.5)) * float64(duplicateBoostsByTerm[term])
			}
			for _, result := range allResults[lenAllResults:] {
				if len(result) == 0 {
					continue
				}
				for j := range result {
					if len(result[j]) == 0 {
						continue
					}
					for k := range result[j] {
						if result[j][k] != nil {
							result[j][k].SetIdf(globalIdfs[result[j][k].QueryTerm()])
						}
					}
				}
			}
			helpers.AnnotateSlowQueryLog(ctx, "kwd_2_terms_"+tokenization, len(queryTerms))
		}
	}

	if ctx.Err() != nil {
		return nil, nil, false, fmt.Errorf("after createBlockTerm: %w", ctx.Err())
	}

	// Cross-property AND: when enabled and all searched properties fall under a
	// single tokenization group, every query token must appear in at least one of
	// the searched properties (tokens may be spread across them) rather than all
	// tokens within one property. A single non-empty group guarantees one shared
	// queryTerms list / index space; otherwise we fall back to per-property AND.
	crossPropAnd := false
	var crossPropQueryTerms []string
	if params.SearchOperator == common_filters.SearchOperatorAnd && params.MatchTokensAcrossProperties {
		nonEmptyGroups := 0
		var groupKey string
		for tok, propNames := range propNamesByTokenization {
			if len(propNames) > 0 {
				nonEmptyGroups++
				groupKey = tok
			}
		}
		if nonEmptyGroups == 1 && len(queryTermsByTokenization[groupKey]) > 0 {
			crossPropAnd = true
			crossPropQueryTerms = queryTermsByTokenization[groupKey]
		}
	}

	// For unlimited queries, inflate the limit to the total candidate count so the
	// top-K heap can hold every match.
	unlimited := limit == 0
	if unlimited {
		for _, perProperty := range allResults {
			for _, perSegment := range perProperty {
				for _, perTerm := range perSegment {
					if perTerm != nil {
						limit += perTerm.Count()
					}
				}
			}
		}
	}

	// The cross-property pass produces the final global top-K in one shot, so it
	// uses the true limit and skips combineResults' per-property merge.
	if crossPropAnd {
		objects, scores, err := b.wandBlockCrossPropAnd(ctx, allResults, crossPropQueryTerms, averagePropLength, limit, params, additional)
		return objects, scores, false, err
	}

	// all results. Sum up the length of the results from all terms to get an upper bound of how many results there are
	internalLimit := limit
	if !unlimited {
		// TODO: the limit is increased by 10 to make sure candidates that are on the edge of the limit are not missed for multi-property search
		// the proper fix is to either make sure that the limit is always high enough, or force a rerank of the top results from all properties
		defaultLimit := int(math.Max(float64(limit)*1.1, float64(limit+10)))
		// allow overriding the defaultLimit with an env var
		internalLimitString := os.Getenv("BLOCKMAX_WAND_PER_SEGMENT_LIMIT")
		if internalLimitString != "" {
			// if the env var is set, use it as the limit
			internalLimit, _ = strconv.Atoi(internalLimitString)
		}

		if internalLimit < defaultLimit {
			// if the limit is smaller than the defaultLimit, use the defaultLimit
			internalLimit = defaultLimit
		}
	}

	termSearchTime := time.Since(start)
	start = time.Now()
	helpers.AnnotateSlowQueryLog(ctx, "kwd_3_term_time", termSearchTime)

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

			// return early if there aren't enough terms to match
			if len(allResults[i][j]) < minimumOrTokensMatchByProperty[i] {
				continue
			}

			eg.Go(func() (err error) {
				var topKHeap *priorityqueue.Queue[[]*terms.DocPointerWithScore]
				if params.SearchOperator == common_filters.SearchOperatorAnd {
					topKHeap = lsmkv.DoBlockMaxAnd(ctx, internalLimit, allResults[i][j], averagePropLength, params.AdditionalExplanations, len(termCounts[i]), minimumOrTokensMatchByProperty[i], b.logger)
				} else {
					topKHeap, _ = lsmkv.DoBlockMaxWand(ctx, internalLimit, allResults[i][j], averagePropLength, params.AdditionalExplanations, len(termCounts[i]), minimumOrTokensMatchByProperty[i], b.logger)
				}
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
		return nil, nil, false, err
	}

	blockSearchTime := time.Since(start)
	start = time.Now()
	helpers.AnnotateSlowQueryLog(ctx, "kwd_4_bmw_time", blockSearchTime)

	objects, scores := b.combineResults(allIds, allScores, allExplanation, termCounts, additional, limit)

	combineTime := time.Since(start)
	helpers.AnnotateSlowQueryLog(ctx, "kwd_5_objects_time", combineTime)
	helpers.AnnotateSlowQueryLog(ctx, "kwd_6_res_count", len(objects))

	return objects, scores, false, nil
}

func (b *BM25Searcher) combineResults(allIds [][][]uint64, allScores [][][]float32, allExplanation [][][][]*terms.DocPointerWithScore, queryTerms [][]string, additional additional.Properties, limit int) ([]*storobj.Object, []float32) {
	// Preallocate by the real upper bound (total result rows across
	// properties/segments), not limit*len(allIds): for unlimited (limit==0)
	// queries the caller inflates limit to the sum of all term counts, which
	// would over-allocate these slices by orders of magnitude.
	totalRows, totalTerms := 0, 0
	for i := range allIds {
		for j := range allIds[i] {
			totalRows += len(allIds[i][j])
		}
		totalTerms += len(queryTerms[i])
	}

	// combine all results
	combinedIds := make([]uint64, 0, totalRows)
	combinedScores := make([]float32, 0, totalRows)
	combinedExplanations := make([][]*terms.DocPointerWithScore, 0, totalRows)
	combinedTerms := make([]string, 0, totalTerms)

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

// wandBlockCrossPropAnd runs a single cross-property AND intersection: a document
// is returned only if every query token is present in at least one of the searched
// properties. It groups the per-property/per-segment terms by query-term index into
// MergedTerms, intersects them in one pass, and returns the final ranked objects
// directly (bypassing combineResults, whose per-property max + cross-property sum is
// folded into MergedTerm.Score).
func (b *BM25Searcher) wandBlockCrossPropAnd(ctx context.Context, allResults [][][]*lsmkv.SegmentBlockMax, queryTerms []string, averagePropLength float64, limit int, params searchparams.KeywordRanking, additional additional.Properties) ([]*storobj.Object, []float32, error) {
	mergedTerms, ok := lsmkv.BuildCrossPropMergedTerms(allResults, len(queryTerms))
	if !ok {
		// at least one query token is absent from every property — AND can't match
		return []*storobj.Object{}, []float32{}, nil
	}

	topKHeap := lsmkv.DoBlockMaxAndCrossProp(ctx, limit, mergedTerms, averagePropLength, params.AdditionalExplanations, len(queryTerms), b.logger)

	ids, scores, explanations, err := b.getTopKIds(topKHeap)
	if err != nil {
		return nil, nil, err
	}

	// getTopKIds returns an empty (not per-id) explanations slice when explanations
	// were not requested; downstream indexing is positional, so collapse it to nil.
	if len(explanations) != len(ids) {
		explanations = nil
	}

	ids, scores, explanations = b.sortResultsByScore(ids, scores, explanations)
	objLimit := int(math.Min(float64(limit), float64(len(ids))))
	return b.getObjectsAndScores(ids, scores, explanations, queryTerms, additional, objLimit)
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

func (b *BM25Searcher) sortResultsByExternalId(objects []*storobj.Object, scores []float32) ([]*storobj.Object, []float32) {
	sorter := &objectIdsSorter{
		objects: objects,
		scores:  scores,
	}
	sort.Sort(sorter)
	return sorter.objects, sorter.scores
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
		objsBatch, err := storobj.ObjectsByDocIDWithEmpty(objectsBucket, ids[startAt:endAt], additionalProps, nil, b.logger)
		if err != nil {
			return objs, nil, errors.Errorf("objects loading")
		}
		for i, obj := range objsBatch {
			if obj == nil || obj.DocID != ids[startAt+i] {
				continue
			}
			objs = append(objs, obj)
			scoresResult = append(scoresResult, scores[startAt+i])
			if explanations != nil {
				explanationsResults = append(explanationsResults, explanations[startAt+i])
			}
		}
		startAt = endAt
		endAt = min(endAt+limit, len(ids))
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

	objs, scoresResult = b.sortResultsByExternalId(objs, scoresResult)

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

type objectIdsSorter struct {
	objects []*storobj.Object
	scores  []float32
}

func (s *objectIdsSorter) Len() int {
	return len(s.objects)
}

func (s *objectIdsSorter) Less(i, j int) bool {
	if s.scores[i] == s.scores[j] {
		return s.objects[i].Object.ID > s.objects[j].Object.ID
	}
	return s.scores[i] < s.scores[j]
}

func (s *objectIdsSorter) Swap(i, j int) {
	s.scores[i], s.scores[j] = s.scores[j], s.scores[i]
	s.objects[i], s.objects[j] = s.objects[j], s.objects[i]
}
