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
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/terms"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

type BM25Searcher struct {
	config         schema.BM25Config
	store          *lsmkv.Store
	getClass       func(string) *models.Class
	classSearcher  ClassSearcher // to allow recursive searches on ref-props
	propIndices    propertyspecific.Indices
	propLenTracker propLengthRetriever
	logger         logrus.FieldLogger
	shardVersion   uint16
}

type propLengthRetriever interface {
	PropertyMean(prop string) (float32, error)
}

type termListRequest struct {
	term               string
	termId             int
	duplicateTextBoost int
	propertyNames      []string
	propertyBoosts     map[string]float32
}

func NewBM25Searcher(config schema.BM25Config, store *lsmkv.Store,
	getClass func(string) *models.Class, propIndices propertyspecific.Indices,
	classSearcher ClassSearcher, propLenTracker propLengthRetriever,
	logger logrus.FieldLogger, shardVersion uint16,
) *BM25Searcher {
	return &BM25Searcher{
		config:         config,
		store:          store,
		getClass:       getClass,
		propIndices:    propIndices,
		classSearcher:  classSearcher,
		propLenTracker: propLenTracker,
		logger:         logger.WithField("action", "bm25_search"),
		shardVersion:   shardVersion,
	}
}

func (b *BM25Searcher) BM25F(ctx context.Context, filterDocIds helpers.AllowList,
	className schema.ClassName, limit int, keywordRanking searchparams.KeywordRanking, additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	// WEAVIATE-471 - If a property is not searchable, return an error
	for _, property := range keywordRanking.Properties {
		if !PropertyHasSearchableIndex(b.getClass(className.String()), property) {
			return nil, nil, inverted.NewMissingSearchableIndexError(property)
		}
	}

	class := b.getClass(className.String())
	if class == nil {
		return nil, nil, fmt.Errorf("could not find class %s in schema", className)
	}

	var objs []*storobj.Object
	var scores []float32
	var err error

	// TODO: amourao - move this to the global config
	if os.Getenv("USE_BLOCKMAX_WAND") == "false" {
		objs, scores, err = b.wand(ctx, filterDocIds, class, keywordRanking, limit, additional)
	} else {
		objs, scores, err = b.wandBlock(ctx, filterDocIds, class, keywordRanking, limit, additional)
	}
	if err != nil {
		return nil, nil, errors.Wrap(err, "wand")
	}

	return objs, scores, nil
}

func (b *BM25Searcher) GetPropertyLengthTracker() *JsonShardMetaData {
	return b.propLenTracker.(*JsonShardMetaData)
}

func (b *BM25Searcher) generateQueryTermsAndStats(class *models.Class, params searchparams.KeywordRanking) (bool, float64, map[string][]string, map[string][]string, map[string][]int, map[string]float32, float64, error) {
	N := float64(b.store.Bucket(helpers.ObjectsBucketLSM).Count())

	// This flag checks whether all buckets are of the inverted strategy,
	// and thus, compatible with BlockMaxWAND, or if there are other strategies present,
	// which would require the old WAND implementation.
	allBucketsAreInverted := true

	var stopWordDetector *stopwords.Detector
	if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.Stopwords != nil {
		var err error
		stopWordDetector, err = stopwords.NewDetectorFromConfig(*(class.InvertedIndexConfig.Stopwords))
		if err != nil {
			return false, 0, nil, nil, nil, nil, 0, err
		}
	}

	// There are currently cases, for different tokenization:
	// word, lowercase, whitespace and field.
	// Query is tokenized and respective properties are then searched for the search terms,
	// results at the end are combined using WAND
	queryTermsByTokenization := map[string][]string{}
	duplicateBoostsByTokenization := map[string][]int{}
	propNamesByTokenization := map[string][]string{}
	propertyBoosts := make(map[string]float32, len(params.Properties))

	for _, tokenization := range helpers.Tokenizations {
		queryTerms, dupBoosts := helpers.TokenizeAndCountDuplicates(tokenization, params.Query)
		queryTermsByTokenization[tokenization] = queryTerms
		duplicateBoostsByTokenization[tokenization] = dupBoosts

		// stopword filtering for word tokenization
		if tokenization == models.PropertyTokenizationWord {
			queryTerms, dupBoosts = b.removeStopwordsFromQueryTerms(queryTermsByTokenization[tokenization],
				duplicateBoostsByTokenization[tokenization], stopWordDetector)
			queryTermsByTokenization[tokenization] = queryTerms
			duplicateBoostsByTokenization[tokenization] = dupBoosts
		}

		propNamesByTokenization[tokenization] = make([]string, 0)
	}

	averagePropLength := 0.
	averagePropLengthCount := 0
	for _, propertyWithBoost := range params.Properties {
		property := propertyWithBoost
		propBoost := 1
		if strings.Contains(propertyWithBoost, "^") {
			property = strings.Split(propertyWithBoost, "^")[0]
			boostStr := strings.Split(propertyWithBoost, "^")[1]
			propBoost, _ = strconv.Atoi(boostStr)
		}
		propertyBoosts[property] = float32(propBoost)

		propMean, err := b.GetPropertyLengthTracker().PropertyMean(property)
		if err != nil {
			return false, 0, nil, nil, nil, nil, 0, err
		}

		bucket := b.GetBucket(property)
		if bucket == nil {
			return false, 0, nil, nil, nil, nil, 0, fmt.Errorf("could not find bucket for property %v", property)
		}

		if bucket.Strategy() != lsmkv.StrategyInverted {
			allBucketsAreInverted = false
		}

		// A NaN here is the results of a corrupted prop length tracker.
		// This is a workaround to try and avoid 0 or NaN scores.
		// There is an extra check below in case all prop lengths are NaN or 0.
		// Related issue https://github.com/weaviate/weaviate/issues/6247
		if !math.IsNaN(float64(propMean)) {
			averagePropLength += float64(propMean)
			averagePropLengthCount++
		}

		prop, err := schema.GetPropertyByName(class, property)
		if err != nil {
			return false, 0, nil, nil, nil, nil, 0, err
		}

		switch dt, _ := schema.AsPrimitive(prop.DataType); dt {
		case schema.DataTypeText, schema.DataTypeTextArray:
			if _, exists := propNamesByTokenization[prop.Tokenization]; !exists {
				return false, 0, nil, nil, nil, nil, 0, fmt.Errorf("cannot handle tokenization '%v' of property '%s'",
					prop.Tokenization, prop.Name)
			}
			propNamesByTokenization[prop.Tokenization] = append(propNamesByTokenization[prop.Tokenization], property)
		default:
			return false, 0, nil, nil, nil, nil, 0, fmt.Errorf("cannot handle datatype '%v' of property '%s'", dt, prop.Name)
		}
	}

	averagePropLength = averagePropLength / float64(averagePropLengthCount)

	// If this value is zero or NaN, the prop length tracker is fully corrupted.
	// This is a workaround to avoid 0 or NaN scores.
	// Related issue https://github.com/weaviate/weaviate/issues/6247
	// sane default, if all prop lengths are NaN or 0
	if math.IsNaN(averagePropLength) || averagePropLength == 0 {
		averagePropLength = 40.0
	}
	return allBucketsAreInverted, N, propNamesByTokenization, queryTermsByTokenization, duplicateBoostsByTokenization, propertyBoosts, averagePropLength, nil
}

func (b *BM25Searcher) wand(
	ctx context.Context, filterDocIds helpers.AllowList, class *models.Class, params searchparams.KeywordRanking, limit int, additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	_, N, propNamesByTokenization, queryTermsByTokenization, duplicateBoostsByTokenization, propertyBoosts, averagePropLength, err := b.generateQueryTermsAndStats(class, params)
	if err != nil {
		return nil, nil, err
	}

	allRequests := make([]termListRequest, 0, 1000)
	allQueryTerms := make([]string, 0, 1000)
	minimumOrTokensMatch := math.MaxInt64

	for _, tokenization := range helpers.Tokenizations {
		propNames := propNamesByTokenization[tokenization]
		if len(propNames) > 0 {
			queryTerms, duplicateBoosts := queryTermsByTokenization[tokenization], duplicateBoostsByTokenization[tokenization]
			for queryTermIndex, queryTerm := range queryTerms {
				allRequests = append(allRequests, termListRequest{
					term:               queryTerm,
					termId:             len(allRequests),
					duplicateTextBoost: duplicateBoosts[queryTermIndex],
					propertyNames:      propNames,
					propertyBoosts:     propertyBoosts,
				})
				allQueryTerms = append(allQueryTerms, queryTerm)
			}
			minimumOrTokensMatchByTokenization := params.MinimumOrTokensMatch
			if params.SearchOperator == common_filters.SearchOperatorAnd {
				minimumOrTokensMatchByTokenization = len(queryTerms)
			}
			if minimumOrTokensMatchByTokenization < minimumOrTokensMatch {
				minimumOrTokensMatch = minimumOrTokensMatchByTokenization
			}
		}
	}

	results := make([]*terms.Term, len(allRequests))

	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU)

	for _, request := range allRequests {
		term := request.term
		termId := request.termId
		propNames := request.propertyNames
		duplicateBoost := request.duplicateTextBoost

		eg.Go(func() (err error) {
			defer func() {
				p := recover()
				if p != nil {
					b.logger.
						WithField("query_term", term).
						WithField("prop_names", propNames).
						WithField("has_filter", filterDocIds != nil).
						Errorf("panic: %v", p)
					debug.PrintStack()
					err = fmt.Errorf("an internal error occurred during BM25 search")
				}
			}()

			termResult, termErr := b.createTerm(N, filterDocIds, term, termId, propNames, propertyBoosts, duplicateBoost, ctx)
			if termErr != nil {
				err = termErr
				return
			}
			results[termId] = termResult
			return
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	// all results. Sum up the length of the results from all terms to get an upper bound of how many results there are
	if limit == 0 {
		for _, res := range results {
			if res != nil {
				limit += len(res.Data)
			}
		}
	}

	resultsNonNil := make([]terms.TermInterface, 0, len(results))
	for _, res := range results {
		if res != nil {
			resultsNonNil = append(resultsNonNil, res)
		}
	}

	combinedTerms := &terms.Terms{
		T:     resultsNonNil,
		Count: len(allRequests),
	}

	topKHeap := lsmkv.DoWand(limit, combinedTerms, averagePropLength, params.AdditionalExplanations, minimumOrTokensMatch)

	return b.getTopKObjects(topKHeap, params.AdditionalExplanations, allQueryTerms, additional)
}

func (b *BM25Searcher) removeStopwordsFromQueryTerms(queryTerms []string,
	duplicateBoost []int, detector *stopwords.Detector,
) ([]string, []int) {
	if detector == nil || len(queryTerms) == 0 {
		return queryTerms, duplicateBoost
	}

	i := 0
WordLoop:
	for {
		if i == len(queryTerms) {
			return queryTerms, duplicateBoost
		}
		queryTerm := queryTerms[i]
		if detector.IsStopword(queryTerm) {
			queryTerms[i] = queryTerms[len(queryTerms)-1]
			queryTerms = queryTerms[:len(queryTerms)-1]
			duplicateBoost[i] = duplicateBoost[len(duplicateBoost)-1]
			duplicateBoost = duplicateBoost[:len(duplicateBoost)-1]

			continue WordLoop
		}

		i++
	}
}

func (b *BM25Searcher) getTopKObjects(topKHeap *priorityqueue.Queue[[]*terms.DocPointerWithScore], additionalExplanations bool,
	allRequests []string, additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	objectsBucket := b.store.Bucket(helpers.ObjectsBucketLSM)
	scores := make([]float32, 0, topKHeap.Len())
	ids := make([]uint64, 0, topKHeap.Len())
	explanations := make([][]*terms.DocPointerWithScore, 0, topKHeap.Len())
	for topKHeap.Len() > 0 {
		res := topKHeap.Pop()
		ids = append(ids, res.ID)
		scores = append(scores, res.Dist)
		explanations = append(explanations, res.Value)
	}

	objs, err := storobj.ObjectsByDocID(objectsBucket, ids, additional, nil, b.logger)
	if err != nil {
		return objs, nil, errors.Errorf("objects loading")
	}

	// handle case that an object was removed
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
	}

	if additionalExplanations {
		for k := range objs {
			// add score explanation
			if objs[k].AdditionalProperties() == nil {
				objs[k].Object.Additional = make(map[string]interface{})
			}
			for j, result := range explanations[k] {
				if result == nil {
					continue
				}
				queryTerm := allRequests[j]
				objs[k].Object.Additional["BM25F_"+queryTerm+"_frequency"] = result.Frequency
				objs[k].Object.Additional["BM25F_"+queryTerm+"_propLength"] = result.PropLength
			}
		}
	}

	return objs, scores, nil
}

func (b *BM25Searcher) getTopKIds(topKHeap *priorityqueue.Queue[[]*terms.DocPointerWithScore]) ([]uint64, []float32, [][]*terms.DocPointerWithScore, error) {
	scores := make([]float32, 0, topKHeap.Len())
	ids := make([]uint64, 0, topKHeap.Len())
	explanations := make([][]*terms.DocPointerWithScore, 0, topKHeap.Len())
	for topKHeap.Len() > 0 {
		res := topKHeap.Pop()
		ids = append(ids, res.ID)
		scores = append(scores, res.Dist)
		if res.Value != nil {
			explanations = append(explanations, res.Value)
		}
	}
	return ids, scores, explanations, nil
}

func (b *BM25Searcher) createTerm(N float64, filterDocIds helpers.AllowList, query string, queryTermIndex int, propertyNames []string, propertyBoosts map[string]float32, duplicateTextBoost int, ctx context.Context) (*terms.Term, error) {
	termResult := terms.NewTerm(query, queryTermIndex, float32(1.0), b.config)

	var filteredDocIDs *sroar.Bitmap
	var filteredDocIDsThread []*sroar.Bitmap
	if filterDocIds != nil {
		filteredDocIDs = sroar.NewBitmap() // to build the global n if there is a filter
		filteredDocIDsThread = make([]*sroar.Bitmap, len(propertyNames))
	}

	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU)

	allMsAndProps := make([][]terms.DocPointerWithScore, len(propertyNames))
	for i, propName := range propertyNames {
		i := i
		propName := propName

		eg.Go(
			func() error {
				bucket := b.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
				if bucket == nil {
					return fmt.Errorf("could not find bucket for property %v", propName)
				}
				preM, err := bucket.DocPointerWithScoreList(ctx, []byte(query), propertyBoosts[propName])
				if err != nil {
					return err
				}

				var m []terms.DocPointerWithScore
				if filterDocIds != nil {
					if filteredDocIDsThread[i] == nil {
						filteredDocIDsThread[i] = sroar.NewBitmap()
					}
					m = make([]terms.DocPointerWithScore, 0, len(preM))
					for _, val := range preM {
						docID := val.Id
						if filterDocIds.Contains(docID) {
							m = append(m, val)
						} else {
							filteredDocIDsThread[i].Set(docID)
						}
					}
				} else {
					m = preM
				}

				allMsAndProps[i] = m
				return nil
			},
		)
	}
	if err := eg.Wait(); err != nil {
		return termResult, err
	}

	if filterDocIds != nil {
		for _, docIDs := range filteredDocIDsThread {
			if docIDs != nil {
				filteredDocIDs.OrConc(docIDs, concurrency.SROAR_MERGE)
			}
		}
	}

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
		if filterDocIds != nil {
			n += float64(filteredDocIDs.GetCardinality())
		}
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
					b.logger.Warnf("Skipping pair in BM25: Index %d is out of range for key %d, length %d.", ind, key, len(docMapPairs))
					continue
				}
				if ind < len(docMapPairs) && docMapPairs[ind].Id != key {
					b.logger.Warnf("Skipping pair in BM25: id at %d in doc map pairs, %d, differs from current key, %d", ind, docMapPairs[ind].Id, key)
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
	if filterDocIds != nil {
		n += float64(filteredDocIDs.GetCardinality())
	}
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

func PropertyHasSearchableIndex(class *models.Class, tentativePropertyName string) bool {
	if class == nil {
		return false
	}

	propertyName := strings.Split(tentativePropertyName, "^")[0]
	p, err := schema.GetPropertyByName(class, propertyName)
	if err != nil {
		return false
	}
	return HasSearchableIndex(p)
}

func (b *BM25Searcher) GetBucket(propName string) *lsmkv.Bucket {
	return b.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
}
