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
	"strconv"
	"strings"

	"github.com/weaviate/weaviate/entities/additional"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
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

	objs, scores, err := b.wand(ctx, filterDocIds, class, keywordRanking, limit, additional)
	if err != nil {
		return nil, nil, errors.Wrap(err, "wand")
	}

	return objs, scores, nil
}

func (b *BM25Searcher) GetPropertyLengthTracker() *JsonShardMetaData {
	return b.propLenTracker.(*JsonShardMetaData)
}

func (b *BM25Searcher) wand(
	ctx context.Context, filterDocIds helpers.AllowList, class *models.Class, params searchparams.KeywordRanking, limit int,
	additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	N := float64(b.store.Bucket(helpers.ObjectsBucketLSM).Count())

	var stopWordDetector *stopwords.Detector
	if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.Stopwords != nil {
		var err error
		stopWordDetector, err = stopwords.NewDetectorFromConfig(*(class.InvertedIndexConfig.Stopwords))
		if err != nil {
			return nil, nil, err
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
			return nil, nil, err
		}
		averagePropLength += float64(propMean)

		prop, err := schema.GetPropertyByName(class, property)
		if err != nil {
			return nil, nil, err
		}

		switch dt, _ := schema.AsPrimitive(prop.DataType); dt {
		case schema.DataTypeText, schema.DataTypeTextArray:
			if _, exists := propNamesByTokenization[prop.Tokenization]; !exists {
				return nil, nil, fmt.Errorf("cannot handle tokenization '%v' of property '%s'",
					prop.Tokenization, prop.Name)
			}
			propNamesByTokenization[prop.Tokenization] = append(propNamesByTokenization[prop.Tokenization], property)
		default:
			return nil, nil, fmt.Errorf("cannot handle datatype '%v' of property '%s'", dt, prop.Name)
		}
	}

	averagePropLength = averagePropLength / float64(len(params.Properties))

	// 100 is a reasonable expected capacity for the total number of terms to query.
	allTerms := make([]string, 0, 100)
	currentTerm := 0

	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU)

	for _, tokenization := range helpers.Tokenizations {
		propNames := propNamesByTokenization[tokenization]
		if len(propNames) > 0 {
			queryTerms, duplicateBoosts := helpers.TokenizeAndCountDuplicates(tokenization, params.Query)
			if tokenization == models.PropertyTokenizationWord {
				queryTerms, _ = b.removeStopwordsFromQueryTerms(
					queryTerms, duplicateBoosts, stopWordDetector)
			}
			allTerms = append(allTerms, queryTerms...)
		}
	}

	results := make([]terms.Term, len(allTerms))

	for _, tokenization := range helpers.Tokenizations {
		propNames := propNamesByTokenization[tokenization]
		if len(propNames) > 0 {
			queryTerms, duplicateBoosts := helpers.TokenizeAndCountDuplicates(tokenization, params.Query)

			// stopword filtering for word tokenization
			if tokenization == models.PropertyTokenizationWord {
				queryTerms, duplicateBoosts = b.removeStopwordsFromQueryTerms(
					queryTerms, duplicateBoosts, stopWordDetector)
			}

			for i := range queryTerms {
				j := i
				term := allTerms[currentTerm]
				currentTerm++
				termId := (currentTerm - 1)

				eg.Go(func() (err error) {
					termResult, termErr := b.createTerm(ctx, N, filterDocIds, term, termId, propNames,
						propertyBoosts, duplicateBoosts[j], params.AdditionalExplanations)
					if termErr != nil {
						err = termErr
						return
					}
					results[termId] = &termResult
					return
				}, "query_term", queryTerms[j], "prop_names", propNames, "has_filter", filterDocIds != nil)
			}
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	// remove gaps
	for i := range results {
		if i >= len(results) {
			break // in case the length of results changed during the loop
		}
		if results[i] == nil {
			if i == len(results)-1 {
				results = results[:i]
			} else {
				results = append(results[:i], results[i+1:]...)
			}
		}
	}

	// all results. Sum up the length of the results from all terms to get an upper bound of how many results there are
	if limit == 0 {
		for _, res := range results {
			limit += res.Count()
		}
	}

	finalTerms := terms.Terms{
		T:     results,
		Count: len(allTerms),
	}

	topKHeap := b.getTopKHeap(limit, finalTerms, averagePropLength, params.AdditionalExplanations)
	return b.getTopKObjects(topKHeap, params.AdditionalExplanations, additional, allTerms)
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
	additional additional.Properties, allTerms []string,
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
				queryTerm := allTerms[j]
				objs[k].Object.Additional["BM25F_"+queryTerm+"_frequency"] = result.Frequency
				objs[k].Object.Additional["BM25F_"+queryTerm+"_propLength"] = result.PropLength
			}

		}
	}

	return objs, scores, nil
}

func (b *BM25Searcher) getTopKHeap(limit int, results terms.Terms, averagePropLength float64, additionalExplanations bool,
) *priorityqueue.Queue[[]*terms.DocPointerWithScore] {
	topKHeap := priorityqueue.NewMin[[]*terms.DocPointerWithScore](limit)
	worstDist := float64(-10000) // tf score can be negative
	results.FullSort()
	for {
		if results.CompletelyExhausted() || results.Pivot(worstDist) {
			return topKHeap
		}

		id, score, docInfos := results.ScoreNext(averagePropLength, b.config, additionalExplanations)

		if topKHeap.Len() < limit || topKHeap.Top().Dist < float32(score) {
			topKHeap.InsertWithValue(id, float32(score), docInfos)
			for topKHeap.Len() > limit {
				topKHeap.Pop()
			}
			// only update the worst distance when the queue is full, otherwise results can be missing if the first
			// entry that is checked already has a very high score
			if topKHeap.Len() >= limit {
				worstDist = float64(topKHeap.Top().Dist)
			}
		}
	}
}

func (b *BM25Searcher) createTerm(ctx context.Context, N float64, filterDocIds helpers.AllowList, query string, queryTermIndex int,
	propertyNames []string, propertyBoosts map[string]float32, duplicateTextBoost int,
	additionalExplanations bool,
) (term, error) {
	termResult := term{queryTerm: query, queryTermIndex: queryTermIndex}
	filteredDocIDs := sroar.NewBitmap() // to build the global n if there is a filter
	filteredDocIDsThread := make([]*sroar.Bitmap, len(propertyNames))

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
				filteredDocIDs.Or(docIDs)
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
		termResult.exhausted = true
		return termResult, nil
	}

	if len(allMsAndProps) == 1 {
		termResult.data = allMsAndProps[0]
		termResult.idf = math.Log(float64(1)+(N-float64(len(termResult.data))+0.5)/(float64(len(termResult.data))+0.5)) * float64(duplicateTextBoost)
		termResult.posPointer = 0
		termResult.idPointer = termResult.data[0].Id
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
		termResult.exhausted = true
		return termResult, nil
	}
	termResult.data = docMapPairs

	n := float64(len(docMapPairs))
	if filterDocIds != nil {
		n += float64(filteredDocIDs.GetCardinality())
	}
	termResult.idf = math.Log(float64(1)+(N-n+0.5)/(n+0.5)) * float64(duplicateTextBoost)

	// catch special case where there are no results and would panic termResult.data[0].id
	// related to #4125
	if len(termResult.data) == 0 {
		termResult.posPointer = 0
		termResult.idPointer = 0
		termResult.exhausted = true
		return termResult, nil
	}

	termResult.posPointer = 0
	termResult.idPointer = termResult.data[0].Id
	return termResult, nil
}

type term struct {
	// doubles as max impact (with tf=1, the max impact would be 1*idf), if there
	// is a boost for a queryTerm, simply apply it here once
	idf float64

	idPointer      uint64
	posPointer     uint64
	data           []terms.DocPointerWithScore
	exhausted      bool
	queryTerm      string
	queryTermIndex int
}

func (t *term) ScoreAndAdvance(averagePropLength float64, config schema.BM25Config) (uint64, float64, terms.DocPointerWithScore) {
	id := t.idPointer
	pair := t.data[t.posPointer]
	freq := float64(pair.Frequency)
	tf := freq / (freq + config.K1*(1-config.B+config.B*float64(pair.PropLength)/averagePropLength))

	// advance
	t.posPointer++
	if t.posPointer >= uint64(len(t.data)) {
		t.exhausted = true
	} else {
		t.idPointer = t.data[t.posPointer].Id
	}

	return id, tf * t.idf, pair
}

func (t *term) AdvanceAtLeast(minID uint64) {
	for t.idPointer < minID {
		t.posPointer++
		if t.posPointer >= uint64(len(t.data)) {
			t.exhausted = true
			return
		}
		t.idPointer = t.data[t.posPointer].Id
	}
}

func (t *term) IsExhausted() bool {
	return t.exhausted
}

func (t *term) IdPointer() uint64 {
	return t.idPointer
}

func (t *term) IDF() float64 {
	return t.idf
}

func (t *term) QueryTerm() string {
	return t.queryTerm
}

func (t *term) QueryTermIndex() int {
	return t.queryTermIndex
}

func (t *term) Count() int {
	return len(t.data)
}

type MapPairsAndPropName struct {
	propname string
	MapPairs []lsmkv.MapPair
}

type AllMapPairsAndPropName []MapPairsAndPropName

// provide sort interface
func (m AllMapPairsAndPropName) Len() int {
	return len(m)
}

func (m AllMapPairsAndPropName) Less(i, j int) bool {
	return len(m[i].MapPairs) < len(m[j].MapPairs)
}

func (m AllMapPairsAndPropName) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
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
