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
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/weaviate/weaviate/entities/additional"

	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
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

	uniqueTerms := make(map[string]struct{})
	for _, tokenization := range helpers.Tokenizations {
		propNames := propNamesByTokenization[tokenization]
		if len(propNames) > 0 {
			queryTerms, duplicateBoosts := helpers.TokenizeAndCountDuplicates(tokenization, params.Query)

			// stopword filtering for word tokenization
			if tokenization == models.PropertyTokenizationWord {
				queryTerms, _ = b.removeStopwordsFromQueryTerms(
					queryTerms, duplicateBoosts, stopWordDetector)
			}

			for i := range queryTerms {
				uniqueTerms[queryTerms[i]] = struct{}{}
			}
		}
	}

	results := make(map[string]*terms, 100)

	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU)

	var resultsLock sync.Mutex

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

				eg.Go(func() (err error) {
					termResult, termErr := b.createTerm(ctx, N, filterDocIds, queryTerms[j], propNames,
						propertyBoosts, duplicateBoosts[j], params.AdditionalExplanations)
					if termErr != nil {
						err = termErr
						return
					}
					resultsLock.Lock()
					for _, term := range termResult {
						if term == nil {
							continue
						}
						if _, exists := results[term.property]; !exists {
							results[term.property] = &terms{}
						}
						results[term.property].data = append(results[term.property].data, term)
					}
					resultsLock.Unlock()
					return
				}, "query_term", queryTerms[j], "prop_names", propNames, "has_filter", filterDocIds != nil)
			}
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	topKResults := make(map[string][]*storobj.Object, len(results))
	topKScores := make(map[string][]float32, len(results))

	eg = enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(len(results))
	for propName, result := range results {
		propName := propName
		result := result

		eg.Go(func() error {
			topKHeap := b.getTopKHeap(limit, result, averagePropLength)
			res, sco, err := b.getTopKObjects(topKHeap, additional)
			if err != nil {
				return err
			}
			resultsLock.Lock()
			topKResults[propName] = res
			topKScores[propName] = sco
			resultsLock.Unlock()
			return nil
		}, "prop_name", propName)
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	if len(topKResults) == 0 {
		return nil, nil, nil
	}

	// combine results
	combinedResults := make([]*storobj.Object, 0, limit)
	combinedScores := make([]float32, 0, limit)
	for _, res := range topKResults {
		combinedResults = append(combinedResults, res...)
	}
	for _, sco := range topKScores {
		combinedScores = append(combinedScores, sco...)
	}

	return combinedResults, combinedScores, nil
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

func (b *BM25Searcher) getTopKObjects(topKHeap *priorityqueue.Queue[any], additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	objectsBucket := b.store.Bucket(helpers.ObjectsBucketLSM)
	if objectsBucket == nil {
		return nil, nil, errors.Errorf("objects bucket not found")
	}

	scores := make([]float32, 0, topKHeap.Len())
	ids := make([]uint64, 0, topKHeap.Len())
	for topKHeap.Len() > 0 {
		res := topKHeap.Pop()
		ids = append(ids, res.ID)
		scores = append(scores, res.Dist)
	}

	objs, err := storobj.ObjectsByDocID(objectsBucket, ids, additional, b.logger)
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
		ids = idsTmp
	}

	return objs, scores, nil
}

func (b *BM25Searcher) getTopKHeap(limit int, results *terms, averagePropLength float64,
) *priorityqueue.Queue[any] {
	topKHeap := priorityqueue.NewMin[any](limit)
	worstDist := float64(-10000) // tf score can be negative
	sort.Sort(results)
	for {
		if results.completelyExhausted() || results.pivot(worstDist) {
			return topKHeap
		}

		id, score := results.scoreNext(averagePropLength, b.config)

		if topKHeap.Len() < limit || topKHeap.Top().Dist < float32(score) {
			topKHeap.Insert(id, float32(score))
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

func (b *BM25Searcher) createTerm(ctx context.Context, N float64, filterDocIds helpers.AllowList, query string,
	propertyNames []string, propertyBoosts map[string]float32, duplicateTextBoost int,
	additionalExplanations bool,
) ([]*term, error) {
	filteredDocIDs := sroar.NewBitmap() // to build the global n if there is a filter
	filterLock := sync.Mutex{}
	eg := enterrors.NewErrorGroupWrapper(b.logger)
	eg.SetLimit(_NUMCPU)

	terms := make([]*term, len(propertyNames))
	for i, propName := range propertyNames {
		i := i
		propName := propName

		eg.Go(
			func() error {
				bucket := b.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
				if bucket == nil {
					return fmt.Errorf("could not find bucket for property %v", propName)
				}
				preM, err := bucket.MapList(ctx, []byte(query))
				if err != nil {
					return err
				}

				var m []lsmkv.MapPair
				if filterDocIds != nil {
					m = make([]lsmkv.MapPair, 0, len(preM))
					for _, val := range preM {
						docID := binary.BigEndian.Uint64(val.Key)
						if filterDocIds.Contains(docID) {
							m = append(m, val)
						} else {
							filterLock.Lock()
							filteredDocIDs.Set(docID)
							filterLock.Unlock()
						}
					}
				} else {
					m = preM
				}
				n := float64(len(m))
				if filterDocIds != nil {
					n += float64(filteredDocIDs.GetCardinality())
				}
				idf := math.Log(float64(1)+(N-n+0.5)/(n+0.5)) * float64(duplicateTextBoost)

				if len(m) == 0 {
					return nil
				}

				terms[i] = &term{
					posPointer: 0,
					idPointer:  m[0].Id(),
					queryTerm:  query,
					property:   propName,
					exhausted:  false,
					data:       m,
					idf:        idf,
					boost:      propertyBoosts[propName],
				}

				return nil
			},
		)
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	// remove gaps
	for i := range terms {
		if i >= len(terms) {
			break
		}
		if terms[i] == nil {
			if i == len(terms)-1 {
				terms = terms[:i]
			} else {
				terms = append(terms[:i], terms[i+1:]...)
			}
		}
	}

	return terms, nil
}

type term struct {
	// doubles as max impact (with tf=1, the max impact would be 1*idf), if there
	// is a boost for a queryTerm, simply apply it here once
	idf float64

	idPointer  uint64
	posPointer uint64
	data       []lsmkv.MapPair
	exhausted  bool
	queryTerm  string
	property   string
	boost      float32
}

func (t *term) scoreAndAdvance(averagePropLength float64, config schema.BM25Config) (uint64, float64) {
	id := t.idPointer
	pair := t.data[t.posPointer]
	freq := float64(pair.Frequency())
	tf := freq / (freq + config.K1*(1-config.B+config.B*float64(pair.PropLength())/averagePropLength))

	// advance
	t.posPointer++
	if t.posPointer >= uint64(len(t.data)) {
		t.exhausted = true
	} else {
		t.idPointer = t.data[t.posPointer].Id()
	}

	return id, tf * t.idf * float64(t.boost)
}

func (t *term) advanceAtLeast(minID uint64) {
	for t.idPointer < minID {
		t.posPointer++
		if t.posPointer >= uint64(len(t.data)) {
			t.exhausted = true
			return
		}
		t.idPointer = t.data[t.posPointer].Id()
	}
}

type terms struct {
	data []*term
}

func (t terms) completelyExhausted() bool {
	for i := range t.data {
		if !t.data[i].exhausted {
			return false
		}
	}
	return true
}

func (t terms) pivot(minScore float64) bool {
	minID, pivotPoint, abort := t.findMinID(minScore)
	if abort {
		return true
	}
	if pivotPoint == 0 {
		return false
	}

	t.advanceAllAtLeast(minID)
	sort.Sort(t)
	return false
}

func (t terms) advanceAllAtLeast(minID uint64) {
	for i := range t.data {
		t.data[i].advanceAtLeast(minID)
	}
}

func (t terms) findMinID(minScore float64) (uint64, int, bool) {
	cumScore := float64(0)
	i := 0
	for i < len(t.data) && len(t.data) > 0 {
		term := t.data[i]
		if term.exhausted {
			continue
		}
		cumScore += term.idf
		if cumScore >= minScore {
			return term.idPointer, i, false
		}
		i++
	}

	return 0, 0, true
}

func (t terms) findFirstNonExhausted() (int, bool) {
	for i := range t.data {
		if !t.data[i].exhausted {
			return i, true
		}
	}

	return -1, false
}

func (t terms) scoreNext(averagePropLength float64, config schema.BM25Config) (uint64, float64) {
	pos, ok := t.findFirstNonExhausted()
	if !ok {
		// done, nothing left to score
		return 0, 0
	}

	id := t.data[pos].idPointer
	var cumScore float64
	for i := 0; i < len(t.data); i++ {
		if t.data[i].idPointer != id || t.data[i].exhausted {
			continue
		}
		_, score := t.data[i].scoreAndAdvance(averagePropLength, config)
		cumScore += score
	}

	sort.Sort(t) // pointer was advanced in scoreAndAdvance

	return id, cumScore
}

// provide sort interface
func (t terms) Len() int {
	return len(t.data)
}

func (t terms) Less(i, j int) bool {
	return t.data[i].idPointer < t.data[j].idPointer
}

func (t terms) Swap(i, j int) {
	t.data[i], t.data[j] = t.data[j], t.data[i]
}

type MapPairsAndPropName struct {
	MapPairs []lsmkv.MapPair
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
