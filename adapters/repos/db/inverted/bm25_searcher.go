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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"golang.org/x/sync/errgroup"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"

	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

type BM25Searcher struct {
	config         schema.BM25Config
	store          *lsmkv.Store
	schema         schema.Schema
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
	schema schema.Schema, propIndices propertyspecific.Indices,
	classSearcher ClassSearcher, propLenTracker propLengthRetriever,
	logger logrus.FieldLogger, shardVersion uint16,
) *BM25Searcher {
	return &BM25Searcher{
		config:         config,
		store:          store,
		schema:         schema,
		propIndices:    propIndices,
		classSearcher:  classSearcher,
		propLenTracker: propLenTracker,
		logger:         logger.WithField("action", "bm25_search"),
		shardVersion:   shardVersion,
	}
}

func (b *BM25Searcher) BM25F(ctx context.Context, filterDocIds helpers.AllowList, className schema.ClassName, limit int, keywordRanking searchparams.KeywordRanking) ([]*storobj.Object, []float32, error) {
	// WEAVIATE-471 - If a property is not searchable, return an error
	for _, property := range keywordRanking.Properties {
		if !PropertyHasSearchableIndex(b.schema.Objects, string(className), property) {
			return nil, nil, inverted.NewMissingSearchableIndexError(property)
		}
	}
	class, err := schema.GetClassByName(b.schema.Objects, string(className))
	if err != nil {
		return nil, nil, err
	}

	objs, scores, err := b.wand(ctx, filterDocIds, class, keywordRanking, limit)
	if err != nil {
		return nil, nil, errors.Wrap(err, "wand")
	}

	return objs, scores, nil
}

func (b *BM25Searcher) GetPropertyLengthTracker() *JsonPropertyLengthTracker {
	return b.propLenTracker.(*JsonPropertyLengthTracker)
}

func (b *BM25Searcher) wand(
	ctx context.Context, filterDocIds helpers.AllowList, class *models.Class, params searchparams.KeywordRanking, limit int,
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

	propNamesByTokenization := map[string][]string{}
	propertyBoosts := make(map[string]float32, len(params.Properties))

	for _, tokenization := range helpers.Tokenizations {
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
	results := make(terms, 0, 100)
	indices := make([]map[uint64]int, 0, 100)

	var eg errgroup.Group
	eg.SetLimit(_NUMCPU)

	var resultsLock sync.Mutex

	for _, tokenization := range helpers.Tokenizations {
		propNames := propNamesByTokenization[tokenization]
		if len(propNames) > 0 {
			queryTerms, duplicateBoosts := helpers.TokenizeAndCountDuplicates(tokenization, params.Query)

			// stopword filtering for word tokenization
			if tokenization == models.PropertyTokenizationWord {
				queryTerms, duplicateBoosts = b.removeStopwordsFromQueryTerms(queryTerms, duplicateBoosts, stopWordDetector)
			}

			for i := range queryTerms {
				j := i

				eg.Go(func() (err error) {
					defer func() {
						p := recover()
						if p != nil {
							b.logger.
								WithField("query_term", queryTerms[j]).
								WithField("prop_names", propNames).
								WithField("has_filter", filterDocIds != nil).
								Errorf("panic: %v", p)
							debug.PrintStack()
							err = fmt.Errorf("an internal error occurred during BM25 search")
						}
					}()

					termResult, docIndices, termErr := b.createTerm(N, filterDocIds, queryTerms[j], propNames,
						propertyBoosts, duplicateBoosts[j], params.AdditionalExplanations)
					if termErr != nil {
						err = termErr
						return
					}
					resultsLock.Lock()
					results = append(results, termResult)
					indices = append(indices, docIndices)
					resultsLock.Unlock()
					return
				})
			}
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	// all results. Sum up the length of the results from all terms to get an upper bound of how many results there are
	if limit == 0 {
		for _, ind := range indices {
			limit += len(ind)
		}
	}

	// the results are needed in the original order to be able to locate frequency/property length for the top-results
	resultsOriginalOrder := make(terms, len(results))
	copy(resultsOriginalOrder, results)

	topKHeap := b.getTopKHeap(limit, results, averagePropLength)
	return b.getTopKObjects(topKHeap, resultsOriginalOrder, indices, params.AdditionalExplanations)
}

func (b *BM25Searcher) removeStopwordsFromQueryTerms(queryTerms []string, duplicateBoost []int, detector *stopwords.Detector) ([]string, []int) {
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

func (b *BM25Searcher) getTopKObjects(topKHeap *priorityqueue.Queue[any],
	results terms, indices []map[uint64]int, additionalExplanations bool,
) ([]*storobj.Object, []float32, error) {
	objectsBucket := b.store.Bucket(helpers.ObjectsBucketLSM)
	if objectsBucket == nil {
		return nil, nil, errors.Errorf("objects bucket not found")
	}

	objects := make([]*storobj.Object, 0, topKHeap.Len())
	scores := make([]float32, 0, topKHeap.Len())

	buf := make([]byte, 8)
	for topKHeap.Len() > 0 {
		res := topKHeap.Pop()
		binary.LittleEndian.PutUint64(buf, res.ID)
		objectByte, err := objectsBucket.GetBySecondary(0, buf)
		if err != nil {
			return nil, nil, err
		}

		// If there is a crash and WAL recovery, the inverted index may have objects that are not in the objects bucket.
		// This is an issue that needs to be fixed, but for now we need to reduce the huge amount of log messages that
		// are generated by this issue. Logging the first time we encounter a missing object in a query still resulted
		// in a huge amount of log messages and it will happen on all queries, so we not log at all for now.
		// The user has already been alerted about ppossible data loss when the WAL recovery happened.
		// TODO: consider deleting these entries from the inverted index and alerting the user
		if len(objectByte) == 0 {
			continue
		}

		obj, err := storobj.FromBinary(objectByte)
		if err != nil {
			return nil, nil, err
		}

		if additionalExplanations {
			// add score explanation
			if obj.AdditionalProperties() == nil {
				obj.Object.Additional = make(map[string]interface{})
			}
			for j, result := range results {
				if termIndice, ok := indices[j][res.ID]; ok {
					queryTerm := result.queryTerm
					if len(result.data) <= termIndice {
						b.logger.Warnf("Skipping object explanation in BM25: term indice %v is out of range for query term %v, length %d, id %v", termIndice, queryTerm, len(result.data), res.ID)
						continue
					}
					obj.Object.Additional["BM25F_"+queryTerm+"_frequency"] = result.data[termIndice].frequency
					obj.Object.Additional["BM25F_"+queryTerm+"_propLength"] = result.data[termIndice].propLength
				}
			}
		}
		objects = append(objects, obj)
		scores = append(scores, res.Dist)

	}
	return objects, scores, nil
}

func (b *BM25Searcher) getTopKHeap(limit int, results terms, averagePropLength float64,
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

func (b *BM25Searcher) createTerm(N float64, filterDocIds helpers.AllowList, query string, propertyNames []string, propertyBoosts map[string]float32, duplicateTextBoost int, additionalExplanations bool) (term, map[uint64]int, error) {
	termResult := term{queryTerm: query}

	var eg errgroup.Group
	eg.SetLimit(_NUMCPU)

	allMsAndProps := make(AllMapPairsAndPropName, len(propertyNames))
	filteredDocIDsThread := make([]*sroar.Bitmap, len(propertyNames))

	for i, propName := range propertyNames {
		i := i
		propName := propName

		eg.Go(
			func() error {
				bucket := b.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
				if bucket == nil {
					return fmt.Errorf("could not find bucket for property %v", propName)
				}
				preM, err := bucket.MapList([]byte(query))
				if err != nil {
					return err
				}

				var m []lsmkv.MapPair
				if filterDocIds != nil {
					if filteredDocIDsThread[i] == nil {
						filteredDocIDsThread[i] = sroar.NewBitmap()
					}
					m = make([]lsmkv.MapPair, 0, len(preM))
					for _, val := range preM {
						docID := binary.BigEndian.Uint64(val.Key)
						if filterDocIds.Contains(docID) {
							m = append(m, val)
						} else {
							filteredDocIDsThread[i].Set(docID)
						}
					}
				} else {
					m = preM
				}

				allMsAndProps[i] = MapPairsAndPropName{MapPairs: m, propname: propName}
				return nil
			},
		)

	}

	if err := eg.Wait(); err != nil {
		return termResult, nil, err
	}

	filteredDocIDs := sroar.NewBitmap() // to build the global n if there is a filter
	if filterDocIds != nil {
		for _, docIDs := range filteredDocIDsThread {
			if docIDs != nil {
				filteredDocIDs.Or(docIDs)
			}
		}
	}

	indices := make([]int, len(allMsAndProps))

	var docMapPairs []docPointerWithScore = nil

	// The indices are needed for two things:
	// a) combining the results of different properties
	// b) Retrieve additional information that helps to understand the results when debugging. The retrieval is done
	//    in a later step, after it is clear which objects are the most relevant
	var docMapPairsIndices map[uint64]int = nil

	for {
		i := -1
		minKey := make([]byte, 8)
		for ti, mAndProps := range allMsAndProps {
			if indices[ti] >= len(mAndProps.MapPairs) {
				continue
			}
			ki := mAndProps.MapPairs[indices[ti]].Key
			if i == -1 || bytes.Compare(ki, minKey) < 0 {
				i = ti
				copy(minKey, ki)
			}
		}

		if i == -1 {
			break
		}

		m := allMsAndProps[i].MapPairs
		k := indices[i]
		val := m[indices[i]]

		indices[i]++

		propName := allMsAndProps[i].propname

		// only create maps/slices if we know how many entries there are
		if docMapPairs == nil {
			docMapPairs = make([]docPointerWithScore, 0, len(m))
			docMapPairsIndices = make(map[uint64]int, len(m))
			if len(val.Value) < 8 {
				b.logger.Warnf("Skipping pair in BM25: MapPair.Value should be 8 bytes long, but is %d.", len(val.Value))
				continue
			}
			freqBits := binary.LittleEndian.Uint32(val.Value[0:4])
			propLenBits := binary.LittleEndian.Uint32(val.Value[4:8])
			docMapPairs = append(docMapPairs,
				docPointerWithScore{
					id:         binary.BigEndian.Uint64(val.Key),
					frequency:  math.Float32frombits(freqBits) * propertyBoosts[propName],
					propLength: math.Float32frombits(propLenBits),
				})
			docMapPairsIndices[binary.BigEndian.Uint64(val.Key)] = k

		} else {
			if len(val.Value) < 8 {
				b.logger.Warnf("Skipping pair in BM25: MapPair.Value should be 8 bytes long, but is %d.", len(val.Value))
				continue
			}
			key := binary.BigEndian.Uint64(val.Key)
			ind, ok := docMapPairsIndices[key]
			freqBits := binary.LittleEndian.Uint32(val.Value[0:4])
			propLenBits := binary.LittleEndian.Uint32(val.Value[4:8])
			if ok {
				if ind >= len(docMapPairs) {
					// the index is not valid anymore, but the key is still in the map
					b.logger.Warnf("Skipping pair in BM25: Index %d is out of range for key %d, length %d.", ind, key, len(docMapPairs))
					continue
				}
				if ind < len(docMapPairs) && docMapPairs[ind].id != key {
					b.logger.Warnf("Skipping pair in BM25: id at %d in doc map pairs, %d, differs from current key, %d", ind, docMapPairs[ind].id, key)
					continue
				}

				docMapPairs[ind].propLength += math.Float32frombits(propLenBits)
				docMapPairs[ind].frequency += math.Float32frombits(freqBits) * propertyBoosts[propName]
			} else {
				docMapPairs = append(docMapPairs,
					docPointerWithScore{
						id:         binary.BigEndian.Uint64(val.Key),
						frequency:  math.Float32frombits(freqBits) * propertyBoosts[propName],
						propLength: math.Float32frombits(propLenBits),
					})
				docMapPairsIndices[binary.BigEndian.Uint64(val.Key)] = len(docMapPairs) - 1 // current last entry

			}

		}
	}
	if docMapPairs == nil {
		termResult.exhausted = true
		return termResult, docMapPairsIndices, nil
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
		return termResult, docMapPairsIndices, nil
	}

	termResult.posPointer = 0
	termResult.idPointer = termResult.data[0].id
	return termResult, docMapPairsIndices, nil
}

type term struct {
	// doubles as max impact (with tf=1, the max impact would be 1*idf), if there
	// is a boost for a queryTerm, simply apply it here once
	idf float64

	idPointer  uint64
	posPointer uint64
	data       []docPointerWithScore
	exhausted  bool
	queryTerm  string
}

func (t *term) scoreAndAdvance(averagePropLength float64, config schema.BM25Config) (uint64, float64) {
	id := t.idPointer
	pair := t.data[t.posPointer]
	freq := float64(pair.frequency)
	tf := freq / (freq + config.K1*(1-config.B+config.B*float64(pair.propLength)/averagePropLength))

	// advance
	t.posPointer++
	if t.posPointer >= uint64(len(t.data)) {
		t.exhausted = true
	} else {
		t.idPointer = t.data[t.posPointer].id
	}

	return id, tf * t.idf
}

func (t *term) advanceAtLeast(minID uint64) {
	for t.idPointer < minID {
		t.posPointer++
		if t.posPointer >= uint64(len(t.data)) {
			t.exhausted = true
			return
		}
		t.idPointer = t.data[t.posPointer].id
	}
}

type terms []term

func (t terms) completelyExhausted() bool {
	for i := range t {
		if !t[i].exhausted {
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
	for i := range t {
		t[i].advanceAtLeast(minID)
	}
}

func (t terms) findMinID(minScore float64) (uint64, int, bool) {
	cumScore := float64(0)

	for i, term := range t {
		if term.exhausted {
			continue
		}
		cumScore += term.idf
		if cumScore >= minScore {
			return term.idPointer, i, false
		}
	}

	return 0, 0, true
}

func (t terms) findFirstNonExhausted() (int, bool) {
	for i := range t {
		if !t[i].exhausted {
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

	id := t[pos].idPointer
	var cumScore float64
	for i := pos; i < len(t); i++ {
		if t[i].idPointer != id || t[i].exhausted {
			continue
		}
		_, score := t[i].scoreAndAdvance(averagePropLength, config)
		cumScore += score
	}

	sort.Sort(t) // pointer was advanced in scoreAndAdvance

	return id, cumScore
}

// provide sort interface
func (t terms) Len() int {
	return len(t)
}

func (t terms) Less(i, j int) bool {
	return t[i].idPointer < t[j].idPointer
}

func (t terms) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
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

func PropertyHasSearchableIndex(schemaDefinition *models.Schema, className, tentativePropertyName string) bool {
	propertyName := strings.Split(tentativePropertyName, "^")[0]
	c, err := schema.GetClassByName(schemaDefinition, string(className))
	if err != nil {
		return false
	}
	p, err := schema.GetPropertyByName(c, propertyName)
	if err != nil {
		return false
	}
	return HasSearchableIndex(p)
}
