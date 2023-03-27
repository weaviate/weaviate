//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"golang.org/x/sync/errgroup"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/priorityqueue"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/propertyspecific"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

type BM25Searcher struct {
	config        schema.BM25Config
	store         *lsmkv.Store
	schema        schema.Schema
	rowCache      cacher
	classSearcher ClassSearcher // to allow recursive searches on ref-props
	propIndices   propertyspecific.Indices
	deletedDocIDs DeletedDocIDChecker
	propLengths   propLengthRetriever
	logger        logrus.FieldLogger
	shardVersion  uint16
}

type propLengthRetriever interface {
	PropertyMean(prop string) (float32, error)
}

func NewBM25Searcher(config schema.BM25Config, store *lsmkv.Store, schema schema.Schema,
	rowCache cacher, propIndices propertyspecific.Indices,
	classSearcher ClassSearcher, deletedDocIDs DeletedDocIDChecker,
	propLengths propLengthRetriever, logger logrus.FieldLogger,
	shardVersion uint16,
) *BM25Searcher {
	return &BM25Searcher{
		config:        config,
		store:         store,
		schema:        schema,
		rowCache:      rowCache,
		propIndices:   propIndices,
		classSearcher: classSearcher,
		deletedDocIDs: deletedDocIDs,
		propLengths:   propLengths,
		logger:        logger.WithField("action", "bm25_search"),
		shardVersion:  shardVersion,
	}
}

func (b *BM25Searcher) BM25F(ctx context.Context, filterDocIds helpers.AllowList, className schema.ClassName, limit int,
	keywordRanking searchparams.KeywordRanking,
	filter *filters.LocalFilter, sort []filters.Sort, additional additional.Properties,
	objectByIndexID func(index uint64) *storobj.Object,
) ([]*storobj.Object, []float32, error) {
	// WEAVIATE-471 - If a property is not searchable, return an error
	for _, property := range keywordRanking.Properties {
		if !schema.PropertyIsIndexed(b.schema.Objects, string(className), property) {
			return nil, nil, errors.New("Property " + property + " is not indexed.  Please choose another property or add an index to this property")
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

// Objects returns a list of full objects
func (b *BM25Searcher) Objects(ctx context.Context, filterDocIds helpers.AllowList, limit int,
	keywordRanking searchparams.KeywordRanking,
	filter *filters.LocalFilter, sort []filters.Sort, additional additional.Properties,
	className schema.ClassName,
) ([]*storobj.Object, []float32, error) {
	class, err := schema.GetClassByName(b.schema.Objects, string(className))
	if err != nil {
		return nil, []float32{}, errors.Wrap(err, "get class by name")
	}
	property := keywordRanking.Properties[0]
	p, err := schema.GetPropertyByName(class, property)
	if err != nil {
		return nil, []float32{}, errors.Wrap(err, "read property from class")
	}
	indexed := p.IndexInverted

	if indexed == nil || *indexed {
		keywordRanking.Properties = keywordRanking.Properties[:1]
		return b.wand(ctx, filterDocIds, class, keywordRanking, limit)
	} else {
		return []*storobj.Object{}, []float32{}, nil
	}
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
	// Text, string and field.
	// For the first two the query is tokenized accordingly and for the last one the full query is used. The respective
	// properties are then searched for the search terms and the results at the end are combined using WAND
	queryTextTerms, duplicateTextBoost := helpers.TokenizeTextAndCountDuplicates(params.Query)
	queryTextTerms, duplicateTextBoost = b.removeStopwordsFromQueryTerms(queryTextTerms, duplicateTextBoost, stopWordDetector)
	// No stopword filtering for strings as they should retain the value as is
	queryStringTerms, duplicateStringBoost := helpers.TokenizeStringAndCountDuplicates(params.Query)

	propertyNamesFullQuery := make([]string, 0)
	propertyNamesText := make([]string, 0)
	propertyNamesString := make([]string, 0)
	propertyBoosts := make(map[string]float32, len(params.Properties))

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

		propMean, err := b.propLengths.PropertyMean(property)
		if err != nil {
			return nil, nil, err
		}
		averagePropLength += float64(propMean)

		prop, err := schema.GetPropertyByName(class, property)
		if err != nil {
			return nil, nil, err
		}

		if prop.Tokenization == "word" {
			if prop.DataType[0] == "text" || prop.DataType[0] == "text[]" {
				propertyNamesText = append(propertyNamesText, property)
			} else if prop.DataType[0] == "string" || prop.DataType[0] == "string[]" {
				propertyNamesString = append(propertyNamesString, property)
			} else {
				return nil, nil, fmt.Errorf("cannot handle datatype %v", prop.DataType[0])
			}
		} else {
			propertyNamesFullQuery = append(propertyNamesFullQuery, property)
		}
	}

	averagePropLength = averagePropLength / float64(len(params.Properties))

	// preallocate the results (+1 is for full query)
	fullQueryLength := 0
	if len(propertyNamesFullQuery) > 0 {
		fullQueryLength = 1
	}
	stringLength := 0
	if len(propertyNamesString) > 0 {
		stringLength = len(queryStringTerms)
	}
	textLength := 0
	if len(propertyNamesText) > 0 {
		textLength = len(queryTextTerms)
	}
	lengthAllResults := textLength + stringLength + fullQueryLength
	results := make(terms, lengthAllResults)
	indices := make([]map[uint64]int, lengthAllResults)

	var eg errgroup.Group
	if len(propertyNamesText) > 0 {
		for i := range queryTextTerms {
			queryTerm := queryTextTerms[i]
			j := i
			eg.Go(func() error {
				termResult, docIndices, err := b.createTerm(N, filterDocIds, queryTerm, propertyNamesText, propertyBoosts, duplicateTextBoost[j], params.AdditionalExplanations)
				if err != nil {
					return err
				}
				results[j] = termResult
				indices[j] = docIndices
				return nil
			})
		}
	}

	if len(propertyNamesString) > 0 {
		for i := range queryStringTerms {
			queryTerm := queryStringTerms[i]
			ind := i
			j := ind + textLength

			eg.Go(func() error {
				termResult, docIndices, err := b.createTerm(N, filterDocIds, queryTerm, propertyNamesString, propertyBoosts, duplicateStringBoost[ind], params.AdditionalExplanations)
				if err != nil {
					return err
				}
				results[j] = termResult
				indices[j] = docIndices
				return nil
			})
		}
	}

	if len(propertyNamesFullQuery) > 0 {
		lengthPreviousResults := stringLength + textLength
		eg.Go(func() error {
			termResult, docIndices, err := b.createTerm(N, filterDocIds, params.Query, propertyNamesFullQuery, propertyBoosts, 1, params.AdditionalExplanations)
			if err != nil {
				return err
			}
			results[lengthPreviousResults] = termResult
			indices[lengthPreviousResults] = docIndices
			return nil
		})
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

func (b *BM25Searcher) getTopKObjects(topKHeap *priorityqueue.Queue, results terms, indices []map[uint64]int, additionalExplanations bool) ([]*storobj.Object, []float32, error) {
	objectsBucket := b.store.Bucket(helpers.ObjectsBucketLSM)
	if objectsBucket == nil {
		return nil, nil, errors.Errorf("objects bucket not found")
	}

	objects := make([]*storobj.Object, 0, topKHeap.Len())
	scores := make([]float32, 0, topKHeap.Len())

	buf := make([]byte, 8)
	for topKHeap.Len() > 0 {
		res := topKHeap.Pop()
		scores = append(scores, res.Dist)
		binary.LittleEndian.PutUint64(buf, res.ID)
		objectByte, err := objectsBucket.GetBySecondary(0, buf)
		if err != nil {
			return nil, nil, err
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
					obj.Object.Additional["BM25F_"+queryTerm+"_frequency"] = result.data[termIndice].frequency
					obj.Object.Additional["BM25F_"+queryTerm+"_propLength"] = result.data[termIndice].propLength
				}
			}
		}
		objects = append(objects, obj)
	}
	return objects, scores, nil
}

func (b *BM25Searcher) getTopKHeap(limit int, results terms, averagePropLength float64) *priorityqueue.Queue {
	topKHeap := priorityqueue.NewMin(limit)
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
	filteredDocIDs := sroar.NewBitmap() // to build the global n if there is a filter

	allMsAndProps := make(AllMapPairsAndPropName, 0, len(propertyNames))
	for _, propName := range propertyNames {

		bucket := b.store.Bucket(helpers.BucketFromPropNameLSM(propName))
		if bucket == nil {
			return termResult, nil, fmt.Errorf("could not find bucket for property %v", propName)
		}
		preM, err := bucket.MapList([]byte(query))
		if err != nil {
			return termResult, nil, err
		}

		var m []lsmkv.MapPair
		if filterDocIds != nil {
			m = make([]lsmkv.MapPair, 0, len(preM))
			for _, val := range preM {
				docID := binary.BigEndian.Uint64(val.Key)
				if filterDocIds.Contains(docID) {
					m = append(m, val)
				} else {
					filteredDocIDs.Set(docID)
				}
			}
		} else {
			m = preM
		}
		if len(m) == 0 {
			continue
		}

		allMsAndProps = append(allMsAndProps, MapPairsAndPropName{MapPairs: m, propname: propName})
	}

	// sort ascending, this code has two effects
	// 1) We can skip writing the indices from the last property to the map (see next comment). Therefore, having the
	//    biggest property at the end will save us most writes on average
	// 2) For the first property all entries are new, and we can create the map with the respective size. When choosing
	//    the second-biggest entry as the first property we save additional allocations later
	sort.Sort(allMsAndProps)
	if len(allMsAndProps) > 2 {
		allMsAndProps[len(allMsAndProps)-2], allMsAndProps[0] = allMsAndProps[0], allMsAndProps[len(allMsAndProps)-2]
	}

	var docMapPairs []docPointerWithScore = nil
	var docMapPairsIndices map[uint64]int = nil
	for i, mAndProps := range allMsAndProps {
		m := mAndProps.MapPairs
		propName := mAndProps.propname

		// The indices are needed for two things:
		// a) combining the results of different properties
		// b) Retrieve additional information that helps to understand the results when debugging. The retrieval is done
		//    in a later step, after it is clear which objects are the most relevant
		//
		// When b) is not needed the results from the last property do not need to be added to the index-map as there
		// won't be any follow-up combinations.
		includeIndicesForLastElement := false
		if additionalExplanations || i < len(allMsAndProps)-1 {
			includeIndicesForLastElement = true
		}

		// only create maps/slices if we know how many entries there are
		if docMapPairs == nil {
			docMapPairs = make([]docPointerWithScore, 0, len(m))
			docMapPairsIndices = make(map[uint64]int, len(m))
			for k, val := range m {
				freqBits := binary.LittleEndian.Uint32(val.Value[0:4])
				propLenBits := binary.LittleEndian.Uint32(val.Value[4:8])
				docMapPairs = append(docMapPairs,
					docPointerWithScore{
						id:         binary.BigEndian.Uint64(val.Key),
						frequency:  math.Float32frombits(freqBits) * propertyBoosts[propName],
						propLength: math.Float32frombits(propLenBits),
					})
				if includeIndicesForLastElement {
					docMapPairsIndices[binary.BigEndian.Uint64(val.Key)] = k
				}
			}
		} else {
			for _, val := range m {
				key := binary.BigEndian.Uint64(val.Key)
				ind, ok := docMapPairsIndices[key]
				freqBits := binary.LittleEndian.Uint32(val.Value[0:4])
				propLenBits := binary.LittleEndian.Uint32(val.Value[4:8])
				if ok {
					docMapPairs[ind].propLength += math.Float32frombits(propLenBits)
					docMapPairs[ind].frequency += math.Float32frombits(freqBits) * propertyBoosts[propName]
				} else {
					docMapPairs = append(docMapPairs,
						docPointerWithScore{
							id:         binary.BigEndian.Uint64(val.Key),
							frequency:  math.Float32frombits(freqBits) * propertyBoosts[propName],
							propLength: math.Float32frombits(propLenBits),
						})
					if includeIndicesForLastElement {
						docMapPairsIndices[binary.BigEndian.Uint64(val.Key)] = len(docMapPairs) - 1 // current last entry
					}
				}
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
