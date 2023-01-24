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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"

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

// Objects returns a list of full objects
func (b *BM25Searcher) Objects(ctx context.Context, limit int,
	keywordRanking *searchparams.KeywordRanking,
	filter *filters.LocalFilter, sort []filters.Sort, additional additional.Properties,
	className schema.ClassName,
) ([]*storobj.Object, []float32, error) {
	defer func() {
		err := recover()
		if err != nil {
			fmt.Println(err)
			debug.PrintStack()
		}
	}()

	c, err := schema.GetClassByName(b.schema.Objects, string(className))
	if err != nil {
		return nil, []float32{}, errors.Wrap(err,
			"get class by name")
	}
	property := keywordRanking.Properties[0]
	p, err := schema.GetPropertyByName(c, property)
	if err != nil {
		return nil, []float32{}, errors.Wrap(err, "read property from class")
	}
	indexed := p.IndexInverted

	if indexed == nil || *indexed {
		return b.wand(ctx, keywordRanking.Query, keywordRanking.Properties[:1], limit)
	} else {
		return []*storobj.Object{}, []float32{}, nil
	}
}

func (b *BM25Searcher) sort(ids docPointersWithScore) docPointersWithScore {
	// TODO: we can probably do this way smarter in a way that we immediately
	// skip anything worse the the current worst candidate
	sort.Slice(ids.docIDs, func(a, b int) bool {
		return ids.docIDs[a].score > ids.docIDs[b].score
	})
	return ids
}

func (b *BM25Searcher) retrieveScoreAndSortForSingleTerm(ctx context.Context,
	property, term string,
) (docPointersWithScore, error) {
	before := time.Now()
	ids, err := b.getIdsWithFrequenciesForTerm(ctx, property, term)
	if err != nil {
		return docPointersWithScore{}, errors.Wrap(err,
			"read doc ids and their frequencies from inverted index")
	}
	took := time.Since(before)
	b.logger.WithField("took", took).
		WithField("event", "retrieve_doc_ids").
		WithField("count", len(ids.docIDs)).
		WithField("term", term).
		Debugf("retrieve %d doc ids for term %q took %s", len(ids.docIDs),
			term, took)

	before = time.Now()
	if err := b.score(ids, property); err != nil {
		return docPointersWithScore{}, err
	}
	took = time.Since(before)
	b.logger.WithField("took", took).
		WithField("event", "score_doc_ids").
		WithField("count", len(ids.docIDs)).
		WithField("term", term).
		Debugf("score %d doc ids for term %q took %s", len(ids.docIDs),
			term, took)

	return ids, nil
}

func CopyIntoMap(a, b map[string]interface{}) map[string]interface{} {
	if b == nil {
		return a
	}
	if a == nil {
		a = make(map[string]interface{})
	}
	for k, v := range b {
		a[k] = v
	}
	return a
}

type term struct {
	// doubles as max impact (with tf=1, the max impact would be 1*idf), if there
	// is a boost for a term, simply apply it here once
	idf float64

	idPointer         uint64
	posPointer        uint64
	data              []lsmkv.MapPair
	term              string // not really needed, just to make debugging easier
	exhausted         bool
	k1                float64
	b                 float64
	averagePropLength float64
}

func (t terms) pivot(minScore float64) {
	minID, pivotPoint := t.findMinID(minScore)
	if pivotPoint == 0 {
		return
	}

	t.advanceAllAtLeast(minID)
	t.sort()
}

func (t terms) advanceAllAtLeast(minID uint64) {
	for i := range t {
		t[i].advanceAtLeast(minID)
	}
}

func (t terms) findMinID(minScore float64) (uint64, int) {
	cumScore := float64(0)

	for i, term := range t {
		cumScore += term.idf
		if cumScore >= minScore {
			return term.idPointer, i
		}
	}

	panic(fmt.Sprintf("score of %f is unreachable", minScore))
}

func (t terms) sort() {
	sort.Slice(t, func(a, b int) bool { return t[a].idPointer < t[b].idPointer })
}

func (t terms) findFirstNonExhausted() (int, bool) {
	for i := range t {
		if !t[i].exhausted {
			return i, true
		}
	}

	return -1, false
}

func (t terms) scoreNext() (uint64, float64, bool) {
	pos, ok := t.findFirstNonExhausted()
	if !ok {
		// done, nothing left to score
		return 0, 0, false
	}

	id := t[pos].idPointer
	var cumScore float64
	for i := pos; i < len(t); i++ {
		if t[i].idPointer != id || t[i].exhausted {
			continue
		}
		_, score := t[i].scoreAndAdvance()
		cumScore += score
	}

	return id, cumScore, true
}

func (t terms) debugPrint() {
	for i, term := range t {
		fmt.Printf("pos %d (%s), next_id=%d, max_impact=%f\n", i, term.term, term.idPointer, term.idf)
	}
	fmt.Printf("\n")
}

func (t *term) scoreAndAdvance() (uint64, float64) {
	id := t.idPointer
	pair := t.data[t.posPointer]
	freqBits := binary.LittleEndian.Uint32(pair.Value[0:4])
	frequency := float64(math.Float32frombits(freqBits))
	propLenBits := binary.LittleEndian.Uint32(pair.Value[4:8])
	propLength := float64(math.Float32frombits(propLenBits))
	tf := frequency / (frequency + t.k1*(1-t.b+t.b*propLength/t.averagePropLength))

	// advance
	t.posPointer++
	if t.posPointer >= uint64(len(t.data)) {
		t.exhausted = true
	} else {
		t.idPointer = binary.BigEndian.Uint64(t.data[t.posPointer].Key)
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
		t.idPointer = binary.BigEndian.Uint64(t.data[t.posPointer].Key)
	}

	return
}

type terms []term

func (b *BM25Searcher) wand(
	ctx context.Context, query string, properties []string, limit int,
) ([]*storobj.Object, []float32, error) {
	objectsBucket := b.store.Bucket(helpers.ObjectsBucketLSM)
	if objectsBucket == nil {
		return nil, nil, errors.Errorf("objects bucket not found")
	}
	N := float64(b.store.Bucket(helpers.ObjectsBucketLSM).Count())

	queryTerms, boost := helpers.SmartSplit(query)
	terms := make(terms, len(queryTerms))

	propertyWithBoost := properties[0]
	// propBoost := 1
	property := propertyWithBoost
	if strings.Contains(propertyWithBoost, "^") {
		property = strings.Split(propertyWithBoost, "^")[0]
		// boostStr := strings.Split(propertyWithBoost, "^")[1]
		// PropBoost, _ = strconv.Atoi(boostStr)
	}

	for i, queryTerm := range queryTerms {
		terms[i].term = queryTerm
		terms[i].k1 = b.config.K1
		terms[i].b = b.config.B
		propMean, err := b.propLengths.PropertyMean(property)
		terms[i].averagePropLength = float64(propMean)
		bucket := b.store.Bucket(helpers.BucketFromPropNameLSM(property))
		if bucket == nil {
			return nil, nil, fmt.Errorf("could not find bucket for property %v", property)
		}
		m, err := bucket.MapList([]byte(queryTerm))
		if err != nil {
			return nil, nil, err
		}

		terms[i].data = m

		n := float64(len(m))
		terms[i].idf = math.Log(float64(1)+(N-n+0.5)/(n+0.5)) * boost[i]

		terms[i].posPointer = 0
		terms[i].idPointer = binary.BigEndian.Uint64(terms[i].data[0].Key)

	}

	topKHeap := priorityqueue.NewMin(limit)
	worstDist := float64(0)
	for {

		terms.pivot(worstDist)

		id, score, ok := terms.scoreNext()
		if !ok {
			// nothing left to score
			break
		}

		if topKHeap.Len() < limit || topKHeap.Top().Dist < float32(score) {
			topKHeap.Insert(id, float32(score))
			for topKHeap.Len() > limit {
				topKHeap.Pop()
			}
		}
		worstDist = float64(topKHeap.Top().Dist)
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
		objects = append(objects, obj)
	}
	return objects, scores, nil
}

func (b *BM25Searcher) BM25F(ctx context.Context, className schema.ClassName, limit int,
	keywordRanking *searchparams.KeywordRanking,
	filter *filters.LocalFilter, sort []filters.Sort, additional additional.Properties,
	objectByIndexID func(index uint64) *storobj.Object,
) ([]*storobj.Object, []float32, error) {
	// WEAVIATE-471 - If a property is not searchable, return an error
	for _, property := range keywordRanking.Properties {
		if !schema.PropertyIsIndexed(b.schema.Objects, string(className), property) {
			return nil, nil, errors.New("Property " + property + " is not indexed.  Please choose another property or add an index to this property")
		}
	}

	objs, scores, err := b.wand(ctx, keywordRanking.Query, keywordRanking.Properties, limit)
	if err != nil {
		return nil, nil, errors.Wrap(err, "wand")
	}

	return objs, scores, nil
}

// BM25F merge the results from multiple properties
func (b *BM25Searcher) mergeIdss(idLists []docPointersWithScore, propNames []string) docPointersWithScore {
	// Merge all ids into the first element of the list (i.e. merge the results from different properties but same query)

	// If there is only one, we are finished
	if len(idLists) == 1 {
		return idLists[0]
	}

	docHash := make(map[uint64]docPointerWithScore)

	total := 0

	for i, list := range idLists {
		for _, doc := range list.docIDs {
			// if id is not in the map, add it
			if _, ok := docHash[doc.id]; !ok {
				if doc.Additional == nil {
					doc.Additional = make(map[string]interface{})
				}
				doc.Additional["BM25F_"+propNames[i]+"_frequency"] = fmt.Sprintf("%v", doc.frequency)
				doc.Additional["BM25F_"+propNames[i]+"_propLength"] = fmt.Sprintf("%v", doc.propLength)
				docHash[doc.id] = doc
			} else {
				// if id is in the map, add the frequency
				existing := docHash[doc.id]
				existing.frequency += doc.frequency
				existing.propLength += doc.propLength
				existing.Additional["BM25F_"+propNames[i]+"_frequency"] = fmt.Sprintf("%v", doc.frequency)
				existing.Additional["BM25F_"+propNames[i]+"_propLength"] = fmt.Sprintf("%v", doc.propLength)
				// TODO: We will have a different propLength for each property, how do we combine them?
				docHash[doc.id] = existing
			}
		}
		total += int(list.count)
	}

	// Make list from docHash
	res := docPointersWithScore{
		docIDs: make([]docPointerWithScore, len(docHash)),
		count:  uint64(total),
	}

	i := 0
	for _, v := range docHash {
		res.docIDs[i] = v
		i++
	}

	return res
}

// BM25F search each given property for a single term.  Results will be combined later
func (b *BM25Searcher) retrieveForSingleTermMultipleProps(
	ctx context.Context, c *models.Class, properties []string, searchTerm string, query string,
) (docPointersWithScore, error) {
	var idss []docPointersWithScore

	var propNames []string
	for _, propertyWithBoost := range properties {
		boost := 1
		property := propertyWithBoost
		if strings.Contains(propertyWithBoost, "^") {
			property = strings.Split(propertyWithBoost, "^")[0]
			boostStr := strings.Split(propertyWithBoost, "^")[1]
			boost, _ = strconv.Atoi(boostStr)
		}
		propNames = append(propNames, property)

		p, err := schema.GetPropertyByName(c, property)
		if err != nil {
			return docPointersWithScore{}, errors.Wrap(err, "read property from class")
		}
		indexed := p.IndexInverted

		// Properties don't have to be indexed, if they are not, they will error on search
		var ids docPointersWithScore
		if indexed == nil || *indexed {
			if p.Tokenization == "word" {
				ids, err = b.getIdsWithFrequenciesForTerm(ctx, property, searchTerm)
			} else {
				ids, err = b.getIdsWithFrequenciesForTerm(ctx, property, query)
			}
			if err != nil {
				return docPointersWithScore{}, errors.Wrap(err,
					"read doc ids and their frequencies from inverted index")
			}
		}

		for i := range ids.docIDs {
			ids.docIDs[i].frequency = ids.docIDs[i].frequency * float64(boost)
		}
		idss = append(idss, ids)
	}

	// Merge the results from different properties
	ids := b.mergeIdss(idss, propNames)
	b.scoreBM25F(ids, propNames)
	return ids, nil
}

// BM25F combine and score the results from multiple properties
func (bm *BM25Searcher) scoreBM25F(ids docPointersWithScore, propNames []string) error {
	for i := range ids.docIDs {
		ids.docIDs[i].score = float64(0)
	}

	for _, propName := range propNames {
		m, err := bm.propLengths.PropertyMean(propName)
		if err != nil {
			return err
		}

		averageDocLen := float64(m)
		k1 := bm.config.K1
		b := bm.config.B

		N := float64(bm.store.Bucket(helpers.ObjectsBucketLSM).Count())
		n := float64(len(ids.docIDs))
		idf := math.Log(float64(1) + (N-n+0.5)/(n+0.5))
		for i, id := range ids.docIDs {
			docLen := id.propLength
			tf := id.frequency / (id.frequency + k1*(1-b+b*docLen/averageDocLen))

			ids.docIDs[i].score = ids.docIDs[i].score + tf*idf
		}
	}

	return nil
}

func (bm *BM25Searcher) score(ids docPointersWithScore, propName string) error {
	m, err := bm.propLengths.PropertyMean(propName)
	if err != nil {
		return err
	}

	averageDocLen := float64(m)
	k1 := bm.config.K1
	b := bm.config.B

	N := float64(bm.store.Bucket(helpers.ObjectsBucketLSM).Count())
	n := float64(len(ids.docIDs))
	idf := math.Log(float64(1) + (N-n+0.5)/(n+0.5))
	for i, id := range ids.docIDs {
		docLen := id.propLength
		tf := id.frequency / (id.frequency + k1*(1-b+b*docLen/averageDocLen))
		ids.docIDs[i].score = tf * idf
	}

	return nil
}

func (b *BM25Searcher) getIdsWithFrequenciesForTerm(ctx context.Context,
	prop, term string,
) (docPointersWithScore, error) {
	bucketName := helpers.BucketFromPropNameLSM(prop)
	bucket := b.store.Bucket(bucketName)
	if bucket == nil {
		return docPointersWithScore{}, fmt.Errorf("bucket %v not found", bucketName)
	}

	return b.docPointersInvertedFrequency(prop, bucket, 0, &propValuePair{
		operator: filters.OperatorEqual,
		value:    []byte(term),
		prop:     prop,
	}, true)
}

func (b *BM25Searcher) docPointersInvertedFrequency(prop string, bucket *lsmkv.Bucket,
	limit int, pv *propValuePair, tolerateDuplicates bool,
) (docPointersWithScore, error) {
	rr := NewRowReaderFrequency(bucket, pv.value, pv.operator, false, b.shardVersion)

	var pointers docPointersWithScore
	var hashes [][]byte

	if err := rr.Read(context.TODO(), func(k []byte, pairs []lsmkv.MapPair) (bool, error) {
		currentDocIDs := make([]docPointerWithScore, len(pairs))
		for i, pair := range pairs {
			// TODO: gh-1833 check version before deciding which endianness to use
			currentDocIDs[i].id = binary.BigEndian.Uint64(pair.Key)
			freqBits := binary.LittleEndian.Uint32(pair.Value[0:4])
			currentDocIDs[i].frequency = float64(math.Float32frombits(freqBits))
			propLenBits := binary.LittleEndian.Uint32(pair.Value[4:8])
			currentDocIDs[i].propLength = float64(math.Float32frombits(propLenBits))
		}

		pointers.count += uint64(len(pairs))
		if len(pointers.docIDs) > 0 {
			pointers.docIDs = append(pointers.docIDs, currentDocIDs...)
		} else {
			pointers.docIDs = currentDocIDs
		}

		hashBucket := b.store.Bucket(helpers.HashBucketFromPropNameLSM(pv.prop))
		if b == nil {
			return false, errors.Errorf("no hash bucket for prop '%s' found", pv.prop)
		}

		// use retrieved k instead of pv.value - they are typically the same, but
		// not on a like operator with wildcard where we only had a partial match
		currHash, err := hashBucket.Get(k)
		if err != nil {
			return false, errors.Wrap(err, "get hash")
		}

		hashes = append(hashes, currHash)
		if limit > 0 && pointers.count >= uint64(limit) {
			return false, nil
		}

		return true, nil
	}); err != nil {
		return pointers, errors.Wrap(err, "read row")
	}

	pointers.checksum = combineChecksums(hashes, pv.operator)

	return pointers, nil
}

func (bm *BM25Searcher) rankedObjectsByDocID(found docPointersWithScore,
	additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	objs := make([]*storobj.Object, len(found.IDs()))
	scores := make([]float32, len(found.IDs()))

	bucket := bm.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return nil, nil, errors.Errorf("objects bucket not found")
	}

	i := 0

	for idx, id := range found.IDs() {
		keyBuf := bytes.NewBuffer(nil)
		binary.Write(keyBuf, binary.LittleEndian, &id)
		docIDBytes := keyBuf.Bytes()
		res, err := bucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return nil, nil, err
		}

		if res == nil {
			continue
		}

		unmarshalled, err := storobj.FromBinaryOptional(res, additional)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "unmarshal data object at position %d", i)
		}

		objs[i], scores[i] = unmarshalled, float32(found.docIDs[idx].score)
		objs[i].Object.Additional = CopyIntoMap(objs[i].Object.Additional, found.docIDs[idx].Additional)
		i++
	}

	return objs[:i], scores[:i], nil
}
