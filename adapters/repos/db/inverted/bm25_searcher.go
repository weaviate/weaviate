//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/adapters/repos/db/propertyspecific"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/sirupsen/logrus"
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
	PropertyCount(prop string) (float32, float32, error)
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

// Object returns a list of full objects
func (b *BM25Searcher) Object(ctx context.Context, limit int,
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

	// TODO: more complex pre-processing with proper split function
	terms := strings.Split(keywordRanking.Query, " ")

	idLists := make([]docPointersWithScore, len(terms))

	for i, term := range terms {
		ids, err := b.retrieveScoreAndSortForSingleTerm(ctx,
			keywordRanking.Properties[0], term)
		if err != nil {
			return nil, nil, err
		}

		idLists[i] = ids
	}

	before := time.Now()
	ids := newScoreMerger(idLists).do()
	took := time.Since(before)
	b.logger.WithField("took", took).
		WithField("event", "merge_scores_of_terms").
		Debugf("merge score of all terms took %s", took)

	ids = b.sort(ids)

	if len(ids.docIDs) > limit {
		ids.docIDs = ids.docIDs[:limit]
	}

	objs, scores, err := b.rankedObjectsByDocID(ids, additional)
	if err != nil {
		return nil, nil, errors.Wrap(err, "resolve doc ids to objects")
	}

	return objs, scores, nil
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

// Merge BM25F scores of all terms
func mergeScores(termresults []docPointersWithScore) docPointersWithScore {
	//Create a hash, iterate over termresults, add the score to the hash, turn the hash into a list
	resultsHash := make(map[uint64]docPointerWithScore)
	for _, id := range termresults {
		for _, doc := range id.docIDs {
			if _, ok := resultsHash[doc.id]; !ok {
				resultsHash[doc.id] = doc
			} else {
				d := resultsHash[doc.id]
				d.score += doc.score
				resultsHash[doc.id] = d
				CopyIntoMap(d.Additional, doc.Additional)
			}
		}
	}

	results := docPointersWithScore{}
	for _, doc := range resultsHash {
		results.docIDs = append(results.docIDs, doc)
	}
	results.count = uint64(len(results.docIDs))

	return results
}

func (b *BM25Searcher) BM25F(ctx context.Context, limit int,
	keywordRanking *searchparams.KeywordRanking,
	filter *filters.LocalFilter, sort []filters.Sort, additional additional.Properties,
	className schema.ClassName,
) ([]*storobj.Object, []float32, error) {
	terms := strings.Split(keywordRanking.Query, " ")
	idLists := make([]docPointersWithScore, len(terms))

	for i, term := range terms {
		lower_term := strings.ToLower(term)
		ids, err := b.retrieveForSingleTermMultipleProps(ctx, keywordRanking.Properties, lower_term)

		if err != nil {
			return nil, nil, err
		}

		idLists[i] = ids
	}

	ids := mergeScores(idLists)
	ids = b.sort(ids)
	objs, scores, err := b.rankedObjectsByDocID(ids, additional)
	if err != nil {
		return nil, nil, errors.Wrap(err, "resolve doc ids to objects")
	}

	return objs, scores, nil

}

// BM25F merge the results from multiple properties
func (b *BM25Searcher) mergeIdss(idLists []docPointersWithScore, propNames []string) docPointersWithScore {
	//Merge all ids into the first element of the list (i.e. merge the results from different properties but same query)

	//If there is only one, we are finished
	if len(idLists) == 1 {
		return idLists[0]
	}

	docHash := make(map[uint64]docPointerWithScore)

	total := 0

	for i, list := range idLists {
		for _, doc := range list.docIDs {
			//if id is not in the map, add it
			if _, ok := docHash[doc.id]; !ok {
				if doc.Additional == nil {
					doc.Additional = make(map[string]interface{})
				}
				doc.Additional["BM25F_"+propNames[i]+"_frequency"] = fmt.Sprintf("%v", doc.frequency)
				doc.Additional["BM25F_"+propNames[i]+"_propLength"] = fmt.Sprintf("%v", doc.propLength)
				docHash[doc.id] = doc
			} else {
				//if id is in the map, add the frequency
				existing := docHash[doc.id]
				existing.frequency += doc.frequency
				existing.propLength += doc.propLength
				existing.Additional["BM25F_"+propNames[i]+"_frequency"] = fmt.Sprintf("%v", doc.frequency)
				existing.Additional["BM25F_"+propNames[i]+"_propLength"] = fmt.Sprintf("%v", doc.propLength)
				//TODO: We will have a different propLength for each property, how do we combine them?
				docHash[doc.id] = existing
			}
		}
		total += int(list.count)
	}

	//Make list from docHash
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
func (b *BM25Searcher) retrieveForSingleTermMultipleProps(ctx context.Context,
	properties []string, term string,
) (docPointersWithScore, error) {
	idss := []docPointersWithScore{}

	searchTerm := term
	boost := 1
	if strings.Contains(term, "^") {
		searchTerm = strings.Split(term, "^")[0]
		boostStr := strings.Split(term, "^")[1]
		boost, _ = strconv.Atoi(boostStr)

	}

	propNames := []string{}

	for _, property := range properties {
		ids, err := b.getIdsWithFrequenciesForTerm(ctx, property, searchTerm)
		if err != nil {
			return docPointersWithScore{}, errors.Wrap(err,
				"read doc ids and their frequencies from inverted index")
		}
		idss = append(idss, ids)
		propNames = append(propNames, property)
	}

	ids := b.mergeIdss(idss, propNames)
	//Boost the frequencies
	for i := range ids.docIDs {
		ids.docIDs[i].frequency = ids.docIDs[i].frequency * float64(boost)
	}
	b.scoreBM25F(ids, properties)
	return ids, nil
}

// BM25F combine and score the results from multiple properties
func (bm *BM25Searcher) scoreBM25F(ids docPointersWithScore, propName []string) error {
	totalPropSum := 0.0
	total := 0.0
	for _, prop := range propName {
		propSum, propTotal, err := bm.propLengths.PropertyCount(prop)
		if err != nil {
			return errors.Wrap(err, "get property length")
		}
		totalPropSum += float64(propSum)
		total += float64(propTotal)
	}

	m := totalPropSum / float64(total)

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
