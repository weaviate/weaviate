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
}

func NewBM25Searcher(config schema.BM25Config, store *lsmkv.Store, schema schema.Schema,
	rowCache cacher, propIndices propertyspecific.Indices,
	classSearcher ClassSearcher, deletedDocIDs DeletedDocIDChecker,
	propLengths propLengthRetriever, logger logrus.FieldLogger,
	shardVersion uint16) *BM25Searcher {
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
	filter *filters.LocalFilter, additional additional.Properties,
	className schema.ClassName) ([]*storobj.Object, []float32, error) {
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

	// TODO: we can probably do this way smarter in a way that we immediately
	// skip anything worse the the current worst candidate
	sort.Slice(ids.docIDs, func(a, b int) bool {
		return ids.docIDs[a].score > ids.docIDs[b].score
	})

	if len(ids.docIDs) > limit {
		ids.docIDs = ids.docIDs[:limit]
	}

	objs, scores, err := b.rankedObjectsByDocID(ids, additional)
	if err != nil {
		return nil, nil, errors.Wrap(err, "resolve doc ids to objects")
	}

	return objs, scores, nil
}

func (b *BM25Searcher) retrieveScoreAndSortForSingleTerm(ctx context.Context,
	property, term string) (docPointersWithScore, error) {
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
	prop, term string) (docPointersWithScore, error) {
	bucketName := helpers.BucketFromPropNameLSM(prop)
	bucket := b.store.Bucket(bucketName)

	return b.docPointersInvertedFrequency(prop, bucket, 0, &propValuePair{
		operator: filters.OperatorEqual,
		value:    []byte(term),
		prop:     prop,
	}, true)
}

func (b *BM25Searcher) docPointersInvertedFrequency(prop string, bucket *lsmkv.Bucket,
	limit int, pv *propValuePair, tolerateDuplicates bool) (docPointersWithScore, error) {
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
	additional additional.Properties) ([]*storobj.Object, []float32, error) {
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
		i++
	}

	return objs[:i], scores[:i], nil
}

// RankedResults implements sort.Interface, allowing
// results aggregated from multiple shards to be
// sorted according to their BM25 ranking
type RankedResults struct {
	Objects []*storobj.Object
	Scores  []float32
}

func (r *RankedResults) Swap(i, j int) {
	r.Objects[i], r.Objects[j] = r.Objects[j], r.Objects[i]
	r.Scores[i], r.Scores[j] = r.Scores[j], r.Scores[i]
}

func (r *RankedResults) Less(i, j int) bool {
	return r.Scores[i] > r.Scores[j]
}

func (r *RankedResults) Len() int {
	return len(r.Scores)
}
