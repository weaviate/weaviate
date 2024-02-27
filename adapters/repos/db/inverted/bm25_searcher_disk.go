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
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

// global var to store wand times
var (
	wandDiskTimes     []float64 = make([]float64, 5)
	wandDiskCounter   int       = 0
	wandDiskLastClass string    = ""
	wandDiskStats     []float64 = make([]float64, 1)
)

func (b *BM25Searcher) wandDisk(
	ctx context.Context, filterDocIds helpers.AllowList, class *models.Class, params searchparams.KeywordRanking, limit int,
) ([]*storobj.Object, []float32, error) {
	if wandDiskLastClass == "" {
		wandDiskLastClass = string(class.Class)
	}
	if wandDiskLastClass != string(class.Class) {
		fmt.Printf("DISK,%v", wandDiskLastClass)
		for i := 0; i < len(wandDiskTimes); i++ {
			fmt.Printf(",%8.2f", wandDiskTimes[i]/float64(wandDiskCounter))
		}
		for i := 0; i < len(wandDiskStats); i++ {
			fmt.Printf(",%8.2f", wandDiskStats[i]/float64(wandDiskCounter))
		}
		fmt.Printf("\n")
		wandDiskTimes = make([]float64, len(wandDiskTimes))
		wandDiskStats = make([]float64, len(wandDiskStats))
		wandDiskCounter = 0
		wandDiskLastClass = string(class.Class)
	}

	wandDiskCounter++
	wandTimesId := 0

	// start timer
	startTime := float64(time.Now().UnixNano()) / 1e6

	N := float64(b.store.Bucket(helpers.ObjectsBucketLSM).Count())

	var stopWordDetector *stopwords.Detector
	if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.Stopwords != nil {
		var err error
		stopWordDetector, err = stopwords.NewDetectorFromConfig(*(class.InvertedIndexConfig.Stopwords))
		if err != nil {
			return nil, nil, err
		}
	}

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

	wandDiskTimes[wandTimesId] += float64(time.Now().UnixNano())/1e6 - startTime
	wandTimesId++
	startTime = float64(time.Now().UnixNano()) / 1e6

	averagePropLength = averagePropLength / float64(len(params.Properties))

	// set of buckets to be searched

	buckets := make(map[*lsmkv.Bucket]string)
	for _, propNameTokens := range propNamesByTokenization {
		for _, propName := range propNameTokens {
			bucket := b.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
			if bucket == nil {
				return nil, nil, fmt.Errorf("could not find bucket for property %v", propName)
			}
			if _, ok := buckets[bucket]; !ok {
				buckets[bucket] = propName
			}
		}
	}

	wandDiskTimes[wandTimesId] += float64(time.Now().UnixNano())/1e6 - startTime
	wandTimesId++
	startTime = float64(time.Now().UnixNano()) / 1e6

	objectsBucket := b.store.Bucket(helpers.ObjectsBucketLSM)
	// iterate them in creation order, keep track of all ids
	// pass the ids to the next bucket to check for tombstones
	topKHeaps := make([]*priorityqueue.Queue[any], len(buckets))
	allTerms := make([]lsmkv.Terms, len(buckets))
	allIds := make(map[uint64]struct{})
	allObjects := make([]*storobj.Object, 0, limit)
	allScores := make([]float32, 0, limit)
	for bucket, propName := range buckets {
		terms := make([]lsmkv.Terms, len(queryTermsByTokenization[models.PropertyTokenizationWord]))
		for i, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
			// pass i to the closure
			i := i
			term := term
			duplicateTextBoost := duplicateBoostsByTokenization[models.PropertyTokenizationWord][i]

			singleTerms, err := bucket.GetTermsForWand([]byte(term), N, float64(duplicateTextBoost), float64(propertyBoosts[propName]))
			if err != nil {
				return nil, nil, err
			}
			terms[i] = singleTerms
		}

		flatTerms := make(lsmkv.Terms, 0, len(terms)*len(terms[0]))
		for _, term := range terms {
			flatTerms = append(flatTerms, term...)
		}

		if len(flatTerms) == 0 {
			continue
		}

		wandDiskStats[0] += float64(len(queryTermsByTokenization[models.PropertyTokenizationWord]))

		wandDiskTimes[wandTimesId] += float64(time.Now().UnixNano())/1e6 - startTime
		wandTimesId++
		startTime = float64(time.Now().UnixNano()) / 1e6

		topKHeap, ids := lsmkv.GetTopKHeap(limit, flatTerms, averagePropLength, b.config, allIds)
		for k, v := range ids {
			allIds[k] = v
		}
		for _, term := range flatTerms {
			term.ClearData()
		}

		topKHeaps = append(topKHeaps, topKHeap)

		wandDiskTimes[wandTimesId] += float64(time.Now().UnixNano())/1e6 - startTime
		wandTimesId++
		startTime = float64(time.Now().UnixNano()) / 1e6

		allTerms = append(allTerms, flatTerms)
		indices := make([]map[uint64]int, len(allTerms))
		objects, scores, err := objectsBucket.GetTopKObjects(topKHeap, flatTerms, indices, params.AdditionalExplanations)

		wandDiskTimes[wandTimesId] += float64(time.Now().UnixNano())/1e6 - startTime
		wandTimesId++
		startTime = float64(time.Now().UnixNano()) / 1e6
		allObjects = append(allObjects, objects...)
		allScores = append(allScores, scores...)
		if err != nil {
			return nil, nil, err
		}
	}

	return allObjects, allScores, nil
}
