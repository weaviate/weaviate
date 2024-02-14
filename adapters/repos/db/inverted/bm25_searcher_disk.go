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

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (b *BM25Searcher) wandDisk(
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
	tokenizationsOrdered := []string{
		models.PropertyTokenizationWord,
		models.PropertyTokenizationLowercase,
		models.PropertyTokenizationWhitespace,
		models.PropertyTokenizationField,
	}

	queryTermsByTokenization := map[string][]string{}
	duplicateBoostsByTokenization := map[string][]int{}
	propNamesByTokenization := map[string][]string{}
	propertyBoosts := make(map[string]float32, len(params.Properties))

	for _, tokenization := range tokenizationsOrdered {
		queryTermsByTokenization[tokenization], duplicateBoostsByTokenization[tokenization] = helpers.TokenizeAndCountDuplicates(tokenization, params.Query)

		// stopword filtering for word tokenization
		if tokenization == models.PropertyTokenizationWord {
			queryTermsByTokenization[tokenization], duplicateBoostsByTokenization[tokenization] = b.removeStopwordsFromQueryTerms(queryTermsByTokenization[tokenization], duplicateBoostsByTokenization[tokenization], stopWordDetector)
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

	// set of buckets to be searched
	buckets := make(map[*lsmkv.Bucket]struct{})
	for _, propNameTokens := range propNamesByTokenization {
		for _, propName := range propNameTokens {
			bucket := b.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
			if bucket == nil {
				return nil, nil, fmt.Errorf("could not find bucket for property %v", propName)
			}
			if _, ok := buckets[bucket]; !ok {
				buckets[bucket] = struct{}{}
			}
		}
	}

	objectsBucket := b.store.Bucket(helpers.ObjectsBucketLSM)
	// iterate them in creation order, keep track of all ids
	// pass the ids to the next bucket to check for tombstones
	topKHeaps := make([]*priorityqueue.Queue[any], len(buckets))
	allTerms := make([]lsmkv.Terms, len(buckets))
	allIds := make(map[uint64]struct{})
	allObjects := make([]*storobj.Object, 0, limit)
	allScores := make([]float32, 0, limit)
	for bucket := range buckets {
		for _, propNameTokens := range propNamesByTokenization {
			for _, propName := range propNameTokens {
				terms := make(lsmkv.Terms, 0, len(queryTermsByTokenization))
				for i, term := range queryTermsByTokenization[models.PropertyTokenizationWord] {
					duplicateTextBoost := duplicateBoostsByTokenization[models.PropertyTokenizationWord][i]

					singleTerms, err := bucket.GetTermsForWand([]byte(term), N, float64(duplicateTextBoost), float64(propertyBoosts[propName]))
					if err == nil {
						terms = append(terms, singleTerms...)
					}
				}
				topKHeap, ids := lsmkv.GetTopKHeap(limit, terms, averagePropLength, b.config, allIds)
				for k, v := range ids {
					allIds[k] = v
				}
				for _, term := range terms {
					term.ClearData()
				}

				topKHeaps = append(topKHeaps, topKHeap)

				allTerms = append(allTerms, terms)
				indices := make([]map[uint64]int, len(allTerms))
				objects, scores, err := objectsBucket.GetTopKObjects(topKHeap, terms, indices, params.AdditionalExplanations)

				allObjects = append(allObjects, objects...)
				allScores = append(allScores, scores...)
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}
	return allObjects, allScores, nil
}
