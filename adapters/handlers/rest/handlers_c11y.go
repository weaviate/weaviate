/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package rest

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	middleware "github.com/go-openapi/runtime/middleware"
	core "github.com/semi-technologies/contextionary/contextionary/core"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/telemetry"

	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/contextionary_api"
)

// TODO: Move to own UC - way too much logic in a handler here !!

type c11y interface {
	VectorForWord(ctx context.Context, word string) ([]float32, error)
	NearestWordsByVector(ctx context.Context, vector []float32, n int, k int) ([]string, []float32, error)
	IsWordPresent(ctx context.Context, word string) (bool, error)
}

func setupC11yHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog, c11y c11y) {
	/*
	 * HANDLE C11Y
	 */

	api.ContextionaryAPIWeaviateC11yWordsHandler = contextionary_api.WeaviateC11yWordsHandlerFunc(func(params contextionary_api.WeaviateC11yWordsParams, principal *models.Principal) middleware.Responder {
		ctx := params.HTTPRequest.Context()

		// the word(s) from the request
		words := params.Words

		// the returnObject
		returnObject := &models.C11yWordsResponse{}

		// set first character to lowercase
		firstChar := []rune(words)
		firstChar[0] = unicode.ToLower(firstChar[0])
		words = string(firstChar)

		// check if there are only letters present
		match, _ := regexp.MatchString("^[a-zA-Z]*$", words)
		if match == false {
			return contextionary_api.NewWeaviateC11yWordsBadRequest().WithPayload(createErrorResponseObject("Can't parse the word(s). They should only contain a-zA-Z"))
		}

		// split words to validate if they are in the contextionary
		wordArray := split(words)

		//if there are more words presented, add a concat response
		if len(wordArray) > 1 {
			// declare the return object
			returnObject.ConcatenatedWord = &models.C11yWordsResponseConcatenatedWord{}

			// set concat word
			returnObject.ConcatenatedWord.ConcatenatedWord = words

			// set individual words
			returnObject.ConcatenatedWord.SingleWords = wordArray

			// loop over the words and collect vectors to calculate centroid
			collectVectors := []core.Vector{}
			collectWeights := []float32{}
			for _, word := range wordArray {
				infoVector, err := c11y.VectorForWord(ctx, word)
				if err != nil {
					return contextionary_api.NewWeaviateC11yWordsBadRequest().WithPayload(createErrorResponseObject("Can't create the vector representation for the word"))
				}
				// collect the word vector based on idx
				collectVectors = append(collectVectors, core.NewVector(infoVector))
				collectWeights = append(collectWeights, 1.0)
			}

			// compute the centroid
			weightedCentroid, err := core.ComputeWeightedCentroid(collectVectors, collectWeights)
			if err != nil {
				return contextionary_api.NewWeaviateC11yWordsBadRequest().WithPayload(createErrorResponseObject("Can't compute weighted centroid"))
			}
			returnObject.ConcatenatedWord.ConcatenatedVector = weightedCentroid.ToArray()

			// relate words of centroid
			cnnWords, cnnDists, err := c11y.NearestWordsByVector(ctx, weightedCentroid.ToArray(), 12, 32)
			if err != nil {
				return contextionary_api.NewWeaviateC11yWordsBadRequest().WithPayload(createErrorResponseObject("Can't compute nearest neighbors of ComputeWeightedCentroid"))
			}
			returnObject.ConcatenatedWord.ConcatenatedNearestNeighbors = []*models.C11yNearestNeighborsItems0{}

			// loop over NN Idx' and append to the return object
			for i, word := range cnnWords {
				nearestNeighborsItem := models.C11yNearestNeighborsItems0{}
				nearestNeighborsItem.Word = word
				nearestNeighborsItem.Distance = cnnDists[i]
				returnObject.ConcatenatedWord.ConcatenatedNearestNeighbors = append(returnObject.ConcatenatedWord.ConcatenatedNearestNeighbors, &nearestNeighborsItem)
			}
		}

		// loop over the words and populate the return response for single words
		for _, word := range wordArray {

			// declare the return object
			singleReturnObject := &models.C11yWordsResponseIndividualWordsItems0{}

			// set the current word and retrieve the index in the contextionary
			singleReturnObject.Word = word
			present, err := c11y.IsWordPresent(ctx, word)
			if err != nil {
				return contextionary_api.NewWeaviateC11yWordsBadRequest().WithPayload(errPayloadFromSingleErr(fmt.Errorf(
					"could not check word presence for word %s: %v", word, err)))
			}

			if !present {
				// word not found
				singleReturnObject.InC11y = false

				// append to returnObject.SingleWord
				returnObject.IndividualWords = append(returnObject.IndividualWords, singleReturnObject)
			} else {
				// word is found
				singleReturnObject.InC11y = true

				// define the Info struct
				singleReturnObject.Info = &models.C11yWordsResponseIndividualWordsItems0Info{}

				// collect & set the vector
				infoVector, err := c11y.VectorForWord(ctx, word)
				if err != nil {
					return contextionary_api.NewWeaviateC11yWordsBadRequest().WithPayload(createErrorResponseObject("Can't create the vector representation for the word"))
				}
				singleReturnObject.Info.Vector = infoVector

				// collect & set the 28 nearestNeighbors
				nnWords, nnDists, err := c11y.NearestWordsByVector(ctx, infoVector, 12, 32)
				if err != nil {
					return contextionary_api.NewWeaviateC11yWordsBadRequest().WithPayload(createErrorResponseObject("Can't collect nearestNeighbors for this vector"))
				}
				singleReturnObject.Info.NearestNeighbors = []*models.C11yNearestNeighborsItems0{}

				// loop over NN Idx' and append to the info object
				for i, word := range nnWords {
					nearestNeighborsItem := models.C11yNearestNeighborsItems0{}
					nearestNeighborsItem.Word = word
					nearestNeighborsItem.Distance = nnDists[i]
					singleReturnObject.Info.NearestNeighbors = append(singleReturnObject.Info.NearestNeighbors, &nearestNeighborsItem)
				}

				// append to returnObject.SingleWord
				returnObject.IndividualWords = append(returnObject.IndividualWords, singleReturnObject)
			}

		}

		// Register the request
		go func() {
			requestsLog.Register(telemetry.TypeREST, telemetry.LocalTools)
		}()

		return contextionary_api.NewWeaviateC11yWordsOK().WithPayload(returnObject)
	})

	api.ContextionaryAPIWeaviateC11yCorpusGetHandler = contextionary_api.WeaviateC11yCorpusGetHandlerFunc(func(params contextionary_api.WeaviateC11yCorpusGetParams, principal *models.Principal) middleware.Responder {
		return middleware.NotImplemented("operation contextionary_api.WeaviateC11yCorpusGet has not yet been implemented")
	})

}

// Splits a CamelCase string to an array
// Based on: https://github.com/fatih/camelcase
func split(src string) (entries []string) {
	// don't split invalid utf8
	if !utf8.ValidString(src) {
		return []string{src}
	}
	entries = []string{}
	var runes [][]rune
	lastClass := 0
	class := 0
	// split into fields based on class of unicode character
	for _, r := range src {
		switch true {
		case unicode.IsLower(r):
			class = 1
		case unicode.IsUpper(r):
			class = 2
		case unicode.IsDigit(r):
			class = 3
		default:
			class = 4
		}
		if class == lastClass {
			runes[len(runes)-1] = append(runes[len(runes)-1], r)
		} else {
			runes = append(runes, []rune{r})
		}
		lastClass = class
	}
	// handle upper case -> lower case sequences, e.g.
	// "PDFL", "oader" -> "PDF", "Loader"
	for i := 0; i < len(runes)-1; i++ {
		if unicode.IsUpper(runes[i][0]) && unicode.IsLower(runes[i+1][0]) {
			runes[i+1] = append([]rune{runes[i][len(runes[i])-1]}, runes[i+1]...)
			runes[i] = runes[i][:len(runes[i])-1]
		}
	}
	// construct []string from results
	for _, s := range runes {
		if len(s) > 0 {
			entries = append(entries, strings.ToLower(string(s)))
		}
	}
	return
}
