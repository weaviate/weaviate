/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package restapi

import (
	"regexp"
	"unicode"

	"github.com/creativesoftwarefdn/weaviate/models"

	"github.com/creativesoftwarefdn/weaviate/restapi/operations"
	"github.com/creativesoftwarefdn/weaviate/restapi/operations/contextionary_api"
	middleware "github.com/go-openapi/runtime/middleware"
)

func setupC11yHandlers(api *operations.WeaviateAPI) {
	/*
	 * HANDLE C11Y
	 */

	api.ContextionaryAPIWeaviateC11yContextHandler = contextionary_api.WeaviateC11yContextHandlerFunc(func(params contextionary_api.WeaviateC11yContextParams) middleware.Responder {

		// the word(s) from the request
		words := params.Words

		// the returnObject
		returnObject := &models.C11yContextResponse{}

		// set first character to lowercase
		firstChar := []rune(words)
		firstChar[0] = unicode.ToLower(firstChar[0])
		words = string(firstChar)

		// check if there are only letters present
		match, _ := regexp.MatchString("^[a-zA-Z]*$", words)
		if match == false {
			return contextionary_api.NewWeaviateC11yContextBadRequest().WithPayload(createErrorResponseObject("Can't parse the word(s). They should only contain a-zA-Z"))
		}

		// split words to validate if they are in the contextionary
		wordArray := split(words)

		// loop over the words and populate the return response
		for _, word := range wordArray {
			singleReturnObject := &models.C11yContextResponseResultsItems0{}

			// set the current word and retrieve the index in the contextionary
			singleReturnObject.Word = word
			wordIdx := contextionary.WordToItemIndex(word)

			if wordIdx == -1 {
				// word not found
				singleReturnObject.InC11y = false

				// append to returnObject.Results
				returnObject.Results = append(returnObject.Results, singleReturnObject)
			} else {
				// word is found
				singleReturnObject.InC11y = true

				////
				// TODO: SET VECTOR
				//
				// END RESULT SHOULD BE:
				// singleReturnObject.Info.Vector = contextionary.GetVectorForItemIndex(wordIdx)
				//
				////

				// append to returnObject.Results
				returnObject.Results = append(returnObject.Results, singleReturnObject)
			}

		}

		return contextionary_api.NewWeaviateC11yContextOK().WithPayload(returnObject)
	})

}
