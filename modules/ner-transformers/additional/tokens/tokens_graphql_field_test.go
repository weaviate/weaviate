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

package tokens

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func Test_additionalTokensField(t *testing.T) {
	// given
	tokenProvider := &TokenProvider{}
	classname := "Class"

	// when
	tokens := tokenProvider.additionalTokensField(classname)

	// then
	// the built graphQL field needs to support this structure:
	// Args: {
	// 	    "properties": ["summary"],
	// 	    "limit": 1,
	//     	"distance": 0.7
	// }
	// Type: {
	//   tokens: {
	//     "property": "summary",
	//     "entity": "I-PER",
	//     "distance": 0.8,
	//     "word": "original word",
	//     "startPosition": 1,
	//     "endPosition": 2,
	//   }
	// }

	assert.NotNil(t, tokens)
	assert.Equal(t, "ClassAdditionalTokens", tokens.Type.Name())
	assert.NotNil(t, tokens.Type)
	tokensObjectList, tokensObjectListOK := tokens.Type.(*graphql.List)
	assert.True(t, tokensObjectListOK)
	tokensObject, tokensObjectOK := tokensObjectList.OfType.(*graphql.Object)
	assert.True(t, tokensObjectOK)
	assert.Equal(t, 7, len(tokensObject.Fields()))
	assert.NotNil(t, tokensObject.Fields()["property"])
	assert.NotNil(t, tokensObject.Fields()["entity"])
	assert.NotNil(t, tokensObject.Fields()["certainty"])
	assert.NotNil(t, tokensObject.Fields()["distance"])
	assert.NotNil(t, tokensObject.Fields()["word"])
	assert.NotNil(t, tokensObject.Fields()["startPosition"])
	assert.NotNil(t, tokensObject.Fields()["endPosition"])

	assert.NotNil(t, tokens.Args)
	assert.Equal(t, 4, len(tokens.Args))
	assert.NotNil(t, tokens.Args["certainty"])
	assert.NotNil(t, tokens.Args["distance"])
	assert.NotNil(t, tokens.Args["limit"])
	assert.NotNil(t, tokens.Args["properties"])
}
