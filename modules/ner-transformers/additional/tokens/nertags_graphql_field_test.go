
//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package tokens

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
)

func TestTokensField(t *testing.T) {
	t.Run("should generate NER Result argument properly", func(t *testing.T) {
		// given
		tokensProvider := &TokensProvider{}
		classname := "Class"

		// when
		tokens := tokensProvider.additionalTokensField(classname)

		// then
		// the built graphQL field needs to support this structure:
		// Type: {
		//   tokens: {
		//     property: "property",
		//     entity: "recognized entity",
		//     certainty: 0.2
		//     word: "word"
		//     startPosition: 1
		//     endPosition: 2
		//   }
		// }
		assert.NotNil(t, answer)
		assert.Equal(t, "ClassAdditionalTokens", Tokens.Type.Name())
		assert.NotNil(t, Tokens.Type)
		TokensObject, TokensObjectOK := Tokens.Type.(*graphql.Object)
		assert.True(t, NerRResultObjectOK)
		assert.Equal(t, 6, len(TokensObject.Fields()))
		assert.NotNil(t, TokensObject.Fields()["property"])
		assert.NotNil(t, TokensObject.Fields()["entity"])
		assert.NotNil(t, TokensObject.Fields()["certainty"])
		assert.NotNil(t, TokensObject.Fields()["word"])
		assert.NotNil(t, TokensObject.Fields()["startPosition"])
		assert.NotNil(t, TokensObject.Fields()["endPosition"])
	})
}