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

package answer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
)

func TestAnswerField(t *testing.T) {
	t.Run("should generate answer argument properly", func(t *testing.T) {
		// given
		answerProvider := &AnswerProvider{}
		classname := "Class"

		// when
		answer := answerProvider.additionalAnswerField(classname)

		// then
		// the built graphQL field needs to support this structure:
		// Type: {
		//   answer: {
		//     result: "answer",
		//     startPosition: 1
		//     endPosition: 2
		//     distance: 0.2
		//     property: "propName"
		//     hasAnswer: true
		//   }
		// }
		assert.NotNil(t, answer)
		assert.Equal(t, "ClassAdditionalAnswer", answer.Type.Name())
		assert.NotNil(t, answer.Type)
		answerObject, answerObjectOK := answer.Type.(*graphql.Object)
		assert.True(t, answerObjectOK)
		assert.Equal(t, 5, len(answerObject.Fields()))
		assert.NotNil(t, answerObject.Fields()["result"])
		assert.NotNil(t, answerObject.Fields()["startPosition"])
		assert.NotNil(t, answerObject.Fields()["endPosition"])
		assert.NotNil(t, answerObject.Fields()["property"])
		assert.NotNil(t, answerObject.Fields()["hasAnswer"])
	})
}
