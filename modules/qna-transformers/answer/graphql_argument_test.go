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

package answer

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
)

func TestAnswerGraphQLArgument(t *testing.T) {
	t.Run("should generate answer argument properly", func(t *testing.T) {
		// given
		prefix := "Prefix"
		classname := "Class"
		// when
		answer := answerArgument(prefix, classname)

		// then
		// the built graphQL field needs to support this structure:
		// {
		//   question: "question",
		//   certainty: 0.9
		// }
		assert.NotNil(t, answer)
		assert.Equal(t, "QnATransformersPrefixClassAnswerInpObj", answer.Type.Name())
		answerFields, ok := answer.Type.(*graphql.InputObject)
		assert.True(t, ok)
		assert.NotNil(t, answerFields)
		assert.Equal(t, 2, len(answerFields.Fields()))
		fields := answerFields.Fields()
		question := fields["question"]
		questionNonNull, questionNonNullOK := question.Type.(*graphql.NonNull)
		assert.True(t, questionNonNullOK)
		assert.Equal(t, "String", questionNonNull.OfType.Name())
		assert.NotNil(t, question)
		assert.NotNil(t, fields["certainty"])
	})
}
