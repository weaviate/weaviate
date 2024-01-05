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

package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/modules/qna-transformers/ent"
)

func TestGetAnswer(t *testing.T) {
	t.Run("when the server has a successful answer (with distance)", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: answersResponse{
				answersInput: answersInput{
					Text:     "My name is John",
					Question: "What is my name?",
				},
				Answer:    ptString("John"),
				Certainty: ptFloat(0.7),
				Distance:  ptFloat(0.3),
			},
		})
		defer server.Close()
		c := New(server.URL, 0, nullLogger())
		res, err := c.Answer(context.Background(), "My name is John",
			"What is my name?")
		assert.Nil(t, err)

		expectedResult := ent.AnswerResult{
			Text:      "My name is John",
			Question:  "What is my name?",
			Answer:    ptString("John"),
			Certainty: ptFloat(0.7),
			Distance:  ptFloat(0.6),
		}

		assert.Equal(t, expectedResult.Text, res.Text)
		assert.Equal(t, expectedResult.Question, res.Question)
		assert.Equal(t, expectedResult.Answer, res.Answer)
		assert.Equal(t, expectedResult.Certainty, res.Certainty)
		assert.InDelta(t, *expectedResult.Distance, *res.Distance, 1e-9)
	})

	t.Run("when the server has a successful answer (with certainty)", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: answersResponse{
				answersInput: answersInput{
					Text:     "My name is John",
					Question: "What is my name?",
				},
				Answer:    ptString("John"),
				Certainty: ptFloat(0.7),
			},
		})
		defer server.Close()
		c := New(server.URL, 0, nullLogger())
		res, err := c.Answer(context.Background(), "My name is John",
			"What is my name?")

		assert.Nil(t, err)
		assert.Equal(t, &ent.AnswerResult{
			Text:      "My name is John",
			Question:  "What is my name?",
			Answer:    ptString("John"),
			Certainty: ptFloat(0.7),
			Distance:  additional.CertaintyToDistPtr(ptFloat(0.7)),
		}, res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: answersResponse{
				Error: "some error from the server",
			},
		})
		defer server.Close()
		c := New(server.URL, 0, nullLogger())
		_, err := c.Answer(context.Background(), "My name is John",
			"What is my name?")

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "some error from the server")
	})
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer answersResponse
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/answers/", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.answer.Error != "" {
		w.WriteHeader(500)
	}
	jsonBytes, _ := json.Marshal(f.answer)
	w.Write(jsonBytes)
}

func ptFloat(in float64) *float64 {
	return &in
}

func ptString(in string) *string {
	return &in
}
