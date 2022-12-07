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

package clients

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/semi-technologies/weaviate/modules/qna-openai/ent"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestGetAnswer(t *testing.T) {
	t.Run("when the server has a successful answer ", func(t *testing.T) {
		handler := &testAnswerHandler{
			t: t,
			answer: answersResponse{
				Choices: []choice{{
					FinishReason: "test",
					Index:        0,
					Logprobs:     "",
					Text:         "John",
				}},
				Error: nil,
			},
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("apiKey", nullLogger())
		c.host = server.URL

		expected := ent.AnswerResult{
			Text:     "My name is John",
			Question: "What is my name?",
			Answer:   ptString("John"),
		}

		res, err := c.Answer(context.Background(), "My name is John", "What is my name?", nil)

		assert.Nil(t, err)
		assert.Equal(t, expected, *res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: answersResponse{
				Error: &openAIApiError{
					Message: "some error from the server",
				},
			},
		})
		defer server.Close()

		c := New("apiKey", nullLogger())
		c.host = server.URL

		_, err := c.Answer(context.Background(), "My name is John", "What is my name?", nil)

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
	assert.Equal(f.t, "/v1/completions", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.answer.Error != nil && f.answer.Error.Message != "" {
		outBytes, err := json.Marshal(f.answer)
		require.Nil(f.t, err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write(outBytes)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var b map[string]interface{}
	require.Nil(f.t, json.Unmarshal(bodyBytes, &b))

	outBytes, err := json.Marshal(f.answer)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func ptString(in string) *string {
	return &in
}
