package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/semi-technologies/weaviate/modules/qna-transformers/ent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAnswer(t *testing.T) {
	t.Run("when the server has a successful answer", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: &answersResponse{
				answersInput: answersInput{
					Text:     "My name is John",
					Question: "What is my name?",
				},
				Answer:    ptString("John"),
				Certainty: ptFloat(0.7),
			},
		})
		defer server.Close()
		c := New(server.URL, nullLogger())
		res, err := c.Answer(context.Background(), "My name is John",
			"What is my name?")

		assert.Nil(t, err)
		assert.Equal(t, &ent.AnswerResult{
			Text:      "My name is John",
			Question:  "What is my name?",
			Answer:    ptString("John"),
			Certainty: ptFloat(0.7),
		}, res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			err: &errResponse{
				Error: "some error from the server",
			},
		})
		defer server.Close()
		c := New(server.URL, nullLogger())
		_, err := c.Answer(context.Background(), "My name is John",
			"What is my name?")

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "some error from the server")
	})
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer *answersResponse
	err    *errResponse
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/answers/", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.err != nil {
		w.WriteHeader(500)
		jsonBytes, _ := json.Marshal(f.err)
		w.Write(jsonBytes)
	} else {
		jsonBytes, _ := json.Marshal(f.answer)
		w.Write(jsonBytes)
	}
}

func ptFloat(in float64) *float64 {
	return &in
}

func ptString(in string) *string {
	return &in
}
