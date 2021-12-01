package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/semi-technologies/weaviate/modules/ner-transformers/ent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAnswer(t *testing.T) {
	t.Run("when the server has a successful answer", func(t *testing.T) {
		server := httptest.NewServer(&testNERHandler{
			t: t,
			res: nerResponse{
				nerInput: nerInput{
					Text: "I work at Apple",
				},
				Tokens: []tokenResponse{
					{
						Entity:        "I-ORG",
						Certainty:     0.7,
						Word:          "Apple",
						StartPosition: 20,
						EndPosition:   25,
					},
				},
			},
		})
		defer server.Close()
		c := New(server.URL, nullLogger())
		res, err := c.GetTokens(context.Background(), "prop",
			"I work at Apple")

		assert.Nil(t, err)
		assert.Equal(t, []ent.TokenResult{
			{
				Entity:        "I-ORG",
				Certainty:     0.7,
				Word:          "Apple",
				StartPosition: 20,
				EndPosition:   25,
				Property:      "prop",
			},
		}, res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		server := httptest.NewServer(&testNERHandler{
			t: t,
			res: nerResponse{
				Error: "some error from the server",
			},
		})
		defer server.Close()
		c := New(server.URL, nullLogger())
		_, err := c.GetTokens(context.Background(), "prop",
			"I work at Apple")

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "some error from the server")
	})
}

type testNERHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	res nerResponse
}

func (f *testNERHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/ner/", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.res.Error != "" {
		w.WriteHeader(500)
	}

	jsonBytes, _ := json.Marshal(f.res)
	w.Write(jsonBytes)
}
