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
	"github.com/weaviate/weaviate/modules/ner-transformers/ent"
)

func TestGetAnswer(t *testing.T) {
	t.Run("when the server has a successful answer (with distance)", func(t *testing.T) {
		server := httptest.NewServer(&testNERHandler{
			t: t,
			res: nerResponse{
				nerInput: nerInput{
					Text: "I work at Apple",
				},
				Tokens: []tokenResponse{
					{
						Entity:        "I-ORG",
						Distance:      0.3,
						Word:          "Apple",
						StartPosition: 20,
						EndPosition:   25,
					},
				},
			},
		})
		defer server.Close()
		c := New(server.URL, 0, nullLogger())
		res, err := c.GetTokens(context.Background(), "prop",
			"I work at Apple")

		assert.Nil(t, err)
		assert.Equal(t, []ent.TokenResult{
			{
				Entity:        "I-ORG",
				Distance:      0.3,
				Word:          "Apple",
				StartPosition: 20,
				EndPosition:   25,
				Property:      "prop",
			},
		}, res)
	})

	t.Run("when the server has a successful answer (with certainty)", func(t *testing.T) {
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
		c := New(server.URL, 0, nullLogger())
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
		c := New(server.URL, 0, nullLogger())
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
