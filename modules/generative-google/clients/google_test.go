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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	googleparams "github.com/weaviate/weaviate/modules/generative-google/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents/apikey"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestGetAnswer(t *testing.T) {
	t.Run("when the server has a successful answer ", func(t *testing.T) {
		prompt := "John"
		handler := &testAnswerHandler{
			t: t,
			answer: generateResponse{
				Predictions: []prediction{
					{
						Candidates: []candidate{
							{
								Content: &prompt,
							},
						},
					},
				},
				Error: nil,
			},
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := &google{
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			buildUrlFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID, region string) string {
				return server.URL
			},
			logger: nullLogger(),
		}

		params := googleparams.Params{ApiEndpoint: server.URL}
		props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is john"}}}
		expected := modulecapabilities.GenerateResponse{
			Result: ptString("John"),
		}

		res, err := c.GenerateAllResults(context.Background(), props, "What is my name?", params, false, nil)

		assert.Nil(t, err)
		assert.Equal(t, expected, *res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: generateResponse{
				Error: &googleApiError{
					Message: "some error from the server",
				},
			},
		})
		defer server.Close()

		c := &google{
			apiKey:       "apiKey",
			httpClient:   &http.Client{},
			googleApiKey: apikey.NewGoogleApiKey(),
			buildUrlFn: func(useGenerativeAI bool, apiEndoint, projectID, modelID, region string) string {
				return server.URL
			},
			logger: nullLogger(),
		}

		props := []*modulecapabilities.GenerateProperties{{Text: map[string]string{"prop": "My name is john"}}}

		_, err := c.GenerateAllResults(context.Background(), props, "What is my name?", nil, false, nil)

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to Google failed with status: 500 error: some error from the server")
	})
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer generateResponse
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	var b generateInput
	require.Nil(f.t, json.Unmarshal(bodyBytes, &b))

	require.Len(f.t, b.Instances, 1)
	require.Len(f.t, b.Instances[0].Messages, 1)
	require.True(f.t, len(b.Instances[0].Messages[0].Content) > 0)

	outBytes, err := json.Marshal(f.answer)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func ptString(in string) *string {
	return &in
}
