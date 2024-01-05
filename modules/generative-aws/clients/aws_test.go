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
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestGetAnswer(t *testing.T) {
	t.Run("when the server has a successful answer ", func(t *testing.T) {
		t.Skip("Skipping this test for now")
		handler := &testAnswerHandler{
			t: t,
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := &aws{
			httpClient:   &http.Client{},
			logger:       nullLogger(),
			awsAccessKey: "123",
			awsSecretKey: "123",
			buildBedrockUrlFn: func(service, region, model string) string {
				return server.URL
			},
			buildSagemakerUrlFn: func(service, region, endpoint string) string {
				return server.URL
			},
		}

		textProperties := []map[string]string{{"prop": "My name is john"}}
		expected := generativemodels.GenerateResponse{
			Result: ptString("John"),
		}

		res, err := c.GenerateAllResults(context.Background(), textProperties, "What is my name?", nil)

		assert.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		t.Skip("Skipping this test for now")
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
		})
		defer server.Close()

		c := &aws{
			httpClient:   &http.Client{},
			logger:       nullLogger(),
			awsAccessKey: "123",
			awsSecretKey: "123",
			buildBedrockUrlFn: func(service, region, model string) string {
				return server.URL
			},
			buildSagemakerUrlFn: func(service, region, endpoint string) string {
				return server.URL
			},
		}

		textProperties := []map[string]string{{"prop": "My name is john"}}

		_, err := c.GenerateAllResults(context.Background(), textProperties, "What is my name?", nil)

		require.NotNil(t, err)
		assert.EqualError(t, err, "connection to AWS failed with status: 200 error: some error from the server")
	})
}

type testAnswerHandler struct {
	t *testing.T
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var outBytes []byte
	authHeader := r.Header["Authorization"][0]
	if strings.Contains(authHeader, "bedrock") {
		var request bedrockAmazonGenerateRequest
		require.Nil(f.t, json.Unmarshal(bodyBytes, &request))

		outBytes, err = json.Marshal(request)
		require.Nil(f.t, err)
	}

	w.Write(outBytes)
}

func ptString(in string) *string {
	return &in
}
