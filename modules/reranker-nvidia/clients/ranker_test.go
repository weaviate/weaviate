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

package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

func TestGetScore(t *testing.T) {
	t.Run("when the server has a successful answer", func(t *testing.T) {
		server := httptest.NewServer(&testCrossRankerHandler{
			t: t,
			res: rankResponse{
				Rankings: []ranking{
					{Index: 0, Logit: 0.15},
				},
			},
		})
		defer server.Close()
		c := New("key", 0, nullLogger())

		cfg := fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}}
		res, err := c.Rank(context.Background(), "Where do I work?", []string{"I work at Apple"}, cfg)

		assert.Nil(t, err)
		assert.Equal(t, ent.RankResult{
			Query: "Where do I work?",
			DocumentScores: []ent.DocumentScore{
				{
					Document: "I work at Apple",
					Score:    0.15,
				},
			},
		}, *res)
	})

	t.Run("when the server has an error", func(t *testing.T) {
		server := httptest.NewServer(&testCrossRankerHandler{
			t: t,
			error402: &responseError402{
				Detail: "some error from the server",
			},
		})
		defer server.Close()
		c := New("key", 0, nullLogger())

		cfg := fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}}
		_, err := c.Rank(context.Background(), "prop", []string{"I work at Apple"}, cfg)

		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "some error from the server")
	})

	t.Run("when we send requests in batches", func(t *testing.T) {
		server := httptest.NewServer(&testCrossRankerHandler{
			t: t,
			batchedResults: []rankResponse{
				{
					Rankings: []ranking{
						{Index: 0, Logit: 0.99},
						{Index: 1, Logit: 0.89},
					},
				},
				{
					Rankings: []ranking{
						{Index: 0, Logit: 0.299},
						{Index: 1, Logit: 0.289},
					},
				},
				{
					Rankings: []ranking{
						{Index: 0, Logit: 0.199},
						{Index: 1, Logit: 0.189},
					},
				},
				{
					Rankings: []ranking{
						{Index: 0, Logit: 0.0001},
					},
				},
			},
		})
		defer server.Close()

		c := New("apiKey", 0, nullLogger())
		c.maxDocuments = 2

		query := "Where do I work?"
		documents := []string{
			"Response 1", "Response 2", "Response 3", "Response 4",
			"Response 5", "Response 6", "Response 7",
		}

		cfg := fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}}
		resp, err := c.Rank(context.Background(), query, documents, cfg)

		require.Nil(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.DocumentScores)
		for i := range resp.DocumentScores {
			assert.Equal(t, documents[i], resp.DocumentScores[i].Document)
			if i == 0 {
				assert.Equal(t, 0.99, resp.DocumentScores[i].Score)
			}
			if i == len(documents)-1 {
				assert.Equal(t, 0.0001, resp.DocumentScores[i].Score)
			}
		}
	})
}

type testCrossRankerHandler struct {
	lock           sync.RWMutex
	t              *testing.T
	res            rankResponse
	batchedResults []rankResponse
	error402       *responseError402
}

func (f *testCrossRankerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.lock.Lock()
	defer f.lock.Unlock()

	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.error402 != nil {
		w.WriteHeader(http.StatusPaymentRequired)
		jsonBytes, _ := json.Marshal(f.error402)
		w.Write(jsonBytes)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var req rankRequest
	require.Nil(f.t, json.Unmarshal(bodyBytes, &req))

	containsDocument := func(req rankRequest, in string) bool {
		for _, doc := range req.Passages {
			if doc.Text == in {
				return true
			}
		}
		return false
	}

	index := 0
	if len(f.batchedResults) > 0 {
		if containsDocument(req, "Response 3") {
			index = 1
		}
		if containsDocument(req, "Response 5") {
			index = 2
		}
		if containsDocument(req, "Response 7") {
			index = 3
		}
		f.res = f.batchedResults[index]
	}

	jsonBytes, _ := json.Marshal(f.res)
	w.Write(jsonBytes)
}

type fakeClassConfig struct {
	classConfig           map[string]interface{}
	vectorizePropertyName bool
	skippedProperty       string
	excludedProperty      string
}

func (f fakeClassConfig) Class() map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	return f.classConfig
}

func (f fakeClassConfig) Property(propName string) map[string]interface{} {
	if propName == f.skippedProperty {
		return map[string]interface{}{
			"skip": true,
		}
	}
	if propName == f.excludedProperty {
		return map[string]interface{}{
			"vectorizePropertyName": false,
		}
	}
	if f.vectorizePropertyName {
		return map[string]interface{}{
			"vectorizePropertyName": true,
		}
	}
	return nil
}

func (f fakeClassConfig) Tenant() string {
	return ""
}

func (f fakeClassConfig) TargetVector() string {
	return ""
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
