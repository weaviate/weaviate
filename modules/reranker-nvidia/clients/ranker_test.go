//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	"github.com/weaviate/weaviate/usecases/config"
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

func TestRankResultIndexes(t *testing.T) {
	documents := []string{"I work at Apple", "I work at Google"}
	tests := []struct {
		name     string
		rankings []ranking
		want     []ent.DocumentScore
		wantErr  string
	}{
		{
			name:     "rankings in a different order than the documents",
			rankings: []ranking{{Index: 1, Logit: 0.2}, {Index: 0, Logit: 0.9}},
			want:     []ent.DocumentScore{{Document: documents[0], Score: 0.9}, {Document: documents[1], Score: 0.2}},
		},
		{
			name:     "index equal to the number of documents",
			rankings: []ranking{{Index: 0, Logit: 0.9}, {Index: 2, Logit: 0.2}},
			wantErr:  "result index 2 out of range for 2 documents",
		},
		{
			name:     "negative index",
			rankings: []ranking{{Index: -1, Logit: 0.9}, {Index: 1, Logit: 0.2}},
			wantErr:  "result index -1 out of range for 2 documents",
		},
		{
			name:     "duplicate index",
			rankings: []ranking{{Index: 0, Logit: 0.9}, {Index: 0, Logit: 0.2}},
			wantErr:  "duplicate result index 0",
		},
		{
			name:     "fewer rankings than documents",
			rankings: []ranking{{Index: 0, Logit: 0.9}},
			wantErr:  "1 results for 2 documents",
		},
		{
			name:     "more rankings than documents",
			rankings: []ranking{{Index: 0, Logit: 0.9}, {Index: 1, Logit: 0.2}, {Index: 2, Logit: 0.1}},
			wantErr:  "3 results for 2 documents",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(&testCrossRankerHandler{t: t, res: rankResponse{Rankings: tt.rankings}})
			defer server.Close()
			c := New("key", 0, nullLogger())
			cfg := fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}}

			res, err := c.Rank(context.Background(), "Where do I work?", documents, cfg)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Nil(t, res)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, res.DocumentScores)
		})
	}
}

func TestRankErrorResponses(t *testing.T) {
	tests := []struct {
		name     string
		error402 *responseError402
		error422 *responseError422
		wantErr  string
	}{
		{
			name:     "402 with detail",
			error402: &responseError402{Detail: "payment required"},
			wantErr:  "status: 402 error: payment required",
		},
		{
			name:     "422 with one detail",
			error422: &responseError422{Detail: []errorDetail{{Message: "field required"}}},
			wantErr:  "status: 422 error: field required",
		},
		{
			name: "422 with several details",
			error422: &responseError422{Detail: []errorDetail{
				{Message: "query is required"},
				{Message: "passages must not be empty"},
			}},
			wantErr: "status: 422 error: query is required passages must not be empty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(&testCrossRankerHandler{t: t, error402: tt.error402, error422: tt.error422})
			defer server.Close()
			c := New("key", 0, nullLogger())
			cfg := fakeClassConfig{classConfig: map[string]interface{}{"Model": "large", "baseURL": server.URL}}

			_, err := c.Rank(context.Background(), "Where do I work?", []string{"I work at Apple"}, cfg)

			require.Error(t, err)
			assert.Equal(t, "connection to NVIDIA API failed with "+tt.wantErr, err.Error())
		})
	}
}

type testCrossRankerHandler struct {
	lock           sync.RWMutex
	t              *testing.T
	res            rankResponse
	batchedResults []rankResponse
	error402       *responseError402
	error422       *responseError422
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
	if f.error422 != nil {
		w.WriteHeader(http.StatusUnprocessableEntity)
		jsonBytes, _ := json.Marshal(f.error422)
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

func (f fakeClassConfig) Config() *config.Config {
	return nil
}

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
