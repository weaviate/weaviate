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

package modrerankeropenai

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestMaxConcurrentRequestsFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    int
		wantErr string
	}{
		{name: "unset or empty", value: "", want: 32},
		{name: "lower bound", value: "1", want: 1},
		{name: "upper bound", value: "256", want: 256},
		{name: "zero", value: "0", wantErr: `RERANKER_OPENAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "0"`},
		{name: "above the limit", value: "257", wantErr: `RERANKER_OPENAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "257"`},
		{name: "negative", value: "-4", wantErr: `RERANKER_OPENAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "-4"`},
		{name: "not a number", value: "many", wantErr: `RERANKER_OPENAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "many"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(maxConcurrentRequestsEnv, tt.value)
			got, err := maxConcurrentRequestsFromEnv()
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMaxDocumentsPerQueryFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    int
		wantErr string
	}{
		{name: "unset or empty", value: "", want: 1000},
		{name: "lower bound", value: "1", want: 1},
		{name: "above the default", value: "5000", want: 5000},
		{name: "zero", value: "0", wantErr: `RERANKER_OPENAI_MAX_DOCUMENTS_PER_QUERY must be a whole number of at least 1, got "0"`},
		{name: "negative", value: "-4", wantErr: `RERANKER_OPENAI_MAX_DOCUMENTS_PER_QUERY must be a whole number of at least 1, got "-4"`},
		{name: "not a number", value: "many", wantErr: `RERANKER_OPENAI_MAX_DOCUMENTS_PER_QUERY must be a whole number of at least 1, got "many"`},
		{name: "a fraction", value: "1.5", wantErr: `RERANKER_OPENAI_MAX_DOCUMENTS_PER_QUERY must be a whole number of at least 1, got "1.5"`},
		{
			name:    "above the int range",
			value:   "99999999999999999999",
			wantErr: `RERANKER_OPENAI_MAX_DOCUMENTS_PER_QUERY must be a whole number of at least 1, got "99999999999999999999"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(maxDocumentsPerQueryEnv, tt.value)
			got, err := maxDocumentsPerQueryFromEnv()
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRerankPropertyDataTypes(t *testing.T) {
	assert.Equal(t, []schema.DataType{schema.DataTypeText, schema.DataTypeBlob}, New().RerankPropertyDataTypes())
}

func TestValidateRerankQuery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr string
	}{
		{name: "a query", query: "The customer is about to churn"},
		{name: "a blank query is a query", query: " "},
		{name: "no query", wantErr: "rerank.query is required by reranker-openai: the query is the condition the model judges"},
		{name: "the longest query", query: strings.Repeat("q", 100_000)},
		{
			name:    "a query one byte above the limit",
			query:   strings.Repeat("q", 100_001),
			wantErr: "rerank.query is 100001 bytes; reranker-openai sends it with every document and takes at most 100000 bytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := New().ValidateRerankQuery(tt.query)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateClassRejectsAWrongSetting(t *testing.T) {
	class := &models.Class{
		Class: "Doc",
		ModuleConfig: map[string]any{
			Name: map[string]any{"model": 42},
		},
	}
	cfg := modules.NewClassBasedModuleConfig(class, Name, "", "", &config.Config{})

	err := New().ValidateClass(context.Background(), class, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "model")
}

func TestInitReadsTheConcurrencyEnv(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		set     bool
		wantErr bool
	}{
		{name: "unset"},
		{name: "a valid value", value: "8", set: true},
		{name: "zero", value: "0", set: true, wantErr: true},
		{name: "not a number", value: "abc", set: true, wantErr: true},
		{name: "above the limit", value: "257", set: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(maxConcurrentRequestsEnv, "")
			if tt.set {
				t.Setenv(maxConcurrentRequestsEnv, tt.value)
			}
			logger, _ := test.NewNullLogger()
			m := New()

			err := m.Init(context.Background(), moduletools.NewInitParams("", nil,
				&config.Config{ModuleHttpClientTimeout: time.Second}, logger, nil))

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), maxConcurrentRequestsEnv)
				assert.Nil(t, m.reranker)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, m.reranker)
		})
	}
}

func TestInitReadsTheDocumentsPerQueryEnv(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr string
	}{
		{name: "unset"},
		{name: "a valid value", value: "8"},
		{name: "zero", value: "0", wantErr: `init reranker: RERANKER_OPENAI_MAX_DOCUMENTS_PER_QUERY must be a whole number of at least 1, got "0"`},
		{name: "negative", value: "-1", wantErr: `init reranker: RERANKER_OPENAI_MAX_DOCUMENTS_PER_QUERY must be a whole number of at least 1, got "-1"`},
		{name: "not a number", value: "abc", wantErr: `init reranker: RERANKER_OPENAI_MAX_DOCUMENTS_PER_QUERY must be a whole number of at least 1, got "abc"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(maxConcurrentRequestsEnv, "")
			t.Setenv(maxDocumentsPerQueryEnv, tt.value)
			logger, _ := test.NewNullLogger()
			m := New()

			err := m.Init(context.Background(), moduletools.NewInitParams("", nil,
				&config.Config{ModuleHttpClientTimeout: time.Second}, logger, nil))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Nil(t, m.reranker)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, m.reranker)
		})
	}
}

func TestInitPassesTheDocumentsPerQueryCap(t *testing.T) {
	t.Setenv(maxConcurrentRequestsEnv, "")
	t.Setenv(maxDocumentsPerQueryEnv, "2")
	t.Setenv("OPENAI_APIKEY", "testKey")
	var received atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	logger, _ := test.NewNullLogger()
	m := New()
	require.NoError(t, m.Init(context.Background(), moduletools.NewInitParams("", nil,
		&config.Config{ModuleHttpClientTimeout: time.Second}, logger, nil)))
	class := &models.Class{
		Class:        "Doc",
		Properties:   []*models.Property{{Name: "text", DataType: schema.DataTypeText.PropString()}},
		ModuleConfig: map[string]any{Name: map[string]any{"baseURL": server.URL}},
	}
	cfg := modules.NewClassBasedModuleConfig(class, Name, "", "", &config.Config{})

	_, err := m.reranker.RankProperty(context.Background(), "q", "text",
		[]ent.Document{{ID: "1", Value: "first"}, {ID: "2", Value: "second"}, {ID: "3", Value: "third"}}, cfg)

	require.EqualError(t, err, "rerank of 3 objects: reranker-openai sends one request per object and takes at most 2 per query "+
		"(RERANKER_OPENAI_MAX_DOCUMENTS_PER_QUERY)")
	assert.Zero(t, received.Load())
}

func TestInitPassesTheClientTimeout(t *testing.T) {
	t.Setenv(maxConcurrentRequestsEnv, "")
	t.Setenv("OPENAI_APIKEY", "testKey")
	hold := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-hold:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	logger, _ := test.NewNullLogger()
	m := New()
	require.NoError(t, m.Init(context.Background(), moduletools.NewInitParams("", nil,
		&config.Config{ModuleHttpClientTimeout: 50 * time.Millisecond}, logger, nil)))
	class := &models.Class{
		Class:        "Doc",
		Properties:   []*models.Property{{Name: "text", DataType: schema.DataTypeText.PropString()}},
		ModuleConfig: map[string]any{Name: map[string]any{"baseURL": server.URL}},
	}
	cfg := modules.NewClassBasedModuleConfig(class, Name, "", "", &config.Config{})

	errs := make(chan error, 1)
	var calls sync.WaitGroup
	t.Cleanup(calls.Wait)
	t.Cleanup(func() { close(hold) })
	calls.Go(func() {
		_, err := m.reranker.RankProperty(context.Background(), "q", "text",
			[]ent.Document{{ID: "1", Value: "doc"}}, cfg)
		errs <- err
	})

	select {
	case err := <-errs:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("RankProperty did not return within 5 seconds")
	}
}
