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

package modrerankertypesafeai

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

func TestRerankPropertyDataTypes(t *testing.T) {
	assert.Equal(t, []schema.DataType{schema.DataTypeText, schema.DataTypeBlob}, New().RerankPropertyDataTypes())
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

func TestInitCreatesTheClient(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m := New()

	err := m.Init(context.Background(), moduletools.NewInitParams("", nil, &config.Config{}, logger, nil))

	require.NoError(t, err)
	assert.NotNil(t, m.reranker)
}

func TestValidateRerankQuery(t *testing.T) {
	// 16 bytes are the "In document_49: " before the query of the last
	// document of a full request.
	longest := 100_000 - 16
	tests := []struct {
		name    string
		query   string
		wantErr string
	}{
		{name: "a query", query: "The customer is about to churn"},
		{name: "a blank query is a query", query: " "},
		{name: "no query", wantErr: "rerank.query is required by reranker-typesafeai: the query is the condition the model judges"},
		{name: "the longest query", query: strings.Repeat("q", longest-1)},
		{
			name:    "a query whose question fills a request",
			query:   strings.Repeat("q", longest),
			wantErr: "rerank.query is 99984 bytes; reranker-typesafeai repeats it for every document within 100000 bytes per request",
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

func TestInitGivesTheClientTheHTTPTimeout(t *testing.T) {
	t.Setenv("TYPESAFEAI_APIKEY", "test-key")
	release := make(chan struct{})
	var call sync.WaitGroup
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	// Cleanups run last in, first out: release the handlers, wait for the
	// call, then close the server.
	t.Cleanup(provider.Close)
	t.Cleanup(call.Wait)
	t.Cleanup(func() { close(release) })

	logger, _ := test.NewNullLogger()
	m := New()
	require.NoError(t, m.Init(context.Background(),
		moduletools.NewInitParams("", nil, &config.Config{ModuleHttpClientTimeout: 50 * time.Millisecond}, logger, nil)))
	class := &models.Class{
		Class:      "Doc",
		Properties: []*models.Property{{Name: "text", DataType: []string{"text"}}},
		ModuleConfig: map[string]any{
			Name: map[string]any{"baseURL": provider.URL},
		},
	}
	cfg := modules.NewClassBasedModuleConfig(class, Name, "", "", &config.Config{})

	result := make(chan error, 1)
	call.Go(func() {
		_, err := m.reranker.RankProperty(context.Background(), "q", "text",
			[]ent.Document{{ID: "1", Value: "a document"}}, cfg)
		result <- err
	})

	select {
	case err := <-result:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "got %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the rerank call did not time out")
	}
}
