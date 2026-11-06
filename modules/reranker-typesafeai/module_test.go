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

func TestMaxConcurrentRequestsFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    int
		wantErr string
	}{
		{name: "unset or empty", value: "", want: 16},
		{name: "lower bound", value: "1", want: 1},
		{name: "upper bound", value: "256", want: 256},
		{name: "zero", value: "0", wantErr: `RERANKER_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "0"`},
		{name: "above the limit", value: "257", wantErr: `RERANKER_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "257"`},
		{name: "negative", value: "-4", wantErr: `RERANKER_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "-4"`},
		{name: "not a number", value: "many", wantErr: `RERANKER_TYPESAFEAI_MAX_CONCURRENT_REQUESTS must be a whole number between 1 and 256, got "many"`},
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

func TestInit(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "unset"},
		{name: "a valid cap", value: "8"},
		{name: "zero", value: "0", wantErr: true},
		{name: "not a number", value: "abc", wantErr: true},
		{name: "above the limit", value: "257", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(maxConcurrentRequestsEnv, tt.value)
			logger, _ := test.NewNullLogger()
			m := New()

			err := m.Init(context.Background(), moduletools.NewInitParams("", nil, &config.Config{}, logger, nil))

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

func TestInitGivesTheClientTheHTTPTimeout(t *testing.T) {
	t.Setenv(maxConcurrentRequestsEnv, "")
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
