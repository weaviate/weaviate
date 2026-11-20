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
	"strconv"
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
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestValidateClassRejectsAnEmptyModel(t *testing.T) {
	class := &models.Class{
		Class: "Doc",
		ModuleConfig: map[string]any{
			Name: map[string]any{"model": ""},
		},
	}
	cfg := modules.NewClassBasedModuleConfig(class, Name, "", "", &config.Config{})

	err := New().ValidateClass(context.Background(), class, cfg)
	require.EqualError(t, err, "no model provided")
}

func TestInitPassesTheDocumentsPerQueryCap(t *testing.T) {
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
	documents := make([]string, 1001)
	for i := range documents {
		documents[i] = "document " + strconv.Itoa(i)
	}

	_, err := m.reranker.Rank(context.Background(), "q", "text", documents, cfg)

	require.EqualError(t, err, "rerank of 1001 objects: reranker-openai sends one request per object and takes at most 1000 per query")
	assert.Zero(t, received.Load())
}

func TestInitPassesTheClientTimeout(t *testing.T) {
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
		_, err := m.reranker.Rank(context.Background(), "q", "text",
			[]string{"doc"}, cfg)
		errs <- err
	})

	select {
	case err := <-errs:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("Rank did not return within 5 seconds")
	}
}
