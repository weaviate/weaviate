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

package rest_search

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

var (
	restSearchOnce    sync.Once
	restSearchCompose *docker.DockerCompose
	restSearchErr     error
)

// restSearchServerURI returns the package's Weaviate, starting it on first
// use. The tests share it and use distinct class names.
func restSearchServerURI(t *testing.T) string {
	t.Helper()
	restSearchOnce.Do(func() {
		restSearchCompose, restSearchErr = docker.New().
			WithWeaviate().
			WithText2VecModel2Vec().
			// no key on purpose: queries fail at the module's key check, no network call
			WithText2VecOpenAI("", "", "").
			Start(context.Background())
	})
	require.NoError(t, restSearchErr)
	return restSearchCompose.GetWeaviate().URI()
}

func TestMain(m *testing.M) {
	code := m.Run()
	if restSearchCompose != nil {
		if err := restSearchCompose.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "failed to terminate rest search server: %v\n", err)
		}
	}
	os.Exit(code)
}

// TestRESTSearchVectorizerPrecondition: without the provider API key header
// the module rejects the query before any provider call, so the caller gets
// 422 with the module's message instead of the generic 500.
func TestRESTSearchVectorizerPrecondition(t *testing.T) {
	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(restSearchServerURI(t))

	class := &models.Class{
		Class:      "Keyless",
		Vectorizer: "text2vec-openai",
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, class.Class)

	// objects carry their own vectors, so the import needs no vectorizer
	helper.CreateObjectsBatch(t, []*models.Object{
		{
			ID:         strfmt.UUID("dd44bbee-ca5f-4db7-a412-5fc6a2300001"),
			Class:      class.Class,
			Vector:     models.C11yVector{1, 0},
			Properties: map[string]any{"title": "spaceship galaxy"},
		},
		{
			ID:         strfmt.UUID("dd44bbee-ca5f-4db7-a412-5fc6a2300002"),
			Class:      class.Class,
			Vector:     models.C11yVector{0, 1},
			Properties: map[string]any{"title": "cooking dinner"},
		},
	})

	const wantMsg = "no api key found neither in request header: X-Openai-Api-Key"

	for _, tc := range []struct {
		name       string
		searchType string
		body       map[string]any
	}{
		{name: "near-text", searchType: "near-text", body: map[string]any{"query": []string{"spaceship"}}},
		{name: "hybrid with the default alpha", searchType: "hybrid", body: map[string]any{"query": "spaceship"}},
	} {
		t.Run(tc.name+" without the key header is a 422 naming the header", func(t *testing.T) {
			status, out := postSearch(t, class.Class, tc.searchType, tc.body)
			require.Equal(t, http.StatusUnprocessableEntity, status, "%v", out)
			msg := errMessage(t, out)
			assert.Contains(t, msg, wantMsg)
			assert.NotContains(t, msg, "server log")
		})
	}
}
