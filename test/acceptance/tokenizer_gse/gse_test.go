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

// Package tokenizer_gse checks that the gse (Japanese) and gse_ch (Chinese)
// tokenizers work in the shipped Docker image. The image is built with
// -trimpath, which broke gse's file-based dictionary lookup in v1.34.0 without
// any test noticing: the tokenizer logged an error at startup, classes using it
// were still accepted, and every BM25 query then failed.
package tokenizer_gse

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func TestGseTokenizersInDockerImage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("ENABLE_TOKENIZER_GSE", "true").
		WithWeaviateEnv("ENABLE_TOKENIZER_GSE_CH", "true").
		Start(ctx)
	require.NoError(t, err, "start Weaviate with gse tokenizers enabled")
	t.Cleanup(func() {
		if err := compose.Terminate(context.Background()); err != nil {
			t.Logf("terminate: %v", err)
		}
	})
	helper.SetupClient(compose.GetWeaviate().URI())

	cases := []struct {
		name         string
		tokenization string
		text         string
		query        string // a word inside text that only a dictionary tokenizer can split out
	}{
		{"japanese", models.PropertyTokenizationGse, "私は東京に住んでいます", "東京"},
		{"chinese", models.PropertyTokenizationGseCh, "我爱北京天安门", "北京"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			className := "Gse" + tc.name
			helper.CreateClass(t, &models.Class{
				Class:      className,
				Vectorizer: "none",
				Properties: []*models.Property{{
					Name:         "text",
					DataType:     []string{"text"},
					Tokenization: tc.tokenization,
				}},
			})
			t.Cleanup(func() { helper.DeleteClass(t, className) })
			require.NoError(t, helper.CreateObject(t, &models.Object{
				Class:      className,
				Properties: map[string]interface{}{"text": tc.text},
			}))

			query := fmt.Sprintf(`{ Get { %s(bm25: {query: %q, properties: ["text"]}) { text } } }`, className, tc.query)
			results := graphqlhelper.AssertGraphQL(t, nil, query).Get("Get", className).AsSlice()
			require.Len(t, results, 1, "bm25 for %q must find the object tokenized with %s", tc.query, tc.tokenization)
		})
	}
}
