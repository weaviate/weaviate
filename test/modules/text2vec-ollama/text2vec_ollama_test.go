//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package tests

import (
	"testing"

	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/companies"
)

func testText2VecOllama(rest, grpc, ollamaApiEndpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(rest)
		className := "OllamaVectorizerTest"
		descriptionVectorizer := map[string]any{
			"text2vec-ollama": map[string]any{
				"properties":         []any{"description"},
				"vectorizeClassName": false,
				"apiEndpoint":        ollamaApiEndpoint,
				"model":              "nomic-embed-text",
			},
		}
		emptyVectorizer := map[string]any{
			"text2vec-ollama": map[string]any{
				"properties":         []any{"empty"},
				"vectorizeClassName": false,
				"apiEndpoint":        ollamaApiEndpoint,
				"model":              "nomic-embed-text",
			},
		}
		t.Run("search", func(t *testing.T) {
			companies.TestSuite(t, rest, grpc, className, descriptionVectorizer)
		})
		t.Run("empty values", func(t *testing.T) {
			companies.TestSuiteWithEmptyValues(t, rest, grpc, className, descriptionVectorizer, emptyVectorizer)
		})
	}
}
