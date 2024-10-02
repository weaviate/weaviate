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

package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
)

func apiBasedModulesTests(endpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		helper.SetupClient(endpoint)

		t.Run("check enabled modules", func(t *testing.T) {
			meta := helper.GetMeta(t)
			require.NotNil(t, meta)

			expectedModuleNames := []string{
				"generative-cohere", "generative-google", "generative-openai", "generative-aws", "generative-anyscale", "generative-friendliai",
				"text2vec-cohere", "text2vec-contextionary", "text2vec-openai", "text2vec-huggingface",
				"text2vec-google", "text2vec-aws", "text2vec-transformers", "qna-openai", "reranker-cohere",
				"text2vec-voyageai", "reranker-voyageai",
			}

			modules, ok := meta.Modules.(map[string]interface{})
			require.True(t, ok)
			assert.True(t, len(modules) >= len(expectedModuleNames))

			moduleNames := []string{}
			for name := range modules {
				moduleNames = append(moduleNames, name)
			}
			for _, name := range expectedModuleNames {
				assert.Contains(t, moduleNames, name)
			}
		})
	}
}
