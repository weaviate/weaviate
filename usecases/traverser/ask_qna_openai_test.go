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

package traverser

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
)

// An ask query on a class that never names qna-openai used to fall through
// vector search and report "vectorFromParams was called without any known
// params present" (or a generic vectorizer error). The search must say that
// the module is missing from moduleConfig instead.
func TestVectorFromParams_AskWithoutQnAOpenAIOnClass(t *testing.T) {
	class := &models.Class{
		Class:      "DocumentChunk",
		Vectorizer: "text2vec-openai",
		ModuleConfig: map[string]interface{}{
			"text2vec-openai": map[string]interface{}{"model": "ada"},
		},
	}
	params := map[string]interface{}{
		"ask": map[string]interface{}{"question": "Who is Stanley Kubrick?"},
	}
	vector := newNearParamsVector(askTestProvider(t, class), &fakeNearParamsSearcher{})

	_, err := vector.targetFromParams(context.Background(), nil, nil, params, class.Class, "")
	require.Error(t, err)
	assertAskModuleConfigError(t, err)

	_, err = vector.vectorFromParams(context.Background(), nil, nil, params, class.Class, "", "", 0)
	require.Error(t, err)
	assertAskModuleConfigError(t, err)
}

func TestVectorFromParams_AskWithQnAOpenAIOnClass(t *testing.T) {
	class := &models.Class{
		Class:      "DocumentChunk",
		Vectorizer: "text2vec-openai",
		ModuleConfig: map[string]interface{}{
			"text2vec-openai": map[string]interface{}{"model": "ada"},
			"qna-openai":      map[string]interface{}{},
		},
	}
	params := map[string]interface{}{
		"ask": map[string]interface{}{"question": "Who is Stanley Kubrick?"},
	}
	vector := newNearParamsVector(askTestProvider(t, class), &fakeNearParamsSearcher{})

	targets, err := vector.targetFromParams(context.Background(), nil, nil, params, class.Class, "")
	require.NoError(t, err)
	assert.Equal(t, []string{""}, targets)

	_, err = vector.vectorFromParams(context.Background(), nil, nil, params, class.Class, "", "", 0)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "does not configure the qna-openai module")
	assert.Contains(t, err.Error(), "vectorizer")
}

func assertAskModuleConfigError(t *testing.T, err error) {
	t.Helper()
	assert.Contains(t, err.Error(), `collection "DocumentChunk" does not configure the qna-openai module`)
	assert.Contains(t, err.Error(), "moduleConfig")
	assert.NotContains(t, err.Error(), "without any known params")
	assert.NotContains(t, err.Error(), "vectorizer module")
}

func askTestProvider(t *testing.T, class *models.Class) *modules.Provider {
	t.Helper()
	schemaReader := local.NewMockSchemaReader(t)
	schemaReader.EXPECT().ReadOnlyClass(class.Class).Return(class).Maybe()
	logger, _ := test.NewNullLogger()
	provider := modules.NewProvider(logger, config.Config{})
	provider.SetSchemaGetter(schemaReader)
	provider.Register(&qnaOpenAIAskModule{})
	return provider
}

type qnaOpenAIAskModule struct{}

func (m *qnaOpenAIAskModule) Name() string { return "qna-openai" }

func (m *qnaOpenAIAskModule) Init(context.Context, moduletools.ModuleInitParams) error {
	return nil
}

func (m *qnaOpenAIAskModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextQnA
}

func (m *qnaOpenAIAskModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return map[string]modulecapabilities.GraphQLArgument{
		"ask": {
			ValidateFunction: func(param interface{}) error { return nil },
		},
	}
}
