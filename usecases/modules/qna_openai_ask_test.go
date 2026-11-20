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

package modules

import (
	"context"
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	enschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestValidateSearchParamAskRequiresQnAOpenAI(t *testing.T) {
	const className = "DocumentChunk"
	askValue := map[string]interface{}{"question": "Who is Stanley Kubrick?"}

	tests := []struct {
		name         string
		class        *models.Class
		validateErr  error
		wantErr      string
		wantCalled   bool
		absentSubstr []string
	}{
		{
			name: "text2vec only",
			class: &models.Class{
				Class:      className,
				Vectorizer: "text2vec-openai",
				ModuleConfig: map[string]interface{}{
					"text2vec-openai": map[string]interface{}{"model": "ada"},
				},
			},
			wantErr:    `ask: collection "DocumentChunk" does not configure the qna-openai module`,
			wantCalled: false,
			absentSubstr: []string{
				"without any known params",
				"vectorizer module",
			},
		},
		{
			name: "nil moduleConfig",
			class: &models.Class{
				Class:      className,
				Vectorizer: "text2vec-openai",
			},
			wantErr:    "moduleConfig",
			wantCalled: false,
		},
		{
			name: "moduleConfig is not a map",
			class: &models.Class{
				Class:        className,
				Vectorizer:   "text2vec-openai",
				ModuleConfig: "not-a-map",
			},
			wantErr:    "qna-openai",
			wantCalled: false,
		},
		{
			name: "qna-openai set to null",
			class: &models.Class{
				Class:      className,
				Vectorizer: "text2vec-openai",
				ModuleConfig: map[string]interface{}{
					"text2vec-openai": map[string]interface{}{},
					"qna-openai":      nil,
				},
			},
			wantErr:    "qna-openai",
			wantCalled: false,
		},
		{
			name: "empty qna-openai object is configured",
			class: &models.Class{
				Class:      className,
				Vectorizer: "text2vec-openai",
				ModuleConfig: map[string]interface{}{
					"text2vec-openai": map[string]interface{}{},
					"qna-openai":      map[string]interface{}{},
				},
			},
			wantCalled: true,
		},
		{
			name: "qna-openai with model settings",
			class: &models.Class{
				Class:      className,
				Vectorizer: "text2vec-openai",
				ModuleConfig: map[string]interface{}{
					"text2vec-openai": map[string]interface{}{"model": "ada"},
					"qna-openai":      map[string]interface{}{"model": "gpt-3.5-turbo-instruct"},
				},
			},
			wantCalled: true,
		},
		{
			name: "configured module still validates the ask params",
			class: &models.Class{
				Class:      className,
				Vectorizer: "text2vec-openai",
				ModuleConfig: map[string]interface{}{
					"qna-openai": map[string]interface{}{},
				},
			},
			validateErr: errors.New("ask.question needs to be defined"),
			wantErr:     "ask.question needs to be defined",
			wantCalled:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			p := newAskTestProvider(t, tt.class, &askGraphQLModule{
				name:       qnaOpenAIModuleName,
				moduleType: modulecapabilities.Text2TextQnA,
				validate: func(param interface{}) error {
					called = true
					return tt.validateErr
				},
			})

			err := p.ValidateSearchParam(askArgumentName, askValue, tt.class.Class)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				if tt.wantCalled {
					assert.NotContains(t, err.Error(), "does not configure the qna-openai module")
				} else {
					assert.Contains(t, err.Error(), "moduleConfig")
					assert.Contains(t, err.Error(), className)
				}
				for _, absent := range tt.absentSubstr {
					assert.NotContains(t, err.Error(), absent)
				}
			}
			assert.Equal(t, tt.wantCalled, called)
		})
	}
}

func TestValidateSearchParamAskOtherQnAModuleDoesNotRequireQnAOpenAI(t *testing.T) {
	class := &models.Class{
		Class:      "Books",
		Vectorizer: "text2vec-transformers",
		ModuleConfig: map[string]interface{}{
			"text2vec-transformers": map[string]interface{}{},
		},
	}
	called := false
	p := newAskTestProvider(t, class, &askGraphQLModule{
		name:       "qna-transformers",
		moduleType: modulecapabilities.Text2TextQnA,
		validate: func(param interface{}) error {
			called = true
			return nil
		},
	})

	err := p.ValidateSearchParam(askArgumentName, map[string]interface{}{"question": "Who wrote Dune?"}, class.Class)
	require.NoError(t, err)
	assert.True(t, called, "qna-transformers keeps the single-module default and still validates ask")
}

func TestValidateSearchParamAskUsesTheConfiguredQnAModule(t *testing.T) {
	class := &models.Class{
		Class:      "Books",
		Vectorizer: "text2vec-transformers",
		ModuleConfig: map[string]interface{}{
			"qna-transformers": map[string]interface{}{},
		},
	}
	openAICalled := false
	transformersCalled := false
	p := newAskTestProvider(t, class,
		&askGraphQLModule{
			name:       qnaOpenAIModuleName,
			moduleType: modulecapabilities.Text2TextQnA,
			validate: func(param interface{}) error {
				openAICalled = true
				return nil
			},
		},
		&askGraphQLModule{
			name:       "qna-transformers",
			moduleType: modulecapabilities.Text2TextQnA,
			validate: func(param interface{}) error {
				transformersCalled = true
				return nil
			},
		},
	)

	err := p.ValidateSearchParam(askArgumentName, map[string]interface{}{"question": "Who wrote Dune?"}, class.Class)
	require.NoError(t, err)
	assert.False(t, openAICalled)
	assert.True(t, transformersCalled)
}

func TestValidateSearchParamNearTextDoesNotRequireQnAOpenAI(t *testing.T) {
	class := &models.Class{
		Class:      "DocumentChunk",
		Vectorizer: "text2vec-openai",
		ModuleConfig: map[string]interface{}{
			"text2vec-openai": map[string]interface{}{"model": "ada"},
		},
	}
	p := newAskTestProvider(t, class,
		&askGraphQLModule{
			name:       "text2vec-openai",
			moduleType: modulecapabilities.Text2Vec,
			argName:    "nearText",
			validate:   func(param interface{}) error { return nil },
		},
		&askGraphQLModule{
			name:       qnaOpenAIModuleName,
			moduleType: modulecapabilities.Text2TextQnA,
			validate:   func(param interface{}) error { return errors.New("ask must not be validated for nearText") },
		},
	)

	err := p.ValidateSearchParam("nearText", map[string]interface{}{"concepts": []string{"kubrick"}}, class.Class)
	require.NoError(t, err)
}

func TestValidateSearchParamCrossClassAskDoesNotRequireModuleConfig(t *testing.T) {
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	p.SetSchemaGetter(newMockSchemaReader(t, enschema.Schema{Objects: &models.Schema{}}))
	p.Register(&askGraphQLModule{
		name:       qnaOpenAIModuleName,
		moduleType: modulecapabilities.Text2TextQnA,
		validate:   func(param interface{}) error { return nil },
	})

	require.NoError(t, p.CrossClassValidateSearchParam(askArgumentName, map[string]interface{}{"question": "explore"}))
}

func TestValidateSearchParamExploreUnknownParamDoesNotPanic(t *testing.T) {
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	p.SetSchemaGetter(newMockSchemaReader(t, enschema.Schema{Objects: &models.Schema{}}))

	err := p.CrossClassValidateSearchParam("nearDoesNotExist", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Explore")
	assert.NotContains(t, err.Error(), "nil pointer")
}

func TestModuleConfiguredOnClass(t *testing.T) {
	tests := []struct {
		name   string
		class  *models.Class
		module string
		want   bool
	}{
		{name: "nil class", module: qnaOpenAIModuleName},
		{name: "nil config", class: &models.Class{Class: "C"}, module: qnaOpenAIModuleName},
		{
			name:   "wrong config type",
			class:  &models.Class{Class: "C", ModuleConfig: []string{"qna-openai"}},
			module: qnaOpenAIModuleName,
		},
		{
			name: "other module only",
			class: &models.Class{Class: "C", ModuleConfig: map[string]interface{}{
				"text2vec-openai": map[string]interface{}{},
			}},
			module: qnaOpenAIModuleName,
		},
		{
			name: "null module value",
			class: &models.Class{Class: "C", ModuleConfig: map[string]interface{}{
				"qna-openai": nil,
			}},
			module: qnaOpenAIModuleName,
		},
		{
			name: "empty object",
			class: &models.Class{Class: "C", ModuleConfig: map[string]interface{}{
				"qna-openai": map[string]interface{}{},
			}},
			module: qnaOpenAIModuleName,
			want:   true,
		},
		{
			name: "settings object",
			class: &models.Class{Class: "C", ModuleConfig: map[string]interface{}{
				"qna-openai": map[string]interface{}{"model": "gpt-3.5-turbo-instruct"},
			}},
			module: qnaOpenAIModuleName,
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, moduleConfiguredOnClass(tt.class, tt.module))
		})
	}
}

func newAskTestProvider(t *testing.T, class *models.Class, mods ...modulecapabilities.Module) *Provider {
	t.Helper()
	logger, _ := test.NewNullLogger()
	p := NewProvider(logger, config.Config{})
	p.SetSchemaGetter(newMockSchemaReader(t, enschema.Schema{
		Objects: &models.Schema{Classes: []*models.Class{class}},
	}))
	for _, mod := range mods {
		p.Register(mod)
	}
	return p
}

type askGraphQLModule struct {
	name       string
	moduleType modulecapabilities.ModuleType
	argName    string
	validate   func(param interface{}) error
}

func (m *askGraphQLModule) Name() string { return m.name }

func (m *askGraphQLModule) Init(context.Context, moduletools.ModuleInitParams) error {
	return nil
}

func (m *askGraphQLModule) Type() modulecapabilities.ModuleType { return m.moduleType }

func (m *askGraphQLModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	argName := m.argName
	if argName == "" {
		argName = askArgumentName
	}
	return map[string]modulecapabilities.GraphQLArgument{
		argName: {ValidateFunction: m.validate},
	}
}
