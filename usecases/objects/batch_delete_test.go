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

package objects

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func Test_BatchDelete_RequestValidation(t *testing.T) {
	var (
		vectorRepo *fakeVectorRepo
		manager    *BatchManager
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "Foo",
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
					},
					VectorIndexConfig: hnsw.UserConfig{},
					Vectorizer:        config.VectorizerModuleNone,
				},
			},
		},
	}

	resetAutoSchema := func(autoSchema bool) {
		vectorRepo = &fakeVectorRepo{}
		config := &config.WeaviateConfig{
			Config: config.Config{
				AutoSchema: config.AutoSchema{
					Enabled: runtime.NewDynamicValue(autoSchema),
				},
			},
		}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		logger, _ := test.NewNullLogger()
		authorizer := mocks.NewMockAuthorizer()
		modulesProvider := getFakeModulesProvider()
		manager = NewBatchManager(vectorRepo, modulesProvider, schemaManager, config, logger, authorizer, nil,
			NewAutoSchemaManager(schemaManager, vectorRepo, config, logger, prometheus.NewPedanticRegistry()))
	}

	reset := func() {
		resetAutoSchema(false)
	}
	ctx := context.Background()

	reset()

	t.Run("with invalid input", func(t *testing.T) {
		tests := []struct {
			input         *models.BatchDelete
			expectedError string
		}{
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString(verbosity.OutputVerbose),
					Match: &models.BatchDeleteMatch{
						Class: "SomeClass",
						Where: &models.WhereFilter{
							Path:      []string{"some", "path"},
							Operator:  "Equal",
							ValueText: ptString("value"),
						},
					},
				},
				expectedError: "validate: failed to get class: SomeClass",
			},
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString(verbosity.OutputVerbose),
					Match: &models.BatchDeleteMatch{
						Class: "Foo",
						Where: &models.WhereFilter{
							Path:      []string{"some"},
							Operator:  "Equal",
							ValueText: ptString("value"),
						},
					},
				},
				expectedError: "validate: invalid where filter: no such prop with name 'some' found in class 'Foo' " +
					"in the schema. Check your schema files for which properties in this class are available",
			},
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString(verbosity.OutputVerbose),
				},
				expectedError: "validate: empty match clause",
			},
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString(verbosity.OutputVerbose),
					Match: &models.BatchDeleteMatch{
						Class: "",
					},
				},
				expectedError: "validate: empty match.class clause",
			},
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString(verbosity.OutputVerbose),
					Match: &models.BatchDeleteMatch{
						Class: "Foo",
					},
				},
				expectedError: "validate: empty match.where clause",
			},
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString(verbosity.OutputVerbose),
					Match: &models.BatchDeleteMatch{
						Class: "Foo",
						Where: &models.WhereFilter{
							Path:      []string{},
							Operator:  "Equal",
							ValueText: ptString("name"),
						},
					},
				},
				expectedError: "validate: failed to parse where filter: invalid where filter: field 'path': must have at least one element",
			},
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString("Simplified Chinese"),
					Match: &models.BatchDeleteMatch{
						Class: "Foo",
						Where: &models.WhereFilter{
							Path:      []string{"name"},
							Operator:  "Equal",
							ValueText: ptString("value"),
						},
					},
				},
				expectedError: "validate: invalid output: \"Simplified Chinese\", possible values are: \"minimal\", \"verbose\"",
			},
		}

		for _, test := range tests {
			_, err := manager.DeleteObjects(ctx, nil, test.input.Match, test.input.DeletionTimeUnixMilli, test.input.DryRun, test.input.Output, nil, "")
			assert.Equal(t, test.expectedError, err.Error())
		}
	})

	t.Run("class-not-found classifies as ErrInvalidUserInput", func(t *testing.T) {
		match := &models.BatchDeleteMatch{
			Class: "SomeMissingClass",
			Where: &models.WhereFilter{Path: []string{"name"}, Operator: "Equal", ValueText: ptString("value")},
		}
		_, err := manager.DeleteObjects(ctx, nil, match, nil, nil, nil, nil, "")
		require.Error(t, err)
		var invalid ErrInvalidUserInput
		require.True(t, errors.As(err, &invalid), "expected ErrInvalidUserInput, got %T: %v", err, err)
	})
}

// Test_BatchDelete_NamespaceResolution proves DeleteObjects routes the
// caller's short class name through namespacing.Resolve before authz and
// schema lookup. The namespaced principal submits "Foo"; the manager must
// authorize and look up "customer1:Foo" and the qualified name must reach
// the vector repo on BatchDeleteParams.ClassName. The global-principal
// case is the regression guard that resolveNS is a no-op without a
// namespace.
func Test_BatchDelete_NamespaceResolution(t *testing.T) {
	makeManager := func(t *testing.T, classes []*models.Class) (*BatchManager, *fakeVectorRepo, *mocks.FakeAuthorizer) {
		t.Helper()
		sch := schema.Schema{Objects: &models.Schema{Classes: classes}}
		vectorRepo := &fakeVectorRepo{}
		cfg := &config.WeaviateConfig{
			Config: config.Config{
				AutoSchema: config.AutoSchema{
					Enabled: runtime.NewDynamicValue(false),
				},
				Namespaces: config.Namespaces{
					Enabled: true,
				},
			},
		}
		schemaManager := &fakeSchemaManager{GetSchemaResponse: sch}
		logger, _ := test.NewNullLogger()
		authorizer := mocks.NewMockAuthorizer()
		manager := NewBatchManager(vectorRepo, getFakeModulesProvider(), schemaManager, cfg, logger, authorizer, nil,
			NewAutoSchemaManager(schemaManager, vectorRepo, cfg, logger, prometheus.NewPedanticRegistry()))
		return manager, vectorRepo, authorizer
	}

	fooClass := func(name string) *models.Class {
		return &models.Class{
			Class: name,
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
			VectorIndexConfig: hnsw.UserConfig{},
			Vectorizer:        config.VectorizerModuleNone,
		}
	}

	// DeleteObjects mutates match.Class in place via resolveNS, so each
	// subtest builds its own match.
	newMatch := func() *models.BatchDeleteMatch {
		return &models.BatchDeleteMatch{
			Class: "Foo",
			Where: &models.WhereFilter{
				Path:      []string{"name"},
				Operator:  "Equal",
				ValueText: ptString("v"),
			},
		}
	}

	t.Run("namespaced principal qualifies the class end to end", func(t *testing.T) {
		manager, vectorRepo, authorizer := makeManager(t, []*models.Class{fooClass("customer1:Foo")})
		vectorRepo.On("BatchDeleteObjects", mock.MatchedBy(func(p BatchDeleteParams) bool {
			return string(p.ClassName) == "customer1:Foo" && p.Filters != nil && string(p.Filters.Root.On.Class) == "customer1:Foo"
		})).Return(BatchDeleteResult{DeletionTime: time.Time{}}, nil)

		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		_, err := manager.DeleteObjects(context.Background(), principal, newMatch(), nil, ptBool(false), ptString(verbosity.OutputMinimal), nil, "")
		require.NoError(t, err)

		// Authz was invoked against the qualified resource.
		require.NotEmpty(t, authorizer.Calls())
		require.Contains(t, authorizer.Calls()[0].Resources[0], "customer1:Foo")

		vectorRepo.AssertExpectations(t)
	})

	t.Run("namespaced principal cannot reach a foreign-namespace class", func(t *testing.T) {
		// Schema only has customer2:Foo. customer1's "Foo" resolves to
		// "customer1:Foo" which is not present, so GetCachedClass fails
		// and the delete is rejected before reaching the repo.
		manager, _, _ := makeManager(t, []*models.Class{fooClass("customer2:Foo")})

		principal := &models.Principal{Username: "u", Namespace: "customer1"}
		_, err := manager.DeleteObjects(context.Background(), principal, newMatch(), nil, ptBool(false), ptString(verbosity.OutputMinimal), nil, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "customer1:Foo")
	})

	t.Run("global principal leaves the class unchanged", func(t *testing.T) {
		// Global principal (no namespace) must leave short class names
		// untouched: no qualification, no rewriting.
		manager, vectorRepo, authorizer := makeManager(t, []*models.Class{fooClass("Foo")})
		vectorRepo.On("BatchDeleteObjects", mock.MatchedBy(func(p BatchDeleteParams) bool {
			return string(p.ClassName) == "Foo" && string(p.Filters.Root.On.Class) == "Foo"
		})).Return(BatchDeleteResult{DeletionTime: time.Time{}}, nil)

		principal := &models.Principal{Username: "admin"}
		_, err := manager.DeleteObjects(context.Background(), principal, newMatch(), nil, ptBool(false), ptString(verbosity.OutputMinimal), nil, "")
		require.NoError(t, err)
		require.NotEmpty(t, authorizer.Calls())
		require.Contains(t, authorizer.Calls()[0].Resources[0], "Foo")
		require.NotContains(t, authorizer.Calls()[0].Resources[0], ":Foo")

		vectorRepo.AssertExpectations(t)
	})
}

// Test_BatchDelete_ValidationErrorsAreUserInput pins that a malformed where
// filter is classified as ErrInvalidUserInput so the REST handler returns a
// 422 — not a bare error, which the handler maps to a 500. Covers both filter
// stages: parse (foreign-namespace inner class) and schema validation (unknown
// property).
func Test_BatchDelete_ValidationErrorsAreUserInput(t *testing.T) {
	nameProp := &models.Property{
		Name:         "name",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationWhitespace,
	}
	mkClass := func(name string) *models.Class {
		return &models.Class{
			Class:             name,
			Properties:        []*models.Property{nameProp},
			VectorIndexConfig: hnsw.UserConfig{},
			Vectorizer:        config.VectorizerModuleNone,
		}
	}
	makeManager := func(t *testing.T, nsEnabled bool, classes []*models.Class) *BatchManager {
		t.Helper()
		sch := schema.Schema{Objects: &models.Schema{Classes: classes}}
		cfg := &config.WeaviateConfig{Config: config.Config{
			AutoSchema: config.AutoSchema{Enabled: runtime.NewDynamicValue(false)},
			Namespaces: config.Namespaces{Enabled: nsEnabled},
		}}
		schemaManager := &fakeSchemaManager{GetSchemaResponse: sch}
		logger, _ := test.NewNullLogger()
		vectorRepo := &fakeVectorRepo{}
		return NewBatchManager(vectorRepo, getFakeModulesProvider(), schemaManager, cfg, logger, mocks.NewMockAuthorizer(), nil,
			NewAutoSchemaManager(schemaManager, vectorRepo, cfg, logger, prometheus.NewPedanticRegistry()))
	}

	t.Run("unknown property fails validation as user input", func(t *testing.T) {
		mgr := makeManager(t, false, []*models.Class{mkClass("Foo")})
		match := &models.BatchDeleteMatch{
			Class: "Foo",
			Where: &models.WhereFilter{Path: []string{"doesNotExist"}, Operator: "Equal", ValueText: ptString("v")},
		}
		_, err := mgr.DeleteObjects(context.Background(), &models.Principal{Username: "admin"},
			match, nil, ptBool(true), ptString(verbosity.OutputMinimal), nil, "")
		require.Error(t, err)
		assert.True(t, errors.As(err, &ErrInvalidUserInput{}),
			"unknown-property filter must be ErrInvalidUserInput, got %T: %v", err, err)
	})

	t.Run("foreign-namespace inner class in ref-path fails parse as user input", func(t *testing.T) {
		mgr := makeManager(t, true, []*models.Class{mkClass("customer1:Foo")})
		match := &models.BatchDeleteMatch{
			Class: "Foo",
			Where: &models.WhereFilter{Path: []string{"ref", "customer2:Other", "name"}, Operator: "Equal", ValueText: ptString("v")},
		}
		_, err := mgr.DeleteObjects(context.Background(), &models.Principal{Username: "u", Namespace: "customer1"},
			match, nil, ptBool(true), ptString(verbosity.OutputMinimal), nil, "")
		require.Error(t, err)
		assert.True(t, errors.As(err, &ErrInvalidUserInput{}),
			"foreign-NS inner class must be ErrInvalidUserInput, got %T: %v", err, err)
		assert.Contains(t, err.Error(), "is not a valid class name")
	})
}

func ptBool(b bool) *bool {
	return &b
}

func ptString(s string) *string {
	return &s
}
