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

package schema

import (
	"context"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/fakes"
)

// newTestHandlerWithNamespaces returns a Handler that has the cluster-wide
// NAMESPACES_ENABLED flag flipped on. It is otherwise identical to the helper
// at helpers_test.go:newTestHandler so existing fakes keep working.
func newTestHandlerWithNamespaces(t *testing.T, enabled bool) (*Handler, *fakeSchemaManager) {
	t.Helper()
	schemaManager := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	vectorizerValidator := &fakeVectorizerValidator{
		valid: []string{"text2vec-contextionary", "model1", "model2"},
	}
	cfg := config.Config{
		DefaultVectorizerModule:     config.VectorizerModuleNone,
		DefaultVectorDistanceMetric: "cosine",
	}
	cfg.Namespaces.Enabled = enabled
	fakeClusterState := fakes.NewFakeClusterState()
	fakeValidator := &fakeValidator{}
	schemaParser := NewParser(fakeClusterState, dummyParseVectorConfig, fakeValidator, fakeModulesProvider{}, nil, nil)
	handler, err := NewHandler(
		schemaManager, schemaManager, fakeValidator, logger, mocks.NewMockAuthorizer(),
		&cfg.SchemaHandlerConfig, cfg, dummyParseVectorConfig, vectorizerValidator, dummyValidateInvertedConfig,
		&fakeModuleConfig{}, fakeClusterState, nil, *schemaParser, nil)
	require.NoError(t, err)
	handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
	return &handler, schemaManager
}

func namespacedPrincipal(ns string) *models.Principal {
	return &models.Principal{Username: "u1", Namespace: ns}
}

func globalPrincipal() *models.Principal {
	return &models.Principal{Username: "admin", IsGlobalOperator: true}
}

func TestAddClass(t *testing.T) {
	t.Parallel()
	casts := []struct {
		name       string
		enabled    bool
		principal  *models.Principal
		class      string
		wantClass  string
		wantErrMsg string
	}{
		{
			name:      "namespaced principal qualifies and persists prefixed class",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			class:     "Movies",
			wantClass: "customer1:Movies",
		},
		{
			name:       "global principal rejected on namespaces enabled",
			enabled:    true,
			principal:  globalPrincipal(),
			class:      "Movies",
			wantErrMsg: "must be namespaced",
		},
		{
			name:       "nil principal rejected on namespaces enabled",
			enabled:    true,
			principal:  nil,
			class:      "Movies",
			wantErrMsg: "must be namespaced",
		},
		{
			name:      "global principal allowed on namespaces disabled",
			enabled:   false,
			principal: globalPrincipal(),
			class:     "Movies",
			wantClass: "Movies",
		},
		{
			name:       "namespaced principal short name containing colon rejected",
			enabled:    true,
			principal:  namespacedPrincipal("customer1"),
			class:      "Customer2:Movies",
			wantErrMsg: "is not a valid class name",
		},
		{
			name:       "namespaced principal over length rejected",
			enabled:    true,
			principal:  namespacedPrincipal("customer"),
			class:      "C" + strings.Repeat("x", 254),
			wantErrMsg: "namespaced names must be at most",
		},
	}

	for _, tt := range casts {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)

			class := &models.Class{
				Class:             tt.class,
				Vectorizer:        "model1",
				VectorIndexConfig: map[string]interface{}{},
				ReplicationConfig: &models.ReplicationConfig{Factor: 1},
			}

			if tt.wantClass != "" {
				sm.On("AddClass", mock.MatchedBy(func(c *models.Class) bool {
					return c.Class == tt.wantClass
				}), mock.Anything).Return(nil)
				sm.On("QueryCollectionsCount").Return(0, nil)
			}

			got, _, err := handler.AddClass(context.Background(), tt.principal, class)
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantClass, got.Class)
		})
	}
}

func TestAddAlias(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		enabled    bool
		principal  *models.Principal
		alias      string
		class      string
		wantAlias  string
		wantClass  string
		wantErrMsg string
		mockTarget string
	}{
		{
			name:       "namespaced principal qualifies both",
			enabled:    true,
			principal:  namespacedPrincipal("customer1"),
			alias:      "Films",
			class:      "Movies",
			wantAlias:  "customer1:Films",
			wantClass:  "customer1:Movies",
			mockTarget: "customer1:Movies",
		},
		{
			name:       "global principal rejected on namespaces enabled",
			enabled:    true,
			principal:  globalPrincipal(),
			alias:      "Films",
			class:      "Movies",
			wantErrMsg: "must be namespaced",
		},
		{
			name:       "namespaced principal colon in target rejected",
			enabled:    true,
			principal:  namespacedPrincipal("customer1"),
			alias:      "Films",
			class:      "Customer2:Movies",
			wantErrMsg: "is not a valid class name",
		},
		{
			name:       "namespaced principal colon in alias rejected",
			enabled:    true,
			principal:  namespacedPrincipal("customer1"),
			alias:      "Customer2:Films",
			class:      "Movies",
			wantErrMsg: "is not a valid alias name",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)

			if tt.mockTarget != "" {
				target := &models.Class{Class: tt.mockTarget}
				sm.On("ReadOnlyClass", tt.mockTarget).Return(target)
				sm.On("CreateAlias", mock.Anything, tt.wantAlias, mock.MatchedBy(func(c *models.Class) bool {
					return c != nil && c.Class == tt.wantClass
				})).Return(nil)
			}

			alias := &models.Alias{Alias: tt.alias, Class: tt.class}
			got, _, err := handler.AddAlias(context.Background(), tt.principal, alias)
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantAlias, got.Alias)
			require.Equal(t, tt.wantClass, got.Class)
		})
	}
}
