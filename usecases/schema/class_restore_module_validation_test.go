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
	"encoding/json"
	"errors"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
)

// TestRestoreClassKeepsLegacyNoneVectorizerClasses pins that module validation
// of a legacy class with vectorizer "none" governs creation only. Such a class
// had no module validation before generative and reranker settings were
// checked on it, so a backup may hold one whose settings are refused today, or
// whose module checks them against a remote API that is down during the
// restore. It is persisted state and must restore; its baseURL is still
// validated when a request is sent.
func TestRestoreClassKeepsLegacyNoneVectorizerClasses(t *testing.T) {
	const refused = "module 'refusing-reranker': settings refused"
	rerankerConfig := map[string]any{"refusing-reranker": map[string]any{"baseURL": "http://stored-before-validation.example.com"}}

	tests := []struct {
		name  string
		class func() *models.Class
		// wantRestoreErr is empty for a class that must restore.
		wantRestoreErr string
	}{
		{
			name: "vectorizer none",
			class: func() *models.Class {
				return &models.Class{Class: "Doc", Vectorizer: config.VectorizerModuleNone, ModuleConfig: rerankerConfig}
			},
		},
		{
			// unchanged: these classes were validated when they were created
			name: "named vectors",
			class: func() *models.Class {
				return &models.Class{
					Class:        "Doc",
					ModuleConfig: rerankerConfig,
					VectorConfig: map[string]models.VectorConfig{
						"main": {Vectorizer: map[string]any{"none": map[string]any{}}, VectorIndexType: "hnsw"},
					},
				}
			},
			wantRestoreErr: refused,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("restore", func(t *testing.T) {
				handler, schemaManager := newTestHandlerWithRefusingReranker(t)
				schemaManager.On("ReadOnlyClass", mock.Anything).Return((*models.Class)(nil)).Maybe()
				restored := false
				schemaManager.On("RestoreClass", mock.Anything, mock.Anything).
					Run(func(mock.Arguments) { restored = true }).
					Return(nil).Maybe()
				schemaBytes, err := json.Marshal(tt.class())
				require.NoError(t, err)
				descriptor := backup.ClassDescriptor{Name: "Doc", Schema: schemaBytes}

				err = handler.RestoreClass(context.Background(), &descriptor, map[string]string{}, false, false)

				if tt.wantRestoreErr != "" {
					require.EqualError(t, err, tt.wantRestoreErr)
					require.False(t, restored)
					return
				}
				require.NoError(t, err)
				require.True(t, restored)
			})

			t.Run("create is refused", func(t *testing.T) {
				handler, _ := newTestHandlerWithRefusingReranker(t)
				class := tt.class()
				require.NoError(t, handler.setClassDefaults(class, handler.config.Replication))

				err := handler.validateCanAddClass(context.Background(), class, class.Class,
					func(string) (*models.Class, error) { return nil, nil }, false)

				require.EqualError(t, err, refused)
			})
		})
	}
}

func newTestHandlerWithRefusingReranker(t *testing.T) (*Handler, *fakeSchemaManager) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	provider := modules.NewProvider(logger, config.Config{})
	provider.Register(refusingReranker{})
	return newTestHandlerWithModules(t, &fakeDB{}, fakeModulesProvider{}, provider, &fakeVectorizerValidator{})
}

// refusingReranker is a reranker module that refuses every class.
type refusingReranker struct{}

func (refusingReranker) Name() string { return "refusing-reranker" }

func (refusingReranker) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextReranker
}

func (refusingReranker) Init(context.Context, moduletools.ModuleInitParams) error { return nil }

func (refusingReranker) ClassConfigDefaults() map[string]any { return map[string]any{} }

func (refusingReranker) PropertyConfigDefaults(*schema.DataType) map[string]any {
	return map[string]any{}
}

func (refusingReranker) ValidateClass(context.Context, *models.Class, moduletools.ClassConfig) error {
	return errors.New("settings refused")
}
