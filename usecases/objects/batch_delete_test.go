//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
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
							Name:     "name",
							DataType: []string{"string"},
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
					Enabled: autoSchema,
				},
			},
		}
		locks := &fakeLocks{}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		logger, _ := test.NewNullLogger()
		authorizer := &fakeAuthorizer{}
		modulesProvider := getFakeModulesProvider()
		manager = NewBatchManager(vectorRepo, modulesProvider, locks,
			schemaManager, config, logger, authorizer, nil)
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
					Output: ptString(OutputVerbose),
					Match: &models.BatchDeleteMatch{
						Class: "SomeClass",
						Where: &models.WhereFilter{
							Path:        []string{"some", "path"},
							Operator:    "Equal",
							ValueString: ptString("value"),
						},
					},
				},
				expectedError: "validate: class: SomeClass doesn't exist",
			},
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString(OutputVerbose),
					Match: &models.BatchDeleteMatch{
						Class: "Foo",
						Where: &models.WhereFilter{
							Path:        []string{"some"},
							Operator:    "Equal",
							ValueString: ptString("value"),
						},
					},
				},
				expectedError: "validate: invalid where filter: no such prop with name 'some' found in class 'Foo' " +
					"in the schema. Check your schema files for which properties in this class are available",
			},
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString(OutputVerbose),
				},
				expectedError: "validate: empty match clause",
			},
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString(OutputVerbose),
					Match: &models.BatchDeleteMatch{
						Class: "",
					},
				},
				expectedError: "validate: empty match.class clause",
			},
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString(OutputVerbose),
					Match: &models.BatchDeleteMatch{
						Class: "Foo",
					},
				},
				expectedError: "validate: empty match.where clause",
			},
			{
				input: &models.BatchDelete{
					DryRun: ptBool(false),
					Output: ptString(OutputVerbose),
					Match: &models.BatchDeleteMatch{
						Class: "Foo",
						Where: &models.WhereFilter{
							Path:        []string{},
							Operator:    "Equal",
							ValueString: ptString("name"),
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
							Path:        []string{"name"},
							Operator:    "Equal",
							ValueString: ptString("value"),
						},
					},
				},
				expectedError: "validate: invalid output: \"Simplified Chinese\", possible values are: \"minimal\", \"verbose\"",
			},
		}

		for _, test := range tests {
			_, err := manager.DeleteObjects(ctx, nil, test.input.Match,
				test.input.DryRun, test.input.Output)
			assert.Equal(t, test.expectedError, err.Error())
		}
	})
}

func ptBool(b bool) *bool {
	return &b
}

func ptString(s string) *string {
	return &s
}
