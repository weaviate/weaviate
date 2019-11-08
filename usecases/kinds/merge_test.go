package kinds

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_MergeThing(t *testing.T) {
	logger, _ := test.NewNullLogger()

	type testCase struct {
		name           string
		previous       *models.Thing // nil implies return false on Exist()
		updated        *models.Thing
		expectedErr    error
		expectedOutput *MergeDocument
		id             strfmt.UUID
	}

	tests := []testCase{
		testCase{
			id:       "dd59815b-142b-4c54-9b12-482434bd54ca",
			name:     "didn't previously exist",
			previous: nil,
			updated: &models.Thing{
				Class: "Zoo",
				Schema: map[string]interface{}{
					"foo": "bar",
				},
			},
			expectedErr: fmt.Errorf("invalid merge: thing object with id '%s' does not exist",
				"dd59815b-142b-4c54-9b12-482434bd54ca"),
		},
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "adding a new property",
			previous: &models.Thing{
				Class:  "Zoo",
				Schema: map[string]interface{}{},
			},
			updated: &models.Thing{
				Class: "Zoo",
				Schema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedErr: nil,
			expectedOutput: &MergeDocument{
				Kind:  kind.Thing,
				Class: "Zoo",
				ID:    "dd59815b-142b-4c54-9b12-482434bd54ca",
				PrimitiveSchema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vectorRepo := &fakeVectorRepo{}
			schemaManager := &fakeSchemaManager{
				GetSchemaResponse: zooAnimalSchemaForTest(),
			}
			locks := &fakeLocks{}
			network := &fakeNetwork{}
			cfg := &config.WeaviateConfig{}
			authorizer := &fakeAuthorizer{}
			vectorizer := &fakeVectorizer{}
			manager := NewManager(locks, schemaManager, network,
				cfg, logger, authorizer, vectorizer, vectorRepo)

			vectorRepo.On("Exists", mock.Anything).Return(test.previous != nil, nil)

			if test.previous != nil {
				// vectorRepo.On("ThingByID", mock.Anything, mock.Anything, mock.Anything).Return(&search.Result{
				// 	ClassName: test.previous.Class,
				// 	Schema:    test.previous.Schema,
				// }, nil)
			}

			if test.expectedOutput != nil {
				vectorRepo.On("Merge", *test.expectedOutput).Return(nil)

			}

			err := manager.MergeThing(context.Background(), nil, test.id, test.updated)
			assert.Equal(t, test.expectedErr, err)

			vectorRepo.AssertExpectations(t)

		})

	}

}
