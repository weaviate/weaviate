package kinds

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
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
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "adding many primitive properties of different types",
			previous: &models.Thing{
				Class:  "Zoo",
				Schema: map[string]interface{}{},
			},
			updated: &models.Thing{
				Class: "Zoo",
				Schema: map[string]interface{}{
					"name":      "My little pony zoo with extra sparkles",
					"area":      3.222,
					"employees": json.Number("70"),
					"located": map[string]interface{}{
						"latitude":  30.2,
						"longitude": 60.2,
					},
					"foundedIn": "2002-10-02T15:00:00Z",
				},
			},
			expectedErr: nil,
			expectedOutput: &MergeDocument{
				Kind:  kind.Thing,
				Class: "Zoo",
				ID:    "dd59815b-142b-4c54-9b12-482434bd54ca",
				PrimitiveSchema: map[string]interface{}{
					"name":      "My little pony zoo with extra sparkles",
					"area":      3.222,
					"employees": int64(70),
					"located": &models.GeoCoordinates{
						Latitude:  30.2,
						Longitude: 60.2,
					},
					"foundedIn": timeMustParse(time.RFC3339, "2002-10-02T15:00:00Z"),
				},
			},
		},
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "adding a primitive and a ref property",
			previous: &models.Thing{
				Class:  "Zoo",
				Schema: map[string]interface{}{},
			},
			updated: &models.Thing{
				Class: "Zoo",
				Schema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
					"hasAnimals": []interface{}{
						map[string]interface{}{
							"beacon": "weaviate://localhost/things/a8ffc82c-9845-4014-876c-11369353c33c",
						},
					},
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
				References: BatchReferences{
					BatchReference{
						From: crossrefMustParseSource("weaviate://localhost/things/Zoo/dd59815b-142b-4c54-9b12-482434bd54ca/hasAnimals"),
						To:   crossrefMustParse("weaviate://localhost/things/a8ffc82c-9845-4014-876c-11369353c33c"),
					},
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

			if test.expectedOutput != nil {
				vectorRepo.On("Merge", *test.expectedOutput).Return(nil)

			}

			err := manager.MergeThing(context.Background(), nil, test.id, test.updated)
			assert.Equal(t, test.expectedErr, err)

			vectorRepo.AssertExpectations(t)

		})

	}

}

func timeMustParse(layout, value string) time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return t
}

func crossrefMustParse(in string) *crossref.Ref {
	ref, err := crossref.Parse(in)
	if err != nil {
		panic(err)
	}

	return ref
}

func crossrefMustParseSource(in string) *crossref.RefSource {
	ref, err := crossref.ParseSource(in)
	if err != nil {
		panic(err)
	}

	return ref
}
