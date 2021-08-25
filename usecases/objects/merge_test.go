//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_MergeObject(t *testing.T) {
	logger, _ := test.NewNullLogger()

	type testCase struct {
		name                 string
		previous             *models.Object // nil implies return false on Exist()
		updated              *models.Object
		expectedErr          error
		expectedOutput       *MergeDocument
		id                   strfmt.UUID
		vectorizerCalledWith *models.Object
	}

	tests := []testCase{
		testCase{
			id:       "dd59815b-142b-4c54-9b12-482434bd54ca",
			name:     "didn't previously exist",
			previous: nil,
			updated: &models.Object{
				Class: "ZooAction",
				Properties: map[string]interface{}{
					"foo": "bar",
				},
			},
			expectedErr: NewErrInvalidUserInput("invalid merge: object with id '%s' does not exist",
				"dd59815b-142b-4c54-9b12-482434bd54ca"),
		},
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "adding a new property",
			previous: &models.Object{
				Class:      "ZooAction",
				Properties: map[string]interface{}{},
			},
			updated: &models.Object{
				Class: "ZooAction",
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedErr: nil,
			vectorizerCalledWith: &models.Object{
				Class: "ZooAction",
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: 12345,
				Class:      "ZooAction",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				Vector:     []float32{1, 2, 3},
				PrimitiveSchema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
		},
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "without properties",
			previous: &models.Object{
				Class: "ZooAction",
			},
			updated: &models.Object{
				Class: "ZooAction",
			},
			expectedErr: nil,
			vectorizerCalledWith: &models.Object{
				Class:      "ZooAction",
				Properties: map[string]interface{}{},
			},
			expectedOutput: &MergeDocument{
				UpdateTime:      12345,
				Class:           "ZooAction",
				ID:              "dd59815b-142b-4c54-9b12-482434bd54ca",
				Vector:          []float32{1, 2, 3},
				PrimitiveSchema: map[string]interface{}{},
			},
		},
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "adding many primitive properties of different types",
			previous: &models.Object{
				Class:      "ZooAction",
				Properties: map[string]interface{}{},
			},
			updated: &models.Object{
				Class: "ZooAction",
				Properties: map[string]interface{}{
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
			vectorizerCalledWith: &models.Object{
				Class: "ZooAction",
				Properties: map[string]interface{}{
					"name":      "My little pony zoo with extra sparkles",
					"area":      3.222,
					"employees": int64(70),
					"located": &models.GeoCoordinates{
						Latitude:  ptFloat32(30.2),
						Longitude: ptFloat32(60.2),
					},
					"foundedIn": timeMustParse(time.RFC3339, "2002-10-02T15:00:00Z"),
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: 12345,
				Class:      "ZooAction",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				Vector:     []float32{1, 2, 3},
				PrimitiveSchema: map[string]interface{}{
					"name":      "My little pony zoo with extra sparkles",
					"area":      3.222,
					"employees": int64(70),
					"located": &models.GeoCoordinates{
						Latitude:  ptFloat32(30.2),
						Longitude: ptFloat32(60.2),
					},
					"foundedIn": timeMustParse(time.RFC3339, "2002-10-02T15:00:00Z"),
				},
			},
		},
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "adding a primitive and a ref property",
			previous: &models.Object{
				Class:      "ZooAction",
				Properties: map[string]interface{}{},
			},
			updated: &models.Object{
				Class: "ZooAction",
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
					"hasAnimals": []interface{}{
						map[string]interface{}{
							"beacon": "weaviate://localhost/a8ffc82c-9845-4014-876c-11369353c33c",
						},
					},
				},
			},
			expectedErr: nil,
			vectorizerCalledWith: &models.Object{
				Class: "ZooAction",
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: 12345,
				Class:      "ZooAction",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				PrimitiveSchema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
				Vector: []float32{1, 2, 3},
				References: BatchReferences{
					BatchReference{
						From: crossrefMustParseSource("weaviate://localhost/ZooAction/dd59815b-142b-4c54-9b12-482434bd54ca/hasAnimals"),
						To:   crossrefMustParse("weaviate://localhost/a8ffc82c-9845-4014-876c-11369353c33c"),
					},
				},
			},
		},
		testCase{
			name: "udpating the vector of a non-vectorized class",
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			previous: &models.Object{
				Class: "NotVectorized",
				Properties: map[string]interface{}{
					"description": "this description was set initially",
				},
				Vector: []float32{0.7, 0.3},
			},
			updated: &models.Object{
				Class:  "NotVectorized",
				Vector: []float32{0.66, 0.22},
			},
			expectedErr:          nil,
			vectorizerCalledWith: nil,
			expectedOutput: &MergeDocument{
				UpdateTime:      12345,
				Class:           "NotVectorized",
				ID:              "dd59815b-142b-4c54-9b12-482434bd54ca",
				Vector:          []float32{0.66, 0.22},
				PrimitiveSchema: map[string]interface{}{},
			},
		},

		testCase{
			name: "not udpating the vector of a non-vectorized class",
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			previous: &models.Object{
				Class: "NotVectorized",
				Properties: map[string]interface{}{
					"description": "this description was set initially",
				},
				Vector: []float32{0.7, 0.3},
			},
			updated: &models.Object{
				Class: "NotVectorized",
				Properties: map[string]interface{}{
					"description": "this description was updated",
				},
			},
			expectedErr:          nil,
			vectorizerCalledWith: nil,
			expectedOutput: &MergeDocument{
				UpdateTime: 12345,
				Class:      "NotVectorized",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				Vector:     []float32{0.7, 0.3},
				PrimitiveSchema: map[string]interface{}{
					"description": "this description was updated",
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
			cfg := &config.WeaviateConfig{}
			authorizer := &fakeAuthorizer{}
			vectorizer := &fakeVectorizer{}
			vecProvider := &fakeVectorizerProvider{vectorizer}
			manager := NewManager(locks, schemaManager,
				cfg, logger, authorizer, vecProvider, vectorRepo, getFakeModulesProvider())
			manager.timeSource = fakeTimeSource{}

			if test.previous != nil {
				vectorRepo.On("ObjectByID", test.id, traverser.SelectProperties(nil), additional.Properties{}).
					Return(&search.Result{
						Schema:    test.previous.Properties,
						ClassName: test.previous.Class,
						Vector:    test.previous.Vector,
					}, nil)
			} else {
				vectorRepo.On("ObjectByID", test.id, traverser.SelectProperties(nil), additional.Properties{}).
					Return((*search.Result)(nil), nil)
			}

			if test.expectedOutput != nil {
				vectorRepo.On("Merge", *test.expectedOutput).Return(nil)
			}

			if test.vectorizerCalledWith != nil {
				vectorizer.On("UpdateObject", test.vectorizerCalledWith).Return([]float32{1, 2, 3}, nil)
			}

			// only for validation of cross-refs. "Maybe" indicates that if this call
			// doesn't happen the test won't fail
			vectorRepo.On("Exists", mock.Anything).Maybe().Return(true, nil)

			err := manager.MergeObject(context.Background(), nil, test.id, test.updated)
			assert.Equal(t, test.expectedErr, err)

			vectorRepo.AssertExpectations(t)
			vectorizer.AssertExpectations(t)
		})
	}
}

func Test_MergeThing(t *testing.T) {
	logger, _ := test.NewNullLogger()

	type testCase struct {
		name                 string
		previous             *models.Object // nil implies return false on Exist()
		updated              *models.Object
		expectedErr          error
		expectedOutput       *MergeDocument
		id                   strfmt.UUID
		vectorizerCalledWith *models.Object
	}

	tests := []testCase{
		testCase{
			id:       "dd59815b-142b-4c54-9b12-482434bd54ca",
			name:     "didn't previously exist",
			previous: nil,
			updated: &models.Object{
				Class: "Zoo",
				Properties: map[string]interface{}{
					"foo": "bar",
				},
			},
			expectedErr: NewErrInvalidUserInput("invalid merge: object with id '%s' does not exist",
				"dd59815b-142b-4c54-9b12-482434bd54ca"),
		},
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "adding a new property",
			previous: &models.Object{
				Class:      "Zoo",
				Properties: map[string]interface{}{},
			},
			updated: &models.Object{
				Class: "Zoo",
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedErr: nil,
			vectorizerCalledWith: &models.Object{
				Class: "Zoo",
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: 12345,
				Class:      "Zoo",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				Vector:     []float32{1, 2, 3},
				PrimitiveSchema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
		},
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "adding many primitive properties of different types",
			previous: &models.Object{
				Class:      "Zoo",
				Properties: map[string]interface{}{},
			},
			updated: &models.Object{
				Class: "Zoo",
				Properties: map[string]interface{}{
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
			vectorizerCalledWith: &models.Object{
				Class: "Zoo",
				Properties: map[string]interface{}{
					"name":      "My little pony zoo with extra sparkles",
					"area":      3.222,
					"employees": int64(70),
					"located": &models.GeoCoordinates{
						Latitude:  ptFloat32(30.2),
						Longitude: ptFloat32(60.2),
					},
					"foundedIn": timeMustParse(time.RFC3339, "2002-10-02T15:00:00Z"),
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: 12345,
				Class:      "Zoo",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				Vector:     []float32{1, 2, 3},
				PrimitiveSchema: map[string]interface{}{
					"name":      "My little pony zoo with extra sparkles",
					"area":      3.222,
					"employees": int64(70),
					"located": &models.GeoCoordinates{
						Latitude:  ptFloat32(30.2),
						Longitude: ptFloat32(60.2),
					},
					"foundedIn": timeMustParse(time.RFC3339, "2002-10-02T15:00:00Z"),
				},
			},
		},
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "adding a primitive and a ref property",
			previous: &models.Object{
				Class:      "Zoo",
				Properties: map[string]interface{}{},
			},
			updated: &models.Object{
				Class: "Zoo",
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
					"hasAnimals": []interface{}{
						map[string]interface{}{
							"beacon": "weaviate://localhost/a8ffc82c-9845-4014-876c-11369353c33c",
						},
					},
				},
			},
			expectedErr: nil,
			vectorizerCalledWith: &models.Object{
				Class: "Zoo",
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: 12345,
				Class:      "Zoo",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				PrimitiveSchema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
				Vector: []float32{1, 2, 3},
				References: BatchReferences{
					BatchReference{
						From: crossrefMustParseSource("weaviate://localhost/Zoo/dd59815b-142b-4c54-9b12-482434bd54ca/hasAnimals"),
						To:   crossrefMustParse("weaviate://localhost/a8ffc82c-9845-4014-876c-11369353c33c"),
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
			cfg := &config.WeaviateConfig{}
			authorizer := &fakeAuthorizer{}
			vectorizer := &fakeVectorizer{}
			vecProvider := &fakeVectorizerProvider{vectorizer}
			manager := NewManager(locks, schemaManager,
				cfg, logger, authorizer, vecProvider, vectorRepo, getFakeModulesProvider())
			manager.timeSource = fakeTimeSource{}

			if test.previous != nil {
				vectorRepo.On("ObjectByID", test.id, traverser.SelectProperties(nil), additional.Properties{}).
					Return(&search.Result{
						Schema:    test.previous.Properties,
						ClassName: test.previous.Class,
					}, nil)
			} else {
				vectorRepo.On("ObjectByID", test.id, traverser.SelectProperties(nil), additional.Properties{}).
					Return((*search.Result)(nil), nil)
			}

			if test.expectedOutput != nil {
				vectorRepo.On("Merge", *test.expectedOutput).Return(nil)
				vectorizer.On("UpdateObject", test.vectorizerCalledWith).Return([]float32{1, 2, 3}, nil)
			}

			// only for validation of cross-refs. Maybe indicates that if this call
			// doesn't happen the test won't fail
			vectorRepo.On("Exists", mock.Anything).Maybe().Return(true, nil)

			err := manager.MergeObject(context.Background(), nil, test.id, test.updated)
			assert.Equal(t, test.expectedErr, err)

			vectorRepo.AssertExpectations(t)
			vectorizer.AssertExpectations(t)
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

type fakeTimeSource struct{}

func (f fakeTimeSource) Now() int64 {
	return 12345
}

func ptFloat32(in float32) *float32 {
	return &in
}
