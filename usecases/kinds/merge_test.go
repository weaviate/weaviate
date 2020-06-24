//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_MergeAction(t *testing.T) {
	logger, _ := test.NewNullLogger()

	type testCase struct {
		name                 string
		previous             *models.Action // nil implies return false on Exist()
		updated              *models.Action
		expectedErr          error
		expectedOutput       *MergeDocument
		id                   strfmt.UUID
		vectorizerCalledWith *models.Action
	}

	tests := []testCase{
		testCase{
			id:       "dd59815b-142b-4c54-9b12-482434bd54ca",
			name:     "didn't previously exist",
			previous: nil,
			updated: &models.Action{
				Class: "ZooAction",
				Schema: map[string]interface{}{
					"foo": "bar",
				},
			},
			expectedErr: NewErrInvalidUserInput("invalid merge: action object with id '%s' does not exist",
				"dd59815b-142b-4c54-9b12-482434bd54ca"),
		},
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "adding a new property",
			previous: &models.Action{
				Class:  "ZooAction",
				Schema: map[string]interface{}{},
			},
			updated: &models.Action{
				Class: "ZooAction",
				Schema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedErr: nil,
			vectorizerCalledWith: &models.Action{
				Class: "ZooAction",
				Schema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: 12345,
				Kind:       kind.Action,
				Class:      "ZooAction",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				Vector:     []float32{1, 2, 3},
				UnderscoreProperties: models.UnderscoreProperties{
					Interpretation: &models.Interpretation{
						Source: []*models.InterpretationSource{},
					},
				},
				PrimitiveSchema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
		},
		testCase{
			id:   "dd59815b-142b-4c54-9b12-482434bd54ca",
			name: "adding many primitive properties of different types",
			previous: &models.Action{
				Class:  "ZooAction",
				Schema: map[string]interface{}{},
			},
			updated: &models.Action{
				Class: "ZooAction",
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
			vectorizerCalledWith: &models.Action{
				Class: "ZooAction",
				Schema: map[string]interface{}{
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
				Kind:       kind.Action,
				Class:      "ZooAction",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				Vector:     []float32{1, 2, 3},
				UnderscoreProperties: models.UnderscoreProperties{
					Interpretation: &models.Interpretation{
						Source: []*models.InterpretationSource{},
					},
				},
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
			previous: &models.Action{
				Class:  "ZooAction",
				Schema: map[string]interface{}{},
			},
			updated: &models.Action{
				Class: "ZooAction",
				Schema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
					"hasAnimals": []interface{}{
						map[string]interface{}{
							"beacon": "weaviate://localhost/actions/a8ffc82c-9845-4014-876c-11369353c33c",
						},
					},
				},
			},
			expectedErr: nil,
			vectorizerCalledWith: &models.Action{
				Class: "ZooAction",
				Schema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: 12345,
				Kind:       kind.Action,
				Class:      "ZooAction",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				PrimitiveSchema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
				Vector: []float32{1, 2, 3},
				UnderscoreProperties: models.UnderscoreProperties{
					Interpretation: &models.Interpretation{
						Source: []*models.InterpretationSource{},
					},
				},
				References: BatchReferences{
					BatchReference{
						From: crossrefMustParseSource("weaviate://localhost/actions/ZooAction/dd59815b-142b-4c54-9b12-482434bd54ca/hasAnimals"),
						To:   crossrefMustParse("weaviate://localhost/actions/a8ffc82c-9845-4014-876c-11369353c33c"),
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
			extender := &fakeExtender{}
			manager := NewManager(locks, schemaManager, network,
				cfg, logger, authorizer, vectorizer, vectorRepo, extender)
			manager.timeSource = fakeTimeSource{}

			if test.previous != nil {
				vectorRepo.On("ActionByID", test.id, traverser.SelectProperties(nil), traverser.UnderscoreProperties{}).
					Return(&search.Result{
						Schema:    test.previous.Schema,
						ClassName: test.previous.Class,
						Kind:      kind.Action,
					}, nil)
			} else {
				vectorRepo.On("ActionByID", test.id, traverser.SelectProperties(nil), traverser.UnderscoreProperties{}).
					Return((*search.Result)(nil), nil)
			}

			if test.expectedOutput != nil {
				vectorRepo.On("Merge", *test.expectedOutput).Return(nil)
				vectorizer.On("Action", test.vectorizerCalledWith).Return([]float32{1, 2, 3}, nil)
			}

			// only for validation of cross-refs. Maybe indicates that if this call
			// doesn't happen the test won't fail
			vectorRepo.On("Exists", mock.Anything).Maybe().Return(true, nil)

			err := manager.MergeAction(context.Background(), nil, test.id, test.updated)
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
		previous             *models.Thing // nil implies return false on Exist()
		updated              *models.Thing
		expectedErr          error
		expectedOutput       *MergeDocument
		id                   strfmt.UUID
		vectorizerCalledWith *models.Thing
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
			expectedErr: NewErrInvalidUserInput("invalid merge: thing object with id '%s' does not exist",
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
			vectorizerCalledWith: &models.Thing{
				Class: "Zoo",
				Schema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: 12345,
				Kind:       kind.Thing,
				Class:      "Zoo",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				Vector:     []float32{1, 2, 3},
				UnderscoreProperties: models.UnderscoreProperties{
					Interpretation: &models.Interpretation{
						Source: []*models.InterpretationSource{},
					},
				},
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
			vectorizerCalledWith: &models.Thing{
				Class: "Zoo",
				Schema: map[string]interface{}{
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
				Kind:       kind.Thing,
				Class:      "Zoo",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				Vector:     []float32{1, 2, 3},
				UnderscoreProperties: models.UnderscoreProperties{
					Interpretation: &models.Interpretation{
						Source: []*models.InterpretationSource{},
					},
				},
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
			vectorizerCalledWith: &models.Thing{
				Class: "Zoo",
				Schema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: 12345,
				Kind:       kind.Thing,
				Class:      "Zoo",
				ID:         "dd59815b-142b-4c54-9b12-482434bd54ca",
				PrimitiveSchema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
				Vector: []float32{1, 2, 3},
				UnderscoreProperties: models.UnderscoreProperties{
					Interpretation: &models.Interpretation{
						Source: []*models.InterpretationSource{},
					},
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
			extender := &fakeExtender{}
			manager := NewManager(locks, schemaManager, network,
				cfg, logger, authorizer, vectorizer, vectorRepo, extender)
			manager.timeSource = fakeTimeSource{}

			if test.previous != nil {
				vectorRepo.On("ThingByID", test.id, traverser.SelectProperties(nil), traverser.UnderscoreProperties{}).
					Return(&search.Result{
						Schema:    test.previous.Schema,
						ClassName: test.previous.Class,
						Kind:      kind.Thing,
					}, nil)
			} else {
				vectorRepo.On("ThingByID", test.id, traverser.SelectProperties(nil), traverser.UnderscoreProperties{}).
					Return((*search.Result)(nil), nil)
			}

			if test.expectedOutput != nil {
				vectorRepo.On("Merge", *test.expectedOutput).Return(nil)
				vectorizer.On("Thing", test.vectorizerCalledWith).Return([]float32{1, 2, 3}, nil)
			}

			// only for validation of cross-refs. Maybe indicates that if this call
			// doesn't happen the test won't fail
			vectorRepo.On("Exists", mock.Anything).Maybe().Return(true, nil)

			err := manager.MergeThing(context.Background(), nil, test.id, test.updated)
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
