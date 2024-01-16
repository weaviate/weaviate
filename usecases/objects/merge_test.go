//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
)

type stage int

const (
	stageInit = iota
	// stageInputValidation
	stageAuthorization
	stageUpdateValidation
	stageObjectExists
	// stageVectorization
	// stageMerge
	stageCount
)

func Test_MergeObject(t *testing.T) {
	t.Parallel()
	var (
		uuid           = strfmt.UUID("dd59815b-142b-4c54-9b12-482434bd54ca")
		cls            = "ZooAction"
		lastTime int64 = 12345
		errAny         = errors.New("any error")
	)

	tests := []struct {
		name string
		// inputs
		previous             *models.Object
		updated              *models.Object
		vectorizerCalledWith *models.Object

		// outputs
		expectedOutput *MergeDocument
		wantCode       int

		// control return errors
		errMerge        error
		errUpdateObject error
		errGetObject    error
		errExists       error
		stage
	}{
		{
			name:     "empty class",
			previous: nil,
			updated: &models.Object{
				ID: uuid,
			},
			wantCode: StatusBadRequest,
			stage:    stageInit,
		},
		{
			name:     "empty uuid",
			previous: nil,
			updated: &models.Object{
				Class: cls,
			},
			wantCode: StatusBadRequest,
			stage:    stageInit,
		},
		{
			name:     "empty updates",
			previous: nil,
			wantCode: StatusBadRequest,
			stage:    stageInit,
		},
		{
			name:     "object not found",
			previous: nil,
			updated: &models.Object{
				Class: cls,
				ID:    uuid,
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			wantCode: StatusNotFound,
			stage:    stageObjectExists,
		},
		{
			name:     "object failure",
			previous: nil,
			updated: &models.Object{
				Class: cls,
				ID:    uuid,
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			wantCode:     StatusInternalServerError,
			errGetObject: errAny,
			stage:        stageObjectExists,
		},
		{
			name:     "cross-ref not found",
			previous: nil,
			updated: &models.Object{
				Class: cls,
				ID:    uuid,
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
					"hasAnimals": []interface{}{
						map[string]interface{}{
							"beacon": "weaviate://localhost/a8ffc82c-9845-4014-876c-11369353c33c",
						},
					},
				},
			},
			wantCode:  StatusNotFound,
			errExists: errAny,
			stage:     stageAuthorization,
		},
		{
			name: "merge failure",
			previous: &models.Object{
				Class:      cls,
				Properties: map[string]interface{}{},
			},
			updated: &models.Object{
				Class: cls,
				ID:    uuid,
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			vectorizerCalledWith: &models.Object{
				Class: cls,
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: lastTime,
				Class:      cls,
				ID:         uuid,
				Vector:     []float32{1, 2, 3},
				PrimitiveSchema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			errMerge: errAny,
			wantCode: StatusInternalServerError,
			stage:    stageCount,
		},
		{
			name: "vectorization failure",
			previous: &models.Object{
				Class:      cls,
				Properties: map[string]interface{}{},
			},
			updated: &models.Object{
				Class: cls,
				ID:    uuid,
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			vectorizerCalledWith: &models.Object{
				Class: cls,
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			errUpdateObject: errAny,
			wantCode:        StatusInternalServerError,
			stage:           stageCount,
		},
		{
			name: "add property",
			previous: &models.Object{
				Class:      cls,
				Properties: map[string]interface{}{},
			},
			updated: &models.Object{
				Class: cls,
				ID:    uuid,
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			vectorizerCalledWith: &models.Object{
				Class: cls,
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: lastTime,
				Class:      cls,
				ID:         uuid,
				Vector:     []float32{1, 2, 3},
				PrimitiveSchema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			stage: stageCount,
		},
		{
			name: "update property",
			previous: &models.Object{
				Class:      cls,
				Properties: map[string]interface{}{"name": "this name"},
				Vector:     []float32{0.7, 0.3},
			},
			updated: &models.Object{
				Class: cls,
				ID:    uuid,
				Properties: map[string]interface{}{
					"name": "another name",
				},
			},
			vectorizerCalledWith: &models.Object{
				Class: cls,
				Properties: map[string]interface{}{
					"name": "another name",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: lastTime,
				Class:      cls,
				ID:         uuid,
				Vector:     []float32{1, 2, 3},
				PrimitiveSchema: map[string]interface{}{
					"name": "another name",
				},
			},
			stage: stageCount,
		},
		{
			name: "without properties",
			previous: &models.Object{
				Class: cls,
			},
			updated: &models.Object{
				Class: cls,
				ID:    uuid,
			},
			vectorizerCalledWith: &models.Object{
				Class:      cls,
				Properties: map[string]interface{}{},
			},
			expectedOutput: &MergeDocument{
				UpdateTime:      lastTime,
				Class:           cls,
				ID:              uuid,
				Vector:          []float32{1, 2, 3},
				PrimitiveSchema: map[string]interface{}{},
			},
			stage: stageCount,
		},
		{
			name: "add primitive properties of different types",
			previous: &models.Object{
				Class:      cls,
				Properties: map[string]interface{}{},
			},
			updated: &models.Object{
				Class: cls,
				ID:    uuid,
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
			vectorizerCalledWith: &models.Object{
				Class: cls,
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
				UpdateTime: lastTime,
				Class:      cls,
				ID:         uuid,
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
			stage: stageCount,
		},
		{
			name: "add primitive and ref properties",
			previous: &models.Object{
				Class:      cls,
				Properties: map[string]interface{}{},
			},
			updated: &models.Object{
				Class: cls,
				ID:    uuid,
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
					"hasAnimals": []interface{}{
						map[string]interface{}{
							"beacon": "weaviate://localhost/AnimalAction/a8ffc82c-9845-4014-876c-11369353c33c",
						},
					},
				},
			},
			vectorizerCalledWith: &models.Object{
				Class: cls,
				Properties: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
			},
			expectedOutput: &MergeDocument{
				UpdateTime: lastTime,
				Class:      cls,
				ID:         uuid,
				PrimitiveSchema: map[string]interface{}{
					"name": "My little pony zoo with extra sparkles",
				},
				Vector: []float32{1, 2, 3},
				References: BatchReferences{
					BatchReference{
						From: crossrefMustParseSource("weaviate://localhost/ZooAction/dd59815b-142b-4c54-9b12-482434bd54ca/hasAnimals"),
						To:   crossrefMustParse("weaviate://localhost/AnimalAction/a8ffc82c-9845-4014-876c-11369353c33c"),
					},
				},
			},
			stage: stageCount,
		},
		{
			name: "update vector non-vectorized class",
			previous: &models.Object{
				Class: "NotVectorized",
				Properties: map[string]interface{}{
					"description": "this description was set initially",
				},
				Vector: []float32{0.7, 0.3},
			},
			updated: &models.Object{
				Class:  "NotVectorized",
				ID:     uuid,
				Vector: []float32{0.66, 0.22},
			},
			vectorizerCalledWith: nil,
			expectedOutput: &MergeDocument{
				UpdateTime:      lastTime,
				Class:           "NotVectorized",
				ID:              uuid,
				Vector:          []float32{0.66, 0.22},
				PrimitiveSchema: map[string]interface{}{},
			},
			stage: stageCount,
		},
		{
			name: "do not update vector non-vectorized class",
			previous: &models.Object{
				Class: "NotVectorized",
				Properties: map[string]interface{}{
					"description": "this description was set initially",
				},
				Vector: []float32{0.7, 0.3},
			},
			updated: &models.Object{
				Class: "NotVectorized",
				ID:    uuid,
				Properties: map[string]interface{}{
					"description": "this description was updated",
				},
			},
			vectorizerCalledWith: nil,
			expectedOutput: &MergeDocument{
				UpdateTime: lastTime,
				Class:      "NotVectorized",
				ID:         uuid,
				Vector:     []float32{0.7, 0.3},
				PrimitiveSchema: map[string]interface{}{
					"description": "this description was updated",
				},
			},
			stage: stageCount,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeGetManager(zooAnimalSchemaForTest())
			m.timeSource = fakeTimeSource{}
			cls := ""
			if tc.updated != nil {
				cls = tc.updated.Class
			}
			if tc.previous != nil {
				if tc.previous.Properties != nil && tc.updated.Vector == nil {
					m.modulesProvider.On("VectorizerName", mock.Anything).Return("some-module", nil)
				}
				m.repo.On("Object", cls, uuid, search.SelectProperties(nil), additional.Properties{}, "").
					Return(&search.Result{
						Schema:    tc.previous.Properties,
						ClassName: tc.previous.Class,
						Vector:    tc.previous.Vector,
					}, nil)
			} else if tc.stage >= stageAuthorization {
				m.repo.On("Object", cls, uuid, search.SelectProperties(nil), additional.Properties{}, "").
					Return((*search.Result)(nil), tc.errGetObject)
			}

			if tc.expectedOutput != nil {
				m.repo.On("Merge", *tc.expectedOutput).Return(tc.errMerge)
			}

			if tc.vectorizerCalledWith != nil {
				if tc.errUpdateObject != nil {
					m.modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
						Return(nil, tc.errUpdateObject)
				} else {
					m.modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
						Return(tc.expectedOutput.Vector, nil)
				}
			}

			if tc.expectedOutput != nil && tc.expectedOutput.Vector != nil {
				m.modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
					Return(tc.expectedOutput.Vector, tc.errUpdateObject)
			}

			// called during validation of cross-refs only.
			m.repo.On("Exists", mock.Anything, mock.Anything).Maybe().Return(true, tc.errExists)

			err := m.MergeObject(context.Background(), nil, tc.updated, nil)
			code := 0
			if err != nil {
				code = err.Code
			}
			if tc.wantCode != code {
				t.Fatalf("status code want: %v got: %v", tc.wantCode, code)
			} else if code == 0 && err != nil {
				t.Fatal(err)
			}

			m.repo.AssertExpectations(t)
			m.modulesProvider.AssertExpectations(t)
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
