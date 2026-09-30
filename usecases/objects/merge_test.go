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
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
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
				Vectors:    map[string]models.Vector{},
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
				Vectors: nil, // nil because named vectors haven't changed
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
				Vectors: nil, // nil because named vectors haven't changed
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
				Vectors: nil, // nil because named vectors haven't changed
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
				Vectors:         nil, // nil because named vectors haven't changed
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
					"employees": float64(70),
					"located": &models.GeoCoordinates{
						Latitude:  ptFloat32(30.2),
						Longitude: ptFloat32(60.2),
					},
					"foundedIn": timeMustParse(time.RFC3339, "2002-10-02T15:00:00Z"),
				},
				Vectors: nil, // nil because named vectors haven't changed
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
				Vectors: nil, // nil because named vectors haven't changed
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
				Vectors:         nil, // nil because named vectors haven't changed
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
			// vectorizerCalledWith is set to ensure the mock is configured even though
			// no new vector is computed (the mock returns nil, keeping previous vector)
			vectorizerCalledWith: &models.Object{
				Class: "NotVectorized",
				Properties: map[string]interface{}{
					"description": "this description was updated",
				},
			},
			// Vector is nil because the vector hasn't changed - this optimization
			// reduces network bandwidth when replicating patches. The replica-side
			// code preserves the existing vector when Vector is nil.
			expectedOutput: &MergeDocument{
				UpdateTime: lastTime,
				Class:      "NotVectorized",
				ID:         uuid,
				Vector:     nil,
				PrimitiveSchema: map[string]interface{}{
					"description": "this description was updated",
				},
				Vectors: nil, // nil because named vectors haven't changed
			},
			stage: stageCount,
		},
		// Test cases for vector optimization in MergeDocument
		{
			name: "legacy vector explicitly changed in update",
			previous: &models.Object{
				Class: "NotVectorized",
				Properties: map[string]interface{}{
					"description": "original description",
				},
				Vector: []float32{0.7, 0.8},
			},
			updated: &models.Object{
				Class: "NotVectorized",
				ID:    uuid,
				Properties: map[string]interface{}{
					"description": "updated description",
				},
				// Explicitly providing new legacy vector
				Vector: []float32{0.9, 1.0},
			},
			vectorizerCalledWith: &models.Object{
				Class: "NotVectorized",
				Properties: map[string]interface{}{
					"description": "updated description",
				},
			},
			// Legacy vector included because it changed
			expectedOutput: &MergeDocument{
				UpdateTime: lastTime,
				Class:      "NotVectorized",
				ID:         uuid,
				Vector:     []float32{0.9, 1.0},
				PrimitiveSchema: map[string]interface{}{
					"description": "updated description",
				},
				Vectors: nil,
			},
			stage: stageCount,
		},
		{
			name: "same vector explicitly provided is still omitted",
			previous: &models.Object{
				Class: "NotVectorized",
				Properties: map[string]interface{}{
					"description": "original description",
				},
				Vector: []float32{0.7, 0.8},
			},
			updated: &models.Object{
				Class: "NotVectorized",
				ID:    uuid,
				Properties: map[string]interface{}{
					"description": "updated description",
				},
				// Explicitly providing same vector as previous
				Vector: []float32{0.7, 0.8},
			},
			vectorizerCalledWith: &models.Object{
				Class: "NotVectorized",
				Properties: map[string]interface{}{
					"description": "updated description",
				},
			},
			// Vector omitted because it's the same as previous (optimization)
			expectedOutput: &MergeDocument{
				UpdateTime: lastTime,
				Class:      "NotVectorized",
				ID:         uuid,
				Vector:     nil,
				PrimitiveSchema: map[string]interface{}{
					"description": "updated description",
				},
				Vectors: nil,
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
				m.repo.On("Object", cls, uuid, search.SelectProperties(nil), additional.Properties{}, "").
					Return(&search.Result{
						Schema:    tc.previous.Properties,
						ClassName: tc.previous.Class,
						Vector:    tc.previous.Vector,
						Vectors:   tc.previous.Vectors,
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

// Test_MergeObject_TenantReachesVectorizer is a regression test for
// https://github.com/weaviate/weaviate/issues/13344. PATCH hands the
// merged object to the vectorizer, which looks up the stored object to decide
// whether a source property changed (usecases/modules/compare.go). Without the
// tenant on that object the lookup fails on a multi-tenant class, and every
// PATCH re-vectorizes even when nothing changed.
func Test_MergeObject_TenantReachesVectorizer(t *testing.T) {
	t.Parallel()
	var (
		uuid        = strfmt.UUID("dd59815b-142b-4c54-9b12-482434bd54ca")
		errNoTenant = NewErrMultiTenancy(errors.New("has multi-tenancy enabled, but request was without tenant"))
	)

	tests := []struct {
		name   string
		class  *models.Class
		tenant string
		// vectors the stored object already has
		vector  []float32
		vectors models.Vectors
	}{
		{
			name: "multi-tenant class",
			class: &models.Class{
				Class:              "MultiTenantZoo",
				Vectorizer:         config.VectorizerModuleText2VecContextionary,
				VectorIndexConfig:  hnsw.UserConfig{},
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				Properties: []*models.Property{
					{Name: "name", DataType: schema.DataTypeText.PropString()},
				},
			},
			tenant: "tenantA",
			vector: []float32{1, 2, 3},
		},
		{
			name: "multi-tenant class with named vectors",
			class: &models.Class{
				Class: "MultiTenantNamedVectorZoo",
				VectorConfig: map[string]models.VectorConfig{
					"description": {
						Vectorizer: map[string]interface{}{
							config.VectorizerModuleText2VecContextionary: map[string]interface{}{},
						},
						VectorIndexType:   "hnsw",
						VectorIndexConfig: hnsw.UserConfig{},
					},
				},
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
				Properties: []*models.Property{
					{Name: "name", DataType: schema.DataTypeText.PropString()},
				},
			},
			tenant:  "tenantA",
			vectors: models.Vectors{"description": []float32{1, 2, 3}},
		},
		{
			name: "single-tenant class",
			class: &models.Class{
				Class:             "SingleTenantZoo",
				Vectorizer:        config.VectorizerModuleText2VecContextionary,
				VectorIndexConfig: hnsw.UserConfig{},
				Properties: []*models.Property{
					{Name: "name", DataType: schema.DataTypeText.PropString()},
				},
			},
			tenant: "",
			vector: []float32{1, 2, 3},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeGetManager(schema.Schema{Objects: &models.Schema{Classes: []*models.Class{tc.class}}})
			m.timeSource = fakeTimeSource{}
			cls := tc.class.Class

			// The stored object lives in the tenant's shard. Like the real repo,
			// a lookup without a tenant fails on a multi-tenant class.
			m.repo.On("Object", cls, uuid, search.SelectProperties(nil), additional.Properties{}, tc.tenant).
				Return(&search.Result{
					ClassName: cls,
					ID:        uuid,
					Tenant:    tc.tenant,
					Schema:    map[string]interface{}{"name": "Unchanged zoo"},
					Vector:    tc.vector,
					Vectors:   tc.vectors,
				}, nil)
			if tc.tenant != "" {
				m.repo.On("Object", cls, uuid, search.SelectProperties(nil), additional.Properties{}, "").
					Maybe().Return(nil, errNoTenant)
			}
			m.repo.On("Merge", mock.Anything).Return(nil)

			var (
				vectorized *models.Object
				lookupErr  error
			)
			m.modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
				Run(func(args mock.Arguments) {
					vectorized = args.Get(0).(*models.Object)
					// look up the stored object the way the re-vectorize check does
					findObject := args.Get(1).(modulecapabilities.FindObjectFn)
					_, lookupErr = findObject(context.Background(), cls, vectorized.ID,
						nil, additional.Properties{}, vectorized.Tenant)
				}).
				Return(nil, nil)

			err := m.MergeObject(context.Background(), nil, &models.Object{
				Class:      cls,
				ID:         uuid,
				Tenant:     tc.tenant,
				Properties: map[string]interface{}{"name": "Unchanged zoo"},
			}, nil)
			require.Nil(t, err)

			require.NotNil(t, vectorized)
			assert.Equal(t, tc.tenant, vectorized.Tenant, "vectorizer must get the request tenant")
			assert.NoError(t, lookupErr, "stored-object lookup failed, so the vectorizer would re-vectorize")
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

func Test_namedVectorsEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		prev     models.Vectors
		next     models.Vectors
		expected bool
	}{
		{
			name:     "both nil",
			prev:     nil,
			next:     nil,
			expected: true,
		},
		{
			name:     "both empty",
			prev:     models.Vectors{},
			next:     models.Vectors{},
			expected: true,
		},
		{
			name:     "nil vs empty",
			prev:     nil,
			next:     models.Vectors{},
			expected: true,
		},
		{
			name:     "empty vs nil",
			prev:     models.Vectors{},
			next:     nil,
			expected: true,
		},
		{
			name: "single vector equal",
			prev: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
			},
			next: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
			},
			expected: true,
		},
		{
			name: "single vector different values",
			prev: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
			},
			next: models.Vectors{
				"vec1": []float32{1.0, 2.0, 4.0},
			},
			expected: false,
		},
		{
			name: "single vector different lengths",
			prev: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
			},
			next: models.Vectors{
				"vec1": []float32{1.0, 2.0},
			},
			expected: false,
		},
		{
			name: "different number of vectors",
			prev: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
			},
			next: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
				"vec2": []float32{4.0, 5.0, 6.0},
			},
			expected: false,
		},
		{
			name: "different vector names",
			prev: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
			},
			next: models.Vectors{
				"vec2": []float32{1.0, 2.0, 3.0},
			},
			expected: false,
		},
		{
			name: "multiple vectors equal",
			prev: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
				"vec2": []float32{4.0, 5.0, 6.0},
			},
			next: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
				"vec2": []float32{4.0, 5.0, 6.0},
			},
			expected: true,
		},
		{
			name: "multiple vectors one different",
			prev: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
				"vec2": []float32{4.0, 5.0, 6.0},
			},
			next: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
				"vec2": []float32{4.0, 5.0, 7.0},
			},
			expected: false,
		},
		{
			name: "multi-vectors equal",
			prev: models.Vectors{
				"vec1": [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			},
			next: models.Vectors{
				"vec1": [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			},
			expected: true,
		},
		{
			name: "multi-vectors different",
			prev: models.Vectors{
				"vec1": [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			},
			next: models.Vectors{
				"vec1": [][]float32{{1.0, 2.0}, {3.0, 5.0}},
			},
			expected: false,
		},
		{
			name: "multi-vectors different length",
			prev: models.Vectors{
				"vec1": [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			},
			next: models.Vectors{
				"vec1": [][]float32{{1.0, 2.0}},
			},
			expected: false,
		},
		{
			name: "mixed single and multi vectors equal",
			prev: models.Vectors{
				"single": []float32{1.0, 2.0, 3.0},
				"multi":  [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			},
			next: models.Vectors{
				"single": []float32{1.0, 2.0, 3.0},
				"multi":  [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			},
			expected: true,
		},
		{
			name: "type mismatch single vs multi",
			prev: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
			},
			next: models.Vectors{
				"vec1": [][]float32{{1.0, 2.0, 3.0}},
			},
			expected: false,
		},
		{
			name: "prev has vector next is empty",
			prev: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
			},
			next:     models.Vectors{},
			expected: false,
		},
		{
			name: "prev is empty next has vector",
			prev: models.Vectors{},
			next: models.Vectors{
				"vec1": []float32{1.0, 2.0, 3.0},
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := namedVectorsEqual(tc.prev, tc.next)
			if result != tc.expected {
				t.Errorf("namedVectorsEqual() = %v, want %v", result, tc.expected)
			}
		})
	}
}

func Test_vectorEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		prev     models.Vector
		next     models.Vector
		expected bool
	}{
		{
			name:     "both nil",
			prev:     nil,
			next:     nil,
			expected: true,
		},
		{
			name:     "single vectors equal",
			prev:     []float32{1.0, 2.0, 3.0},
			next:     []float32{1.0, 2.0, 3.0},
			expected: true,
		},
		{
			name:     "single vectors different",
			prev:     []float32{1.0, 2.0, 3.0},
			next:     []float32{1.0, 2.0, 4.0},
			expected: false,
		},
		{
			name:     "single vectors different length",
			prev:     []float32{1.0, 2.0, 3.0},
			next:     []float32{1.0, 2.0},
			expected: false,
		},
		{
			name:     "multi vectors equal",
			prev:     [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			next:     [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			expected: true,
		},
		{
			name:     "multi vectors different",
			prev:     [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			next:     [][]float32{{1.0, 2.0}, {3.0, 5.0}},
			expected: false,
		},
		{
			name:     "multi vectors different outer length",
			prev:     [][]float32{{1.0, 2.0}, {3.0, 4.0}},
			next:     [][]float32{{1.0, 2.0}},
			expected: false,
		},
		{
			name:     "type mismatch single vs multi",
			prev:     []float32{1.0, 2.0, 3.0},
			next:     [][]float32{{1.0, 2.0, 3.0}},
			expected: false,
		},
		{
			name:     "type mismatch multi vs single",
			prev:     [][]float32{{1.0, 2.0, 3.0}},
			next:     []float32{1.0, 2.0, 3.0},
			expected: false,
		},
		{
			name:     "single vector vs nil",
			prev:     []float32{1.0, 2.0, 3.0},
			next:     nil,
			expected: false,
		},
		{
			name:     "nil vs single vector",
			prev:     nil,
			next:     []float32{1.0, 2.0, 3.0},
			expected: false,
		},
		{
			name:     "empty single vectors",
			prev:     []float32{},
			next:     []float32{},
			expected: true,
		},
		{
			name:     "empty multi vectors",
			prev:     [][]float32{},
			next:     [][]float32{},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := vectorEqual(tc.prev, tc.next)
			if result != tc.expected {
				t.Errorf("vectorEqual() = %v, want %v", result, tc.expected)
			}
		})
	}
}
