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
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func Test_ReferencesAddDeprecated(t *testing.T) {
	cls := "Zoo"
	id := strfmt.UUID("my-id")
	t.Run("without prior refs", func(t *testing.T) {
		req := AddReferenceInput{
			ID:       id,
			Property: "hasAnimals",
			Ref: models.SingleRef{
				Beacon: strfmt.URI("weaviate://localhost/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
			},
		}
		m := newFakeGetManager(zooAnimalSchemaForTest())
		m.repo.On("Exists", "Animal", mock.Anything).Return(true, nil)
		m.repo.On("ObjectByID", mock.Anything, mock.Anything, mock.Anything).Return(&search.Result{
			ClassName: cls,
			Schema: map[string]interface{}{
				"name": "MyZoo",
			},
		}, nil)
		expectedRefProperty := "hasAnimals"
		source := crossref.NewSource(schema.ClassName(cls), schema.PropertyName(expectedRefProperty), id)
		target := crossref.New("localhost", "Animal", "d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")
		m.repo.On("AddReference", source, target).Return(nil)
		m.modulesProvider.On("UsingRef2Vec", mock.Anything).Return(false)

		err := m.AddObjectReference(context.Background(), nil, &req, nil, "")
		require.Nil(t, err)
		m.repo.AssertExpectations(t)
	})
	t.Run("source object missing", func(t *testing.T) {
		req := AddReferenceInput{
			ID:       strfmt.UUID("my-id"),
			Property: "hasAnimals",
			Ref: models.SingleRef{
				Beacon: strfmt.URI("weaviate://localhost/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
			},
		}
		m := newFakeGetManager(zooAnimalSchemaForTest())
		m.repo.On("ObjectByID", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		err := m.AddObjectReference(context.Background(), nil, &req, nil, "")
		require.NotNil(t, err)
		if !err.BadRequest() {
			t.Errorf("error expected: not found error got: %v", err)
		}
	})
	t.Run("source object missing", func(t *testing.T) {
		req := AddReferenceInput{
			ID:       strfmt.UUID("my-id"),
			Property: "hasAnimals",
			Ref: models.SingleRef{
				Beacon: strfmt.URI("weaviate://localhost/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7"),
			},
		}
		m := newFakeGetManager(zooAnimalSchemaForTest())
		m.repo.On("ObjectByID", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("any"))
		err := m.AddObjectReference(context.Background(), nil, &req, nil, "")
		require.NotNil(t, err)
		if err.Code != StatusInternalServerError {
			t.Errorf("error expected: internal error, got: %v", err)
		}
	})
}

func Test_ReferenceAdd(t *testing.T) {
	t.Parallel()
	var (
		cls    = "Zoo"
		prop   = "hasAnimals"
		id     = strfmt.UUID("d18c8e5e-000-0000-0000-56b0cfe33ce7")
		refID  = strfmt.UUID("d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")
		uri    = strfmt.URI("weaviate://localhost/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")
		anyErr = errors.New("any")
		ref    = models.SingleRef{Beacon: uri}
		req    = AddReferenceInput{
			Class:    cls,
			ID:       id,
			Property: prop,
			Ref:      ref,
		}
		source = crossref.NewSource(schema.ClassName(cls), schema.PropertyName(prop), id)
		target = crossref.New("localhost", "Animal", refID)
	)

	tests := []struct {
		Name string
		// inputs
		Req AddReferenceInput

		// outputs
		ExpectedRef models.SingleRef
		WantCode    int
		WantErr     error
		SrcNotFound bool
		// control errors
		ErrAddRef       error
		ErrTargetExists error
		ErrSrcExists    error
		ErrAuth         error
		ErrLock         error
		ErrSchema       error
		// Stage: 1 -> validation(), 2 -> target exists(), 3 -> source exists(), 4 -> AddReference()
		Stage int
	}{
		{
			Name: "locking", Req: req, Stage: 0,
			WantCode: StatusInternalServerError, WantErr: anyErr, ErrLock: anyErr,
		},
		{
			Name: "authorization", Req: req, Stage: 0,
			WantCode: StatusForbidden, WantErr: anyErr, ErrAuth: anyErr,
		},
		{
			Name: "get schema",
			Req:  req, Stage: 1,
			ErrSchema: anyErr,
			WantCode:  StatusBadRequest,
		},
		{
			Name: "empty data type",
			Req:  AddReferenceInput{Class: cls, ID: id, Property: "emptyType", Ref: ref}, Stage: 1,
			WantCode: StatusBadRequest,
		},
		{
			Name: "primitive data type",
			Req:  AddReferenceInput{Class: cls, ID: id, Property: "name", Ref: ref}, Stage: 1,
			WantCode: StatusBadRequest,
		},
		{
			Name: "unknown property",
			Req:  AddReferenceInput{Class: cls, ID: id, Property: "unknown", Ref: ref}, Stage: 1,
			WantCode: StatusBadRequest,
		},
		{
			Name: "valid class name",
			Req:  AddReferenceInput{Class: "-", ID: id, Property: prop}, Stage: 1,
			WantCode: StatusBadRequest,
		},
		{
			Name: "reserved property name",
			Req:  AddReferenceInput{Class: cls, ID: id, Property: "_id"}, Stage: 1,
			WantCode: StatusBadRequest,
		},
		{
			Name: "valid property name",
			Req:  AddReferenceInput{Class: cls, ID: id, Property: "-"}, Stage: 1,
			WantCode: StatusBadRequest,
		},

		{Name: "add valid reference", Req: req, Stage: 4},
		{
			Name: "referenced class not found", Req: req, Stage: 2,
			WantCode:        StatusBadRequest,
			ErrTargetExists: anyErr,
			WantErr:         anyErr,
		},
		{
			Name: "source object internal error", Req: req, Stage: 3,
			WantCode:     StatusInternalServerError,
			ErrSrcExists: anyErr,
			WantErr:      anyErr,
		},
		{
			Name: "source object missing", Req: req, Stage: 3,
			WantCode:    StatusNotFound,
			SrcNotFound: true,
		},
		{
			Name: "internal error", Req: req, Stage: 4,
			WantCode:  StatusInternalServerError,
			ErrAddRef: anyErr,
			WantErr:   anyErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			m := newFakeGetManager(zooAnimalSchemaForTest())
			m.authorizer.Err = tc.ErrAuth
			m.locks.Err = tc.ErrLock
			m.schemaManager.(*fakeSchemaManager).GetschemaErr = tc.ErrSchema
			m.modulesProvider.On("UsingRef2Vec", mock.Anything).Return(false)
			if tc.Stage >= 2 {
				m.repo.On("Exists", "Animal", refID).Return(true, tc.ErrTargetExists).Once()
			}
			if tc.Stage >= 3 {
				m.repo.On("Exists", tc.Req.Class, tc.Req.ID).Return(!tc.SrcNotFound, tc.ErrSrcExists).Once()
			}
			if tc.Stage >= 4 {
				m.repo.On("AddReference", source, target).Return(tc.ErrAddRef).Once()
			}

			err := m.AddObjectReference(context.Background(), nil, &tc.Req, nil, "")
			if tc.WantCode != 0 {
				code := 0
				if err != nil {
					code = err.Code
				}
				if code != tc.WantCode {
					t.Fatalf("code expected: %v, got %v", tc.WantCode, code)
				}

				if tc.WantErr != nil && !errors.Is(err, tc.WantErr) {
					t.Errorf("wrapped error expected: %v, got %v", tc.WantErr, err.Err)
				}

			}
			m.repo.AssertExpectations(t)
		})
	}
}

func Test_ReferenceUpdate(t *testing.T) {
	t.Parallel()
	var (
		cls    = "Zoo"
		prop   = "hasAnimals"
		id     = strfmt.UUID("d18c8e5e-000-0000-0000-56b0cfe33ce7")
		refID  = strfmt.UUID("d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")
		uri    = strfmt.URI("weaviate://localhost/Animals/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")
		anyErr = errors.New("any")
		refs   = models.MultipleRef{&models.SingleRef{Beacon: uri, Class: "Animals"}}
		req    = PutReferenceInput{
			Class:    cls,
			ID:       id,
			Property: prop,
			Refs:     refs,
		}
	)

	tests := []struct {
		Name string
		// inputs
		Req PutReferenceInput

		// outputs
		ExpectedRef models.SingleRef
		WantCode    int
		WantErr     error
		SrcNotFound bool
		// control errors
		ErrPutRefs      error
		ErrTargetExists error
		ErrSrcExists    error
		ErrAuth         error
		ErrLock         error
		ErrSchema       error
		// Stage: 1 -> validation(), 2 -> target exists(), 3 -> PutObject()
		Stage int
	}{
		{
			Name: "source object internal error", Req: req,
			WantCode:     StatusInternalServerError,
			ErrSrcExists: anyErr,
			WantErr:      NewErrInternal("repo: object by id: %v", anyErr),
			Stage:        1,
		},
		{
			Name: "source object missing", Req: req,
			WantCode:    StatusNotFound,
			SrcNotFound: true,
			Stage:       1,
		},
		{
			Name: "locking", Req: req,
			WantCode: StatusInternalServerError, WantErr: anyErr, ErrLock: anyErr,
			Stage: 1,
		},
		{
			Name: "authorization", Req: req,
			WantCode: StatusForbidden, WantErr: anyErr, ErrAuth: anyErr,
			Stage: 1,
		},
		{
			Name: "get schema",
			Req:  req, Stage: 1,
			ErrSchema: anyErr,
			WantCode:  StatusBadRequest,
		},
		{
			Name: "empty data type",
			Req:  PutReferenceInput{Class: cls, ID: id, Property: "emptyType", Refs: refs}, Stage: 1,
			WantCode: StatusBadRequest,
		},
		{
			Name: "primitive data type",
			Req:  PutReferenceInput{Class: cls, ID: id, Property: "name", Refs: refs}, Stage: 1,
			WantCode: StatusBadRequest,
		},
		{
			Name: "unknown property",
			Req:  PutReferenceInput{Class: cls, ID: id, Property: "unknown", Refs: refs}, Stage: 1,
			WantCode: StatusBadRequest,
		},
		{
			Name: "reserved property name",
			Req:  PutReferenceInput{Class: cls, ID: id, Property: "_id", Refs: refs}, Stage: 1,
			WantCode: StatusBadRequest,
		},
		{
			Name: "valid property name",
			Req:  PutReferenceInput{Class: cls, ID: id, Property: "-", Refs: refs}, Stage: 1,
			WantCode: StatusBadRequest,
		},

		{Name: "update valid reference", Req: req, Stage: 3},
		{
			Name: "referenced class not found", Req: req, Stage: 2,
			WantCode:        StatusBadRequest,
			ErrTargetExists: anyErr,
			WantErr:         anyErr,
		},
		{
			Name: "internal error", Req: req, Stage: 3,
			WantCode:   StatusInternalServerError,
			ErrPutRefs: anyErr,
			WantErr:    anyErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			m := newFakeGetManager(zooAnimalSchemaForTest())
			m.authorizer.Err = tc.ErrAuth
			m.locks.Err = tc.ErrLock
			m.schemaManager.(*fakeSchemaManager).GetschemaErr = tc.ErrSchema
			srcObj := &search.Result{
				ClassName: cls,
				Schema: map[string]interface{}{
					"name": "MyZoo",
				},
			}
			if tc.SrcNotFound {
				srcObj = nil
			}
			if tc.Stage >= 1 {
				m.repo.On("Object", cls, id, mock.Anything, mock.Anything, "").Return(srcObj, tc.ErrSrcExists)
			}

			if tc.Stage >= 2 {
				m.repo.On("Exists", "Animals", refID).Return(true, tc.ErrTargetExists).Once()
			}

			if tc.Stage >= 3 {
				m.repo.On("PutObject", mock.Anything, mock.Anything).Return(tc.ErrPutRefs).Once()
			}

			err := m.UpdateObjectReferences(context.Background(), nil, &tc.Req, nil, "")
			if tc.WantCode != 0 {
				code := 0
				if err != nil {
					code = err.Code
				}
				if code != tc.WantCode {
					t.Fatalf("code expected: %v, got %v", tc.WantCode, code)
				}

				if tc.WantErr != nil && !errors.Is(err, tc.WantErr) {
					t.Errorf("wrapped error expected: %v, got %v", tc.WantErr, err.Err)
				}

			}
			m.repo.AssertExpectations(t)
		})
	}
}

func Test_ReferenceDelete(t *testing.T) {
	t.Parallel()
	var (
		cls    = "Zoo"
		prop   = "hasAnimals"
		id     = strfmt.UUID("d18c8e5e-000-0000-0000-56b0cfe33ce7")
		uri    = strfmt.URI("weaviate://localhost/Animal/d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")
		anyErr = errors.New("any")
		ref    = models.SingleRef{Beacon: uri}
		ref2   = &models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/d18c8e5e-a339-4c15-8af6-56b0cfe33ce5")}
		ref3   = &models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/d18c8e5e-a339-4c15-8af6-56b0cfe33ce6")}
		req    = DeleteReferenceInput{
			Class:     cls,
			ID:        id,
			Property:  prop,
			Reference: ref,
		}
	)

	fakeProperties := func(refs ...*models.SingleRef) map[string]interface{} {
		mrefs := make(models.MultipleRef, len(refs))
		copy(mrefs, refs)
		return map[string]interface{}{
			"name": "MyZoo",
			prop:   mrefs,
		}
	}

	tests := []struct {
		Name string
		// inputs
		Req           DeleteReferenceInput
		properties    interface{}
		NewSrcRefsLen int
		// outputs
		ExpectedRef models.SingleRef
		WantCode    int
		WantErr     error
		SrcNotFound bool
		// control errors
		ErrPutRefs      error
		ErrTargetExists error
		ErrSrcExists    error
		ErrAuth         error
		ErrLock         error
		ErrSchema       error
		// Stage: 1 -> validation(), 2 -> target exists(), 3 -> PutObject()
		Stage int
	}{
		{
			Name: "source object internal error", Req: req,
			WantCode:     StatusInternalServerError,
			ErrSrcExists: anyErr,
			WantErr:      NewErrInternal("repo: object by id: %v", anyErr), Stage: 2,
		},
		{
			Name: "source object missing", Req: req,
			WantCode:    StatusNotFound,
			SrcNotFound: true, Stage: 2,
		},
		{
			Name: "locking", Req: req,
			WantCode: StatusInternalServerError, WantErr: anyErr, ErrLock: anyErr, Stage: 2,
		},
		{
			Name: "authorization", Req: req,
			WantCode: StatusForbidden, WantErr: anyErr, ErrAuth: anyErr, Stage: 2,
		},
		{
			Name: "get schema",
			Req:  req, Stage: 2,
			ErrSchema: anyErr,
			WantCode:  StatusBadRequest,
		},
		{
			Name: "empty data type",
			Req:  DeleteReferenceInput{Class: cls, ID: id, Property: "emptyType", Reference: ref}, Stage: 2,
			WantCode: StatusBadRequest,
		},
		{
			Name: "primitive data type",
			Req:  DeleteReferenceInput{Class: cls, ID: id, Property: "name", Reference: ref}, Stage: 2,
			WantCode: StatusBadRequest,
		},
		{
			Name: "unknown property",
			Req:  DeleteReferenceInput{Class: cls, ID: id, Property: "unknown", Reference: ref}, Stage: 2,
			WantCode: StatusBadRequest,
		},
		{
			Name: "reserved property name",
			Req:  DeleteReferenceInput{Class: cls, ID: id, Property: "_id"}, Stage: 1,
			WantCode: StatusBadRequest,
		},
		{
			Name: "valid property name",
			Req:  DeleteReferenceInput{Class: cls, ID: id, Property: "-"}, Stage: 1,
			WantCode: StatusBadRequest,
		},
		{
			Name:       "delete one reference",
			Req:        req,
			properties: fakeProperties(ref2, &ref, ref3), NewSrcRefsLen: 2,
			Stage: 3,
		},
		{
			Name:       "delete two references",
			Req:        req,
			properties: fakeProperties(&ref, ref2, &ref), NewSrcRefsLen: 1,
			Stage: 3,
		},
		{
			Name:       "delete all references",
			Req:        req,
			properties: fakeProperties(&ref, &ref), NewSrcRefsLen: 0,
			Stage: 3,
		},
		{
			Name:       "reference not found",
			Req:        req,
			properties: fakeProperties(ref2, ref3), NewSrcRefsLen: 2,
			Stage: 2,
		},
		{
			Name:       "wrong reference type",
			Req:        req,
			properties: map[string]interface{}{prop: "wrong reference type"}, NewSrcRefsLen: 0,
			Stage: 2,
		},
		{
			Name:       "empty properties list",
			Req:        req,
			properties: nil, NewSrcRefsLen: 0,
			Stage: 2,
		},
		{
			Name:       "internal error",
			Req:        req,
			properties: fakeProperties(ref2, &ref, ref3), NewSrcRefsLen: 3,
			Stage:      3,
			WantCode:   StatusInternalServerError,
			ErrPutRefs: anyErr,
			WantErr:    anyErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			m := newFakeGetManager(zooAnimalSchemaForTest())
			m.authorizer.Err = tc.ErrAuth
			m.locks.Err = tc.ErrLock
			m.schemaManager.(*fakeSchemaManager).GetschemaErr = tc.ErrSchema
			srcObj := &search.Result{
				ClassName: cls,
				Schema:    tc.properties,
			}
			if tc.SrcNotFound {
				srcObj = nil
			}
			if tc.Stage >= 2 {
				m.repo.On("Object", cls, id, mock.Anything, mock.Anything, "").Return(srcObj, tc.ErrSrcExists)
				m.modulesProvider.On("UsingRef2Vec", mock.Anything).Return(false)
			}

			if tc.Stage >= 3 {
				m.repo.On("PutObject", mock.Anything, mock.Anything).Return(tc.ErrPutRefs).Once()
			}

			err := m.DeleteObjectReference(context.Background(), nil, &tc.Req, nil, "")
			if tc.WantCode != 0 {
				code := 0
				if err != nil {
					code = err.Code
				}
				if code != tc.WantCode {
					t.Fatalf("code expected: %v, got %v", tc.WantCode, code)
				}

				if tc.WantErr != nil && !errors.Is(err, tc.WantErr) {
					t.Errorf("wrapped error expected: %v, got %v", tc.WantErr, err.Err)
				}

			} else if tc.properties != nil {
				refs, ok := srcObj.Schema.(map[string]interface{})[prop].(models.MultipleRef)
				if g, w := len(refs), tc.NewSrcRefsLen; ok && g != w {
					t.Errorf("length of source reference after deletion got:%v, want:%v", g, w)
				}

			}

			m.repo.AssertExpectations(t)
		})
	}
}

func Test_ReferenceAdd_Ref2Vec(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m := newFakeGetManager(articleSchemaForTest())

	req := AddReferenceInput{
		Class:    "Article",
		ID:       strfmt.UUID("e1a60252-c38c-496d-8e54-306e1cedc5c4"),
		Property: "hasParagraphs",
		Ref: models.SingleRef{
			Beacon: strfmt.URI("weaviate://localhost/Paragraph/494a2fe5-3e4c-4e9a-a47e-afcd9814f5ea"),
		},
	}

	source := crossref.NewSource(schema.ClassName(req.Class), schema.PropertyName(req.Property), req.ID)
	target := crossref.New("localhost", "Paragraph", "494a2fe5-3e4c-4e9a-a47e-afcd9814f5ea")
	tenant := "randomTenant"

	parent := &search.Result{
		ID:        strfmt.UUID("e1a60252-c38c-496d-8e54-306e1cedc5c4"),
		ClassName: "Article",
		Schema:    map[string]interface{}{},
	}

	ref1 := &search.Result{
		ID:        strfmt.UUID("494a2fe5-3e4c-4e9a-a47e-afcd9814f5ea"),
		ClassName: "Paragraph",
		Vector:    []float32{2, 4, 6},
	}

	m.repo.On("Exists", "Article", parent.ID).Return(true, nil)
	m.repo.On("Exists", "Paragraph", ref1.ID).Return(true, nil)
	m.repo.On("Object", "Article", parent.ID, search.SelectProperties{}, additional.Properties{}, tenant).Return(parent, nil)
	m.repo.On("Object", "Paragraph", ref1.ID, search.SelectProperties{}, additional.Properties{}, tenant).Return(ref1, nil)
	m.repo.On("AddReference", source, target).Return(nil)
	m.modulesProvider.On("UsingRef2Vec", mock.Anything).Return(true)
	m.modulesProvider.On("UpdateVector", mock.Anything, mock.AnythingOfType(FindObjectFn)).
		Return(ref1.Vector, nil)
	m.repo.On("PutObject", mock.Anything, ref1.Vector).Return(nil)
	err := m.Manager.AddObjectReference(ctx, nil, &req, nil, tenant)
	assert.Nil(t, err)
}

func Test_ReferenceDelete_Ref2Vec(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m := newFakeGetManager(articleSchemaForTest())

	req := DeleteReferenceInput{
		Class:    "Article",
		ID:       strfmt.UUID("e1a60252-c38c-496d-8e54-306e1cedc5c4"),
		Property: "hasParagraphs",
		Reference: models.SingleRef{
			Beacon: strfmt.URI("weaviate://localhost/Paragraph/494a2fe5-3e4c-4e9a-a47e-afcd9814f5ea"),
		},
	}

	tenant := "randomTenant"

	parent := &search.Result{
		ID:        strfmt.UUID("e1a60252-c38c-496d-8e54-306e1cedc5c4"),
		ClassName: "Article",
		Schema:    map[string]interface{}{},
	}

	ref1 := &search.Result{
		ID:        strfmt.UUID("494a2fe5-3e4c-4e9a-a47e-afcd9814f5ea"),
		ClassName: "Paragraph",
		Vector:    []float32{2, 4, 6},
	}

	m.repo.On("Exists", "Article", parent.ID).Return(true, nil)
	m.repo.On("Exists", "Paragraph", ref1.ID).Return(true, nil)
	m.repo.On("Object", req.Class, req.ID, search.SelectProperties{}, additional.Properties{}, tenant).Return(parent, nil)
	m.repo.On("PutObject", parent.Object(), []float32(nil)).Return(nil)
	m.modulesProvider.On("UsingRef2Vec", mock.Anything).Return(true)

	err := m.Manager.DeleteObjectReference(ctx, nil, &req, nil, tenant)
	assert.Nil(t, err)
}

func articleSchemaForTest() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             "Paragraph",
					VectorIndexConfig: hnsw.UserConfig{},
					Properties: []*models.Property{
						{
							Name:     "contents",
							DataType: []string{"text"},
						},
					},
				},
				{
					Class:             "Article",
					VectorIndexConfig: hnsw.UserConfig{},
					Properties: []*models.Property{
						{
							Name:         "title",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:     "hasParagraphs",
							DataType: []string{"Paragraph"},
						},
					},
					Vectorizer: "ref2vec-centroid",
					ModuleConfig: map[string]interface{}{
						"ref2vec-centroid": map[string]interface{}{
							"referenceProperties": []string{"hasParagraphs"},
							"method":              "mean",
						},
					},
				},
			},
		},
	}
}

func zooAnimalSchemaForTest() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:             "ZooAction",
					VectorIndexConfig: hnsw.UserConfig{},
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:     "area",
							DataType: []string{"number"},
						},
						{
							Name:     "employees",
							DataType: []string{"int"},
						},
						{
							Name:     "located",
							DataType: []string{"geoCoordinates"},
						},
						{
							Name:     "foundedIn",
							DataType: []string{"date"},
						},
						{
							Name:     "hasAnimals",
							DataType: []string{"AnimalAction"},
						},
					},
				},
				{
					Class:             "AnimalAction",
					VectorIndexConfig: hnsw.UserConfig{},
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
					},
				},
				{
					Class:             "Zoo",
					VectorIndexConfig: hnsw.UserConfig{},
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
						{
							Name:     "area",
							DataType: []string{"number"},
						},
						{
							Name:     "employees",
							DataType: []string{"int"},
						},
						{
							Name:     "located",
							DataType: []string{"geoCoordinates"},
						},
						{
							Name:     "foundedIn",
							DataType: []string{"date"},
						},
						{
							Name:     "hasAnimals",
							DataType: []string{"Animal"},
						},
						{
							Name:     "emptyType",
							DataType: []string{""},
						},
					},
				},
				{
					Class:             "Animal",
					VectorIndexConfig: hnsw.UserConfig{},
					Properties: []*models.Property{
						{
							Name:         "name",
							DataType:     schema.DataTypeText.PropString(),
							Tokenization: models.PropertyTokenizationWhitespace,
						},
					},
				},
				{
					Class:             "NotVectorized",
					VectorIndexConfig: hnsw.UserConfig{},
					Properties: []*models.Property{
						{
							Name:     "description",
							DataType: []string{"text"},
						},
					},
					Vectorizer: "none",
				},
			},
		},
	}
}
