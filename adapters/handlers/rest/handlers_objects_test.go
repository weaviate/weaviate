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

package rest

import (
	"context"
	stderrors "errors"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/objects"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	"github.com/semi-technologies/weaviate/usecases/config"
	uco "github.com/semi-technologies/weaviate/usecases/objects"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnrichObjectsWithLinks(t *testing.T) {
	t.Run("add object", func(t *testing.T) {
		type test struct {
			name           string
			object         *models.Object
			expectedResult *models.Object
		}

		tests := []test{
			{
				name:           "without props - nothing changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			{
				name: "without ref props - nothing changes",
				object: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			{
				name: "with a ref prop - no origin configured",
				object: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/objects/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fakeManager := &fakeManager{
					addObjectReturn: test.object,
				}
				h := &objectHandlers{manager: fakeManager}
				res := h.addObject(objects.ObjectsCreateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/objects", nil),
					Body:        test.object,
				}, nil)
				parsed, ok := res.(*objects.ObjectsCreateOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	// This test "with an origin conifgured" is not repeated for every handler,
	// as testing this feature once was deemed sufficient
	t.Run("add object - with an origin configured", func(t *testing.T) {
		type test struct {
			name           string
			object         *models.Object
			expectedResult *models.Object
		}

		tests := []test{
			{
				name:           "without props - nothing changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			{
				name: "without ref props - nothing changes",
				object: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			{
				name: "with a ref prop - no origin configured",
				object: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "https://awesomehost.com/v1/objects/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fakeManager := &fakeManager{
					addObjectReturn: test.object,
				}
				config := config.Config{Origin: "https://awesomehost.com"}
				h := &objectHandlers{manager: fakeManager, config: config}
				res := h.addObject(objects.ObjectsCreateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/objects", nil),
					Body:        test.object,
				}, nil)
				parsed, ok := res.(*objects.ObjectsCreateOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	t.Run("get object deprecated", func(t *testing.T) {
		type test struct {
			name           string
			object         *models.Object
			expectedResult *models.Object
		}

		tests := []test{
			{
				name:           "without props - nothing changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			{
				name: "without ref props - nothing changes",
				object: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			{
				name: "with a ref prop - no origin configured",
				object: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/objects/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fakeManager := &fakeManager{
					getObjectReturn: test.object,
				}
				h := &objectHandlers{manager: fakeManager}
				res := h.getObjectDeprecated(objects.ObjectsGetParams{HTTPRequest: httptest.NewRequest("GET", "/v1/objects", nil)}, nil)
				parsed, ok := res.(*objects.ObjectsClassGetOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	t.Run("get objects", func(t *testing.T) {
		type test struct {
			name           string
			object         []*models.Object
			expectedResult []*models.Object
		}

		tests := []test{
			{
				name:           "without props - nothing changes",
				object:         []*models.Object{{Class: "Foo", Properties: nil}},
				expectedResult: []*models.Object{{Class: "Foo", Properties: nil}},
			},
			{
				name: "without ref props - nothing changes",
				object: []*models.Object{
					{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
				expectedResult: []*models.Object{
					{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
			},
			{
				name: "with a ref prop - no origin configured",
				object: []*models.Object{
					{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
						"someRef": models.MultipleRef{
							&models.SingleRef{
								Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
							},
						},
					}},
				},
				expectedResult: []*models.Object{
					{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
						"someRef": models.MultipleRef{
							&models.SingleRef{
								Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
								Href:   "/v1/objects/85f78e29-5937-4390-a121-5379f262b4e5",
							},
						},
					}},
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fakeManager := &fakeManager{
					getObjectsReturn: test.object,
				}
				h := &objectHandlers{manager: fakeManager}
				res := h.getObjects(objects.ObjectsListParams{HTTPRequest: httptest.NewRequest("GET", "/v1/objects", nil)}, nil)
				parsed, ok := res.(*objects.ObjectsListOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload.Objects)
			})
		}
	})

	t.Run("update object deprecated", func(t *testing.T) {
		type test struct {
			name           string
			object         *models.Object
			expectedResult *models.Object
		}

		tests := []test{
			{
				name:           "without props - nothing changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			{
				name: "without ref props - nothing changes",
				object: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			{
				name: "with a ref prop - no origin configured",
				object: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/objects/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fakeManager := &fakeManager{
					updateObjectReturn: test.object,
				}
				h := &objectHandlers{manager: fakeManager}
				res := h.updateObjectDeprecated(objects.ObjectsUpdateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/objects", nil),
					Body:        test.object,
				}, nil)
				parsed, ok := res.(*objects.ObjectsClassPutOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	t.Run("add object", func(t *testing.T) {
		type test struct {
			name           string
			object         *models.Object
			expectedResult *models.Object
		}

		tests := []test{
			{
				name:           "without props - noaction changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			{
				name: "without ref props - noaction changes",
				object: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			{
				name: "with a ref prop - no origin configured",
				object: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Object{Class: "Foo", Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/objects/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fakeManager := &fakeManager{
					addObjectReturn: test.object,
				}
				h := &objectHandlers{manager: fakeManager}
				res := h.addObject(objects.ObjectsCreateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/objects", nil),
					Body:        test.object,
				}, nil)
				parsed, ok := res.(*objects.ObjectsCreateOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	t.Run("get objects", func(t *testing.T) {
		type test struct {
			name           string
			object         []*models.Object
			expectedResult []*models.Object
		}

		tests := []test{
			{
				name:           "without props - noaction changes",
				object:         []*models.Object{{Class: "Foo", Properties: nil}},
				expectedResult: []*models.Object{{Class: "Foo", Properties: nil}},
			},
			{
				name: "without ref props - noaction changes",
				object: []*models.Object{
					{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
				expectedResult: []*models.Object{
					{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
			},
			{
				name: "with a ref prop - no origin configured",
				object: []*models.Object{
					{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
						"someRef": models.MultipleRef{
							&models.SingleRef{
								Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
							},
						},
					}},
				},
				expectedResult: []*models.Object{
					{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
						"someRef": models.MultipleRef{
							&models.SingleRef{
								Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
								Href:   "/v1/objects/85f78e29-5937-4390-a121-5379f262b4e5",
							},
						},
					}},
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fakeManager := &fakeManager{
					getObjectsReturn: test.object,
				}
				h := &objectHandlers{manager: fakeManager}
				res := h.getObjects(objects.ObjectsListParams{HTTPRequest: httptest.NewRequest("GET", "/v1/objects", nil)}, nil)
				parsed, ok := res.(*objects.ObjectsListOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload.Objects)
			})
		}
	})

	// New endpoints which uniquely identify objects of a class
	t.Run("update object", func(t *testing.T) {
		cls := "MyClass"
		type test struct {
			name           string
			object         *models.Object
			expectedResult *models.Object
			err            error
		}

		tests := []test{
			{
				name:           "without props - noaction changes",
				object:         &models.Object{Class: cls, Properties: nil},
				expectedResult: &models.Object{Class: cls, Properties: nil},
			},
			{
				name: "without ref props - noaction changes",
				object: &models.Object{Class: cls, Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Object{Class: cls, Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			{
				name: "with a ref prop - no origin configured",
				object: &models.Object{Class: cls, Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Object{Class: cls, Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/objects/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
			{
				name: "error forbbiden",
				err:  errors.NewForbidden(&models.Principal{}, "get", "Myclass/123"),
			},
			{
				name: "use case err not found",
				err:  uco.ErrInvalidUserInput{},
			},
			{
				name: "unknown error",
				err:  stderrors.New("any error"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fakeManager := &fakeManager{
					updateObjectReturn: test.object,
					updateObjectErr:    test.err,
				}
				h := &objectHandlers{manager: fakeManager}
				res := h.updateObject(objects.ObjectsClassPutParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/objects/123", nil),
					Body:        test.object,
					ID:          "123",
					ClassName:   cls,
				}, nil)
				parsed, ok := res.(*objects.ObjectsClassPutOK)
				if test.err != nil {
					require.False(t, ok)
					return
				}
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	t.Run("patch object", func(t *testing.T) {
		fakeManager := &fakeManager{}
		h := &objectHandlers{manager: fakeManager}
		req := objects.ObjectsClassPatchParams{
			HTTPRequest: httptest.NewRequest("PATCH", "/v1/objects/MyClass/123", nil),
			ClassName:   "MyClass",
			ID:          "123",
			Body:        &models.Object{Properties: map[string]interface{}{"name": "hello world"}},
		}
		res := h.patchObject(req, nil)
		if _, ok := res.(*objects.ObjectsClassPatchNoContent); !ok {
			t.Errorf("unexpected result %v", res)
		}
		fakeManager.patchObjectReturn = &uco.Error{Code: uco.StatusBadRequest}
		res = h.patchObject(req, nil)
		if _, ok := res.(*objects.ObjectsClassPatchUnprocessableEntity); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassPatchUnprocessableEntity{}, res)
		}
		fakeManager.patchObjectReturn = &uco.Error{Code: uco.StatusNotFound}
		res = h.patchObject(req, nil)
		if _, ok := res.(*objects.ObjectsClassPatchNotFound); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassPatchNotFound{}, res)
		}
		fakeManager.patchObjectReturn = &uco.Error{Code: uco.StatusForbidden}
		res = h.patchObject(req, nil)
		if _, ok := res.(*objects.ObjectsClassPatchForbidden); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassPatchForbidden{}, res)
		}
		fakeManager.patchObjectReturn = &uco.Error{Code: uco.StatusInternalServerError}
		res = h.patchObject(req, nil)
		if _, ok := res.(*objects.ObjectsClassPatchInternalServerError); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassPatchInternalServerError{}, res)
		}

		fakeManager.patchObjectReturn = nil
		res = h.patchObject(req, nil)
		if _, ok := res.(*objects.ObjectsClassPatchNoContent); !ok {
			t.Errorf("unexpected result %v", res)
		}
	})

	t.Run("get object", func(t *testing.T) {
		cls := "MyClass"
		type test struct {
			name           string
			object         *models.Object
			err            error
			expectedResult *models.Object
		}

		tests := []test{
			{
				name:           "without props - noaction changes",
				object:         &models.Object{Class: cls, Properties: nil},
				expectedResult: &models.Object{Class: cls, Properties: nil},
			},
			{
				name: "without ref props - noaction changes",
				object: &models.Object{Class: cls, Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Object{Class: cls, Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			{
				name: "with a ref prop - no origin configured",
				object: &models.Object{Class: cls, Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Object{Class: cls, Properties: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/objects/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
			{
				name: "error forbbiden",
				err:  errors.NewForbidden(&models.Principal{}, "get", "Myclass/123"),
			},
			{
				name: "use case err not found",
				err:  uco.ErrNotFound{},
			},
			{
				name: "any other error",
				err:  stderrors.New("unknown error"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fakeManager := &fakeManager{
					getObjectReturn: test.object,
					getObjectErr:    test.err,
				}
				h := &objectHandlers{manager: fakeManager}
				req := objects.ObjectsClassGetParams{
					HTTPRequest: httptest.NewRequest("GET", "/v1/objects/MyClass/123", nil),
					ClassName:   cls,
					ID:          "123",
				}
				res := h.getObject(req, nil)
				parsed, ok := res.(*objects.ObjectsClassGetOK)
				if test.err != nil {
					require.False(t, ok)
					return
				}
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	t.Run("delete object", func(t *testing.T) {
		cls := "MyClass"
		type test struct {
			name string
			err  error
		}

		tests := []test{
			{
				name: "without props - noaction changes",
			},
			{
				name: "error forbbiden",
				err:  errors.NewForbidden(&models.Principal{}, "get", "Myclass/123"),
			},
			{
				name: "use case err not found",
				err:  uco.ErrNotFound{},
			},
			{
				name: "unknown error",
				err:  stderrors.New("any error"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fakeManager := &fakeManager{
					deleteObjectReturn: test.err,
				}
				h := &objectHandlers{manager: fakeManager}
				req := objects.ObjectsClassDeleteParams{
					HTTPRequest: httptest.NewRequest("GET", "/v1/objects/MyClass/123", nil),
					ClassName:   cls,
					ID:          "123",
				}
				res := h.deleteObject(req, nil)
				_, ok := res.(*objects.ObjectsClassDeleteNoContent)
				if test.err != nil {
					require.False(t, ok)
					return
				}
				require.True(t, ok)
			})
		}
	})

	t.Run("head object", func(t *testing.T) {
		m := &fakeManager{
			headObjectReturn: true,
		}
		h := &objectHandlers{manager: m}
		req := objects.ObjectsClassHeadParams{
			HTTPRequest: httptest.NewRequest("HEAD", "/v1/objects/MyClass/123", nil),
			ClassName:   "MyClass",
			ID:          "123",
		}
		res := h.headObject(req, nil)
		if _, ok := res.(*objects.ObjectsClassHeadNoContent); !ok {
			t.Errorf("unexpected result %v", res)
		}

		m.headObjectErr = &uco.Error{Code: uco.StatusForbidden}
		res = h.headObject(req, nil)
		if _, ok := res.(*objects.ObjectsClassHeadForbidden); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassHeadForbidden{}, res)
		}
		m.headObjectErr = &uco.Error{Code: uco.StatusInternalServerError}
		res = h.headObject(req, nil)
		if _, ok := res.(*objects.ObjectsClassHeadInternalServerError); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassHeadInternalServerError{}, res)
		}
		m.headObjectErr = nil
		m.headObjectReturn = false
		res = h.headObject(req, nil)
		if _, ok := res.(*objects.ObjectsClassHeadNotFound); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassHeadNotFound{}, res)
		}
		// same test as before but using old request
		oldRequest := objects.ObjectsHeadParams{HTTPRequest: req.HTTPRequest}
		res = h.headObjectDeprecated(oldRequest, nil)
		if _, ok := res.(*objects.ObjectsClassHeadNotFound); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassHeadNotFound{}, res)
		}
	})
}

type fakeManager struct {
	getObjectReturn *models.Object
	getObjectErr    error

	addObjectReturn    *models.Object
	getObjectsReturn   []*models.Object
	updateObjectReturn *models.Object
	updateObjectErr    error
	deleteObjectReturn error
	patchObjectReturn  *uco.Error
	headObjectReturn   bool
	headObjectErr      *uco.Error
}

func (f *fakeManager) HeadObject(context.Context, *models.Principal, string, strfmt.UUID) (bool, *uco.Error) {
	return f.headObjectReturn, f.headObjectErr
}

func (f *fakeManager) AddObject(_ context.Context, _ *models.Principal, object *models.Object) (*models.Object, error) {
	return object, nil
}

func (f *fakeManager) ValidateObject(_ context.Context, _ *models.Principal, _ *models.Object) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) GetObject(_ context.Context, _ *models.Principal, class string, _ strfmt.UUID, _ additional.Properties) (*models.Object, error) {
	return f.getObjectReturn, f.getObjectErr
}

func (f *fakeManager) GetObjectsClass(ctx context.Context, principal *models.Principal, id strfmt.UUID) (*models.Class, error) {
	class := &models.Class{
		Class:      f.getObjectReturn.Class,
		Vectorizer: "text2vec-contextionary",
	}
	return class, nil
}

func (f *fakeManager) GetObjects(_ context.Context, _ *models.Principal, _ *int64, _ *int64, _ *string, _ *string, _ additional.Properties) ([]*models.Object, error) {
	return f.getObjectsReturn, nil
}

func (f *fakeManager) UpdateObject(_ context.Context, _ *models.Principal, class string, _ strfmt.UUID, updates *models.Object) (*models.Object, error) {
	return updates, f.updateObjectErr
}

func (f *fakeManager) MergeObject(_ context.Context, _ *models.Principal, _ *models.Object) *uco.Error {
	return f.patchObjectReturn
}

func (f *fakeManager) DeleteObject(_ context.Context, _ *models.Principal, class string, _ strfmt.UUID) error {
	return f.deleteObjectReturn
}

func (f *fakeManager) AddObjectReference(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ string, _ *models.SingleRef) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) UpdateObjectReferences(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ string, _ models.MultipleRef) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) DeleteObjectReference(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ string, _ *models.SingleRef) error {
	panic("not implemented") // TODO: Implement
}
