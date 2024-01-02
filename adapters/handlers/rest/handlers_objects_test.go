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

package rest

import (
	"context"
	stderrors "errors"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/objects"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config"
	uco "github.com/weaviate/weaviate/usecases/objects"

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
				h := &objectHandlers{manager: fakeManager, metricRequestsTotal: &fakeMetricRequestsTotal{}}
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

	// This test "with an origin configured" is not repeated for every handler,
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
				h := &objectHandlers{manager: fakeManager, config: config, metricRequestsTotal: &fakeMetricRequestsTotal{}}
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
				h := &objectHandlers{manager: fakeManager, logger: &logrus.Logger{}, metricRequestsTotal: &fakeMetricRequestsTotal{}}
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
					queryResult: test.object,
				}
				h := &objectHandlers{manager: fakeManager, metricRequestsTotal: &fakeMetricRequestsTotal{}}
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
				h := &objectHandlers{manager: fakeManager, logger: &logrus.Logger{}, metricRequestsTotal: &fakeMetricRequestsTotal{}}
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
				h := &objectHandlers{manager: fakeManager, metricRequestsTotal: &fakeMetricRequestsTotal{}}
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
					queryResult: test.object,
				}
				h := &objectHandlers{manager: fakeManager, metricRequestsTotal: &fakeMetricRequestsTotal{}}
				res := h.getObjects(objects.ObjectsListParams{HTTPRequest: httptest.NewRequest("GET", "/v1/objects", nil)}, nil)
				parsed, ok := res.(*objects.ObjectsListOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload.Objects)
			})
		}
	})

	// New endpoints which uniquely identify objects of a class
	t.Run("UpdateObject", func(t *testing.T) {
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
				name: "forbidden",
				err:  errors.NewForbidden(&models.Principal{}, "get", "Myclass/123"),
			},
			{
				name: "validation",
				err:  uco.ErrInvalidUserInput{},
			},
			{
				name: "not found",
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
					updateObjectReturn: test.object,
					updateObjectErr:    test.err,
				}
				h := &objectHandlers{manager: fakeManager, metricRequestsTotal: &fakeMetricRequestsTotal{}}
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

	t.Run("PatchObject", func(t *testing.T) {
		var (
			fakeManager             = &fakeManager{}
			fakeMetricRequestsTotal = &fakeMetricRequestsTotal{}
			h                       = &objectHandlers{
				manager:             fakeManager,
				logger:              &logrus.Logger{},
				metricRequestsTotal: fakeMetricRequestsTotal,
			}
			req = objects.ObjectsClassPatchParams{
				HTTPRequest: httptest.NewRequest("PATCH", "/v1/objects/MyClass/123", nil),
				ClassName:   "MyClass",
				ID:          "123",
				Body:        &models.Object{Properties: map[string]interface{}{"name": "hello world"}},
			}
		)
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

		// test deprecated function
		fakeManager.patchObjectReturn = nil
		res = h.patchObjectDeprecated(objects.ObjectsPatchParams{
			HTTPRequest: httptest.NewRequest("PATCH", "/v1/objects/123", nil),
			ID:          "123",
			Body: &models.Object{
				Class:      "MyClass",
				Properties: map[string]interface{}{"name": "hello world"},
			},
		}, nil)
		if _, ok := res.(*objects.ObjectsClassPatchNoContent); !ok {
			t.Errorf("unexpected result %v", res)
		}
	})

	t.Run("GetObject", func(t *testing.T) {
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
				name: "error forbidden",
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
				h := &objectHandlers{manager: fakeManager, metricRequestsTotal: &fakeMetricRequestsTotal{}}
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

	t.Run("DeleteObject", func(t *testing.T) {
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
				name: "error forbidden",
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
				h := &objectHandlers{manager: fakeManager, metricRequestsTotal: &fakeMetricRequestsTotal{}}
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

	t.Run("HeadObject", func(t *testing.T) {
		m := &fakeManager{
			headObjectReturn: true,
		}
		h := &objectHandlers{manager: m, logger: &logrus.Logger{}, metricRequestsTotal: &fakeMetricRequestsTotal{}}
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

	t.Run("PostReference", func(t *testing.T) {
		m := &fakeManager{}
		h := &objectHandlers{manager: m, logger: &logrus.Logger{}, metricRequestsTotal: &fakeMetricRequestsTotal{}}
		req := objects.ObjectsClassReferencesCreateParams{
			HTTPRequest:  httptest.NewRequest("HEAD", "/v1/objects/MyClass/123/references/prop", nil),
			ClassName:    "MyClass",
			ID:           "123",
			Body:         new(models.SingleRef),
			PropertyName: "prop",
		}
		res := h.addObjectReference(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesCreateOK); !ok {
			t.Errorf("unexpected result %v", res)
		}

		m.addRefErr = &uco.Error{Code: uco.StatusForbidden}
		res = h.addObjectReference(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesCreateForbidden); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesCreateForbidden{}, res)
		}
		// source object not found
		m.addRefErr = &uco.Error{Code: uco.StatusNotFound}
		res = h.addObjectReference(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesCreateNotFound); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesCreateNotFound{}, res)
		}

		m.addRefErr = &uco.Error{Code: uco.StatusInternalServerError}
		res = h.addObjectReference(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesCreateInternalServerError); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesCreateInternalServerError{}, res)
		}
		m.addRefErr = &uco.Error{Code: uco.StatusBadRequest}
		res = h.addObjectReference(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesCreateUnprocessableEntity); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesCreateUnprocessableEntity{}, res)
		}
		// same test as before but using old request
		oldRequest := objects.ObjectsReferencesCreateParams{
			HTTPRequest:  req.HTTPRequest,
			Body:         req.Body,
			ID:           req.ID,
			PropertyName: req.ClassName,
		}
		res = h.addObjectReferenceDeprecated(oldRequest, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesCreateUnprocessableEntity); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesCreateUnprocessableEntity{}, res)
		}
	})

	t.Run("PutReferences", func(t *testing.T) {
		m := &fakeManager{}
		h := &objectHandlers{manager: m, logger: &logrus.Logger{}, metricRequestsTotal: &fakeMetricRequestsTotal{}}
		req := objects.ObjectsClassReferencesPutParams{
			HTTPRequest:  httptest.NewRequest("HEAD", "/v1/objects/MyClass/123/references/prop", nil),
			ClassName:    "MyClass",
			ID:           "123",
			Body:         models.MultipleRef{},
			PropertyName: "prop",
		}
		res := h.putObjectReferences(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesPutOK); !ok {
			t.Errorf("unexpected result %v", res)
		}

		m.putRefErr = &uco.Error{Code: uco.StatusForbidden}
		res = h.putObjectReferences(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesPutForbidden); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesPutForbidden{}, res)
		}
		m.putRefErr = &uco.Error{Code: uco.StatusInternalServerError}
		res = h.putObjectReferences(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesPutInternalServerError); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesPutInternalServerError{}, res)
		}
		m.putRefErr = &uco.Error{Code: uco.StatusBadRequest}
		res = h.putObjectReferences(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesPutUnprocessableEntity); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesPutUnprocessableEntity{}, res)
		}
		// same test as before but using old request
		oldRequest := objects.ObjectsReferencesUpdateParams{
			HTTPRequest:  req.HTTPRequest,
			Body:         req.Body,
			ID:           req.ID,
			PropertyName: req.ClassName,
		}
		res = h.updateObjectReferencesDeprecated(oldRequest, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesPutUnprocessableEntity); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesPutUnprocessableEntity{}, res)
		}
	})

	t.Run("DeleteReference", func(t *testing.T) {
		m := &fakeManager{}
		h := &objectHandlers{manager: m, logger: &logrus.Logger{}, metricRequestsTotal: &fakeMetricRequestsTotal{}}
		req := objects.ObjectsClassReferencesDeleteParams{
			HTTPRequest:  httptest.NewRequest("HEAD", "/v1/objects/MyClass/123/references/prop", nil),
			ClassName:    "MyClass",
			ID:           "123",
			Body:         new(models.SingleRef),
			PropertyName: "prop",
		}
		res := h.deleteObjectReference(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesDeleteNoContent); !ok {
			t.Errorf("unexpected result %v", res)
		}

		m.deleteRefErr = &uco.Error{Code: uco.StatusForbidden}
		res = h.deleteObjectReference(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesDeleteForbidden); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesDeleteForbidden{}, res)
		}
		// source object not found
		m.deleteRefErr = &uco.Error{Code: uco.StatusNotFound}
		res = h.deleteObjectReference(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesDeleteNotFound); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesDeleteNotFound{}, res)
		}

		m.deleteRefErr = &uco.Error{Code: uco.StatusInternalServerError}
		res = h.deleteObjectReference(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesDeleteInternalServerError); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesDeleteInternalServerError{}, res)
		}
		m.deleteRefErr = &uco.Error{Code: uco.StatusBadRequest}
		res = h.deleteObjectReference(req, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesDeleteUnprocessableEntity); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesDeleteUnprocessableEntity{}, res)
		}
		// same test as before but using old request
		oldRequest := objects.ObjectsReferencesDeleteParams{
			HTTPRequest:  req.HTTPRequest,
			Body:         req.Body,
			ID:           req.ID,
			PropertyName: req.ClassName,
		}
		res = h.deleteObjectReferenceDeprecated(oldRequest, nil)
		if _, ok := res.(*objects.ObjectsClassReferencesDeleteUnprocessableEntity); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsClassReferencesDeleteUnprocessableEntity{}, res)
		}
	})

	t.Run("Query", func(t *testing.T) {
		var (
			cls = "MyClass"
			m   = &fakeManager{
				queryErr: nil,
				queryResult: []*models.Object{{
					Properties: map[string]interface{}{"name": "John"},
				}},
			}
			fakeMetricRequestsTotal = &fakeMetricRequestsTotal{}
			h                       = &objectHandlers{
				manager:             m,
				logger:              &logrus.Logger{},
				metricRequestsTotal: fakeMetricRequestsTotal,
			}
			req = objects.ObjectsListParams{
				HTTPRequest: httptest.NewRequest("HEAD", "/v1/objects/", nil),
				Class:       &cls,
			}
		)

		res := h.query(req, nil)
		if _, ok := res.(*objects.ObjectsListOK); !ok {
			t.Errorf("unexpected result %v", res)
		}

		m.queryErr = &uco.Error{Code: uco.StatusForbidden}
		res = h.query(req, nil)
		if _, ok := res.(*objects.ObjectsListForbidden); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsListForbidden{}, res)
		}
		m.queryErr = &uco.Error{Code: uco.StatusNotFound}
		res = h.query(req, nil)
		if _, ok := res.(*objects.ObjectsListNotFound); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsListNotFound{}, res)
		}
		m.queryErr = &uco.Error{Code: uco.StatusBadRequest}
		res = h.query(req, nil)
		if _, ok := res.(*objects.ObjectsListUnprocessableEntity); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsListUnprocessableEntity{}, res)
		}
		m.queryErr = &uco.Error{Code: uco.StatusInternalServerError}
		res = h.query(req, nil)
		if _, ok := res.(*objects.ObjectsListInternalServerError); !ok {
			t.Errorf("expected: %T got: %T", objects.ObjectsListInternalServerError{}, res)
		}
	})
}

type fakeManager struct {
	getObjectReturn *models.Object
	getObjectErr    error

	addObjectReturn    *models.Object
	queryResult        []*models.Object
	queryErr           *uco.Error
	updateObjectReturn *models.Object
	updateObjectErr    error
	deleteObjectReturn error
	patchObjectReturn  *uco.Error
	headObjectReturn   bool
	headObjectErr      *uco.Error
	addRefErr          *uco.Error
	putRefErr          *uco.Error
	deleteRefErr       *uco.Error
}

func (f *fakeManager) HeadObject(context.Context, *models.Principal,
	string, strfmt.UUID, *additional.ReplicationProperties, string,
) (bool, *uco.Error) {
	return f.headObjectReturn, f.headObjectErr
}

func (f *fakeManager) AddObject(_ context.Context, _ *models.Principal,
	object *models.Object, _ *additional.ReplicationProperties,
) (*models.Object, error) {
	return object, nil
}

func (f *fakeManager) ValidateObject(_ context.Context, _ *models.Principal,
	_ *models.Object, _ *additional.ReplicationProperties,
) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) GetObject(_ context.Context, _ *models.Principal, class string,
	_ strfmt.UUID, _ additional.Properties, _ *additional.ReplicationProperties, _ string,
) (*models.Object, error) {
	return f.getObjectReturn, f.getObjectErr
}

func (f *fakeManager) GetObjectsClass(ctx context.Context,
	principal *models.Principal, id strfmt.UUID,
) (*models.Class, error) {
	class := &models.Class{
		Class:      f.getObjectReturn.Class,
		Vectorizer: "text2vec-contextionary",
	}
	return class, nil
}

func (f *fakeManager) GetObjectClassFromName(ctx context.Context, principal *models.Principal,
	className string,
) (*models.Class, error) {
	class := &models.Class{
		Class:      f.getObjectReturn.Class,
		Vectorizer: "text2vec-contextionary",
	}
	return class, nil
}

func (f *fakeManager) GetObjects(ctx context.Context, principal *models.Principal, offset *int64, limit *int64, sort *string, order *string, after *string, addl additional.Properties, tenant string) ([]*models.Object, error) {
	return f.queryResult, nil
}

func (f *fakeManager) Query(_ context.Context,
	_ *models.Principal, _ *uco.QueryParams,
) ([]*models.Object, *uco.Error) {
	return f.queryResult, f.queryErr
}

func (f *fakeManager) UpdateObject(_ context.Context, _ *models.Principal, _ string,
	_ strfmt.UUID, updates *models.Object, _ *additional.ReplicationProperties,
) (*models.Object, error) {
	return updates, f.updateObjectErr
}

func (f *fakeManager) MergeObject(_ context.Context, _ *models.Principal,
	_ *models.Object, _ *additional.ReplicationProperties,
) *uco.Error {
	return f.patchObjectReturn
}

func (f *fakeManager) DeleteObject(_ context.Context, _ *models.Principal,
	class string, _ strfmt.UUID, _ *additional.ReplicationProperties, _ string,
) error {
	return f.deleteObjectReturn
}

func (f *fakeManager) AddObjectReference(context.Context, *models.Principal,
	*uco.AddReferenceInput, *additional.ReplicationProperties, string,
) *uco.Error {
	return f.addRefErr
}

func (f *fakeManager) UpdateObjectReferences(context.Context, *models.Principal,
	*uco.PutReferenceInput, *additional.ReplicationProperties, string,
) *uco.Error {
	return f.putRefErr
}

func (f *fakeManager) DeleteObjectReference(context.Context, *models.Principal,
	*uco.DeleteReferenceInput, *additional.ReplicationProperties, string,
) *uco.Error {
	return f.deleteRefErr
}

type fakeMetricRequestsTotal struct{}

func (f *fakeMetricRequestsTotal) logError(className string, err error)       {}
func (f *fakeMetricRequestsTotal) logOk(className string)                     {}
func (f *fakeMetricRequestsTotal) logUserError(className string)              {}
func (f *fakeMetricRequestsTotal) logServerError(className string, err error) {}
