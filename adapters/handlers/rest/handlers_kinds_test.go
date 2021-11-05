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

package rest

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/objects"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/config"
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
			test{
				name:           "without props - nothing changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			test{
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
			test{
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
				h := &kindHandlers{manager: fakeManager}
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
			test{
				name:           "without props - nothing changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			test{
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
			test{
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
				h := &kindHandlers{manager: fakeManager, config: config}
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

	t.Run("get object", func(t *testing.T) {
		type test struct {
			name           string
			object         *models.Object
			expectedResult *models.Object
		}

		tests := []test{
			test{
				name:           "without props - nothing changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			test{
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
			test{
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
				h := &kindHandlers{manager: fakeManager}
				res := h.getObject(objects.ObjectsGetParams{HTTPRequest: httptest.NewRequest("GET", "/v1/objects", nil)}, nil)
				parsed, ok := res.(*objects.ObjectsGetOK)
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
			test{
				name:           "without props - nothing changes",
				object:         []*models.Object{&models.Object{Class: "Foo", Properties: nil}},
				expectedResult: []*models.Object{&models.Object{Class: "Foo", Properties: nil}},
			},
			test{
				name: "without ref props - nothing changes",
				object: []*models.Object{
					&models.Object{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Object{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
				expectedResult: []*models.Object{
					&models.Object{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Object{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
			},
			test{
				name: "with a ref prop - no origin configured",
				object: []*models.Object{
					&models.Object{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Object{Class: "Bar", Properties: map[string]interface{}{
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
					&models.Object{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Object{Class: "Bar", Properties: map[string]interface{}{
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
				h := &kindHandlers{manager: fakeManager}
				res := h.getObjects(objects.ObjectsListParams{HTTPRequest: httptest.NewRequest("GET", "/v1/objects", nil)}, nil)
				parsed, ok := res.(*objects.ObjectsListOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload.Objects)
			})
		}
	})

	t.Run("update object", func(t *testing.T) {
		type test struct {
			name           string
			object         *models.Object
			expectedResult *models.Object
		}

		tests := []test{
			test{
				name:           "without props - nothing changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			test{
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
			test{
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
				h := &kindHandlers{manager: fakeManager}
				res := h.updateObject(objects.ObjectsUpdateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/objects", nil),
					Body:        test.object,
				}, nil)
				parsed, ok := res.(*objects.ObjectsUpdateOK)
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
			test{
				name:           "without props - noaction changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			test{
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
			test{
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
				h := &kindHandlers{manager: fakeManager}
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

	t.Run("get object", func(t *testing.T) {
		type test struct {
			name           string
			object         *models.Object
			expectedResult *models.Object
		}

		tests := []test{
			test{
				name:           "without props - noaction changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			test{
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
			test{
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
				h := &kindHandlers{manager: fakeManager}
				res := h.getObject(objects.ObjectsGetParams{HTTPRequest: httptest.NewRequest("GET", "/v1/objects", nil)}, nil)
				parsed, ok := res.(*objects.ObjectsGetOK)
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
			test{
				name:           "without props - noaction changes",
				object:         []*models.Object{&models.Object{Class: "Foo", Properties: nil}},
				expectedResult: []*models.Object{&models.Object{Class: "Foo", Properties: nil}},
			},
			test{
				name: "without ref props - noaction changes",
				object: []*models.Object{
					&models.Object{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Object{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
				expectedResult: []*models.Object{
					&models.Object{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Object{Class: "Bar", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
			},
			test{
				name: "with a ref prop - no origin configured",
				object: []*models.Object{
					&models.Object{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Object{Class: "Bar", Properties: map[string]interface{}{
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
					&models.Object{Class: "Foo", Properties: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Object{Class: "Bar", Properties: map[string]interface{}{
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
				h := &kindHandlers{manager: fakeManager}
				res := h.getObjects(objects.ObjectsListParams{HTTPRequest: httptest.NewRequest("GET", "/v1/objects", nil)}, nil)
				parsed, ok := res.(*objects.ObjectsListOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload.Objects)
			})
		}
	})

	t.Run("update object", func(t *testing.T) {
		type test struct {
			name           string
			object         *models.Object
			expectedResult *models.Object
		}

		tests := []test{
			test{
				name:           "without props - noaction changes",
				object:         &models.Object{Class: "Foo", Properties: nil},
				expectedResult: &models.Object{Class: "Foo", Properties: nil},
			},
			test{
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
			test{
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
				h := &kindHandlers{manager: fakeManager}
				res := h.updateObject(objects.ObjectsUpdateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/objects", nil),
					Body:        test.object,
				}, nil)
				parsed, ok := res.(*objects.ObjectsUpdateOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})
}

type fakeManager struct {
	getObjectReturn    *models.Object
	addObjectReturn    *models.Object
	getObjectsReturn   []*models.Object
	updateObjectReturn *models.Object
}

func (f *fakeManager) AddObject(_ context.Context, _ *models.Principal, object *models.Object) (*models.Object, error) {
	return object, nil
}

func (f *fakeManager) ValidateObject(_ context.Context, _ *models.Principal, _ *models.Object) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) GetObject(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ additional.Properties) (*models.Object, error) {
	return f.getObjectReturn, nil
}

func (f *fakeManager) GetObjects(_ context.Context, _ *models.Principal, _ *int64, _ *int64, _ additional.Properties) ([]*models.Object, error) {
	return f.getObjectsReturn, nil
}

func (f *fakeManager) UpdateObject(_ context.Context, _ *models.Principal, _ strfmt.UUID, object *models.Object) (*models.Object, error) {
	return object, nil
}

func (f *fakeManager) MergeObject(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ *models.Object) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) DeleteObject(_ context.Context, _ *models.Principal, _ strfmt.UUID) error {
	panic("not implemented") // TODO: Implement
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
