package rest

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/actions"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnrichObjectsWithLinks(t *testing.T) {
	t.Run("add thing", func(t *testing.T) {
		type test struct {
			name           string
			thing          *models.Thing
			expectedResult *models.Thing
		}

		tests := []test{
			test{
				name:           "without props - nothing changes",
				thing:          &models.Thing{Class: "Foo", Schema: nil},
				expectedResult: &models.Thing{Class: "Foo", Schema: nil},
			},
			test{
				name: "without ref props - nothing changes",
				thing: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			test{
				name: "with a ref prop - no origin configured",
				thing: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/things/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				fakeManager := &fakeManager{
					addThingReturn: test.thing,
				}
				fakeRequestLog := &fakeRequestLog{}
				h := &kindHandlers{manager: fakeManager, requestsLog: fakeRequestLog}
				res := h.addThing(things.ThingsCreateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/things", nil),
					Body:        test.thing,
				}, nil)
				parsed, ok := res.(*things.ThingsCreateOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	// This test "with an origin conifgured" is not repeated for every handler,
	// as testing this feature once was deemed sufficient
	t.Run("add thing - with an origin configured", func(t *testing.T) {
		type test struct {
			name           string
			thing          *models.Thing
			expectedResult *models.Thing
		}

		tests := []test{
			test{
				name:           "without props - nothing changes",
				thing:          &models.Thing{Class: "Foo", Schema: nil},
				expectedResult: &models.Thing{Class: "Foo", Schema: nil},
			},
			test{
				name: "without ref props - nothing changes",
				thing: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			test{
				name: "with a ref prop - no origin configured",
				thing: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "https://awesomehost.com/v1/things/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				fakeManager := &fakeManager{
					addThingReturn: test.thing,
				}
				fakeRequestLog := &fakeRequestLog{}
				config := config.Config{Origin: "https://awesomehost.com"}
				h := &kindHandlers{manager: fakeManager, requestsLog: fakeRequestLog, config: config}
				res := h.addThing(things.ThingsCreateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/things", nil),
					Body:        test.thing,
				}, nil)
				parsed, ok := res.(*things.ThingsCreateOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	t.Run("get thing", func(t *testing.T) {
		type test struct {
			name           string
			thing          *models.Thing
			expectedResult *models.Thing
		}

		tests := []test{
			test{
				name:           "without props - nothing changes",
				thing:          &models.Thing{Class: "Foo", Schema: nil},
				expectedResult: &models.Thing{Class: "Foo", Schema: nil},
			},
			test{
				name: "without ref props - nothing changes",
				thing: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			test{
				name: "with a ref prop - no origin configured",
				thing: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/things/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				fakeManager := &fakeManager{
					getThingReturn: test.thing,
				}
				fakeRequestLog := &fakeRequestLog{}
				h := &kindHandlers{manager: fakeManager, requestsLog: fakeRequestLog}
				res := h.getThing(things.ThingsGetParams{HTTPRequest: httptest.NewRequest("GET", "/v1/things", nil)}, nil)
				parsed, ok := res.(*things.ThingsGetOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	t.Run("get things", func(t *testing.T) {
		type test struct {
			name           string
			thing          []*models.Thing
			expectedResult []*models.Thing
		}

		tests := []test{
			test{
				name:           "without props - nothing changes",
				thing:          []*models.Thing{&models.Thing{Class: "Foo", Schema: nil}},
				expectedResult: []*models.Thing{&models.Thing{Class: "Foo", Schema: nil}},
			},
			test{
				name: "without ref props - nothing changes",
				thing: []*models.Thing{
					&models.Thing{Class: "Foo", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Thing{Class: "Bar", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
				expectedResult: []*models.Thing{
					&models.Thing{Class: "Foo", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Thing{Class: "Bar", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
			},
			test{
				name: "with a ref prop - no origin configured",
				thing: []*models.Thing{
					&models.Thing{Class: "Foo", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Thing{Class: "Bar", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
						"someRef": models.MultipleRef{
							&models.SingleRef{
								Beacon: "weaviate://localhost/things/85f78e29-5937-4390-a121-5379f262b4e5",
							},
						},
					}},
				},
				expectedResult: []*models.Thing{
					&models.Thing{Class: "Foo", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Thing{Class: "Bar", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
						"someRef": models.MultipleRef{
							&models.SingleRef{
								Beacon: "weaviate://localhost/things/85f78e29-5937-4390-a121-5379f262b4e5",
								Href:   "/v1/things/85f78e29-5937-4390-a121-5379f262b4e5",
							},
						},
					}},
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				fakeManager := &fakeManager{
					getThingsReturn: test.thing,
				}
				fakeRequestLog := &fakeRequestLog{}
				h := &kindHandlers{manager: fakeManager, requestsLog: fakeRequestLog}
				res := h.getThings(things.ThingsListParams{HTTPRequest: httptest.NewRequest("GET", "/v1/things", nil)}, nil)
				parsed, ok := res.(*things.ThingsListOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload.Things)
			})
		}
	})

	t.Run("update thing", func(t *testing.T) {
		type test struct {
			name           string
			thing          *models.Thing
			expectedResult *models.Thing
		}

		tests := []test{
			test{
				name:           "without props - nothing changes",
				thing:          &models.Thing{Class: "Foo", Schema: nil},
				expectedResult: &models.Thing{Class: "Foo", Schema: nil},
			},
			test{
				name: "without ref props - nothing changes",
				thing: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			test{
				name: "with a ref prop - no origin configured",
				thing: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Thing{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/things/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/things/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				fakeManager := &fakeManager{
					updateThingReturn: test.thing,
				}
				fakeRequestLog := &fakeRequestLog{}
				h := &kindHandlers{manager: fakeManager, requestsLog: fakeRequestLog}
				res := h.updateThing(things.ThingsUpdateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/things", nil),
					Body:        test.thing,
				}, nil)
				parsed, ok := res.(*things.ThingsUpdateOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	t.Run("add action", func(t *testing.T) {
		type test struct {
			name           string
			action         *models.Action
			expectedResult *models.Action
		}

		tests := []test{
			test{
				name:           "without props - noaction changes",
				action:         &models.Action{Class: "Foo", Schema: nil},
				expectedResult: &models.Action{Class: "Foo", Schema: nil},
			},
			test{
				name: "without ref props - noaction changes",
				action: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			test{
				name: "with a ref prop - no origin configured",
				action: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/actions/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/actions/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/actions/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				fakeManager := &fakeManager{
					addActionReturn: test.action,
				}
				fakeRequestLog := &fakeRequestLog{}
				h := &kindHandlers{manager: fakeManager, requestsLog: fakeRequestLog}
				res := h.addAction(actions.ActionsCreateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/actions", nil),
					Body:        test.action,
				}, nil)
				parsed, ok := res.(*actions.ActionsCreateOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	t.Run("get action", func(t *testing.T) {
		type test struct {
			name           string
			action         *models.Action
			expectedResult *models.Action
		}

		tests := []test{
			test{
				name:           "without props - noaction changes",
				action:         &models.Action{Class: "Foo", Schema: nil},
				expectedResult: &models.Action{Class: "Foo", Schema: nil},
			},
			test{
				name: "without ref props - noaction changes",
				action: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			test{
				name: "with a ref prop - no origin configured",
				action: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/actions/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/actions/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/actions/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				fakeManager := &fakeManager{
					getActionReturn: test.action,
				}
				fakeRequestLog := &fakeRequestLog{}
				h := &kindHandlers{manager: fakeManager, requestsLog: fakeRequestLog}
				res := h.getAction(actions.ActionsGetParams{HTTPRequest: httptest.NewRequest("GET", "/v1/actions", nil)}, nil)
				parsed, ok := res.(*actions.ActionsGetOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

	t.Run("get actions", func(t *testing.T) {
		type test struct {
			name           string
			action         []*models.Action
			expectedResult []*models.Action
		}

		tests := []test{
			test{
				name:           "without props - noaction changes",
				action:         []*models.Action{&models.Action{Class: "Foo", Schema: nil}},
				expectedResult: []*models.Action{&models.Action{Class: "Foo", Schema: nil}},
			},
			test{
				name: "without ref props - noaction changes",
				action: []*models.Action{
					&models.Action{Class: "Foo", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Action{Class: "Bar", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
				expectedResult: []*models.Action{
					&models.Action{Class: "Foo", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Action{Class: "Bar", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
					}},
				},
			},
			test{
				name: "with a ref prop - no origin configured",
				action: []*models.Action{
					&models.Action{Class: "Foo", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Action{Class: "Bar", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
						"someRef": models.MultipleRef{
							&models.SingleRef{
								Beacon: "weaviate://localhost/actions/85f78e29-5937-4390-a121-5379f262b4e5",
							},
						},
					}},
				},
				expectedResult: []*models.Action{
					&models.Action{Class: "Foo", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 134,
					}},
					&models.Action{Class: "Bar", Schema: map[string]interface{}{
						"name":           "hello world",
						"numericalField": 234,
						"someRef": models.MultipleRef{
							&models.SingleRef{
								Beacon: "weaviate://localhost/actions/85f78e29-5937-4390-a121-5379f262b4e5",
								Href:   "/v1/actions/85f78e29-5937-4390-a121-5379f262b4e5",
							},
						},
					}},
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				fakeManager := &fakeManager{
					getActionsReturn: test.action,
				}
				fakeRequestLog := &fakeRequestLog{}
				h := &kindHandlers{manager: fakeManager, requestsLog: fakeRequestLog}
				res := h.getActions(actions.ActionsListParams{HTTPRequest: httptest.NewRequest("GET", "/v1/actions", nil)}, nil)
				parsed, ok := res.(*actions.ActionsListOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload.Actions)
			})
		}
	})

	t.Run("update action", func(t *testing.T) {
		type test struct {
			name           string
			action         *models.Action
			expectedResult *models.Action
		}

		tests := []test{
			test{
				name:           "without props - noaction changes",
				action:         &models.Action{Class: "Foo", Schema: nil},
				expectedResult: &models.Action{Class: "Foo", Schema: nil},
			},
			test{
				name: "without ref props - noaction changes",
				action: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
				expectedResult: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
				}},
			},
			test{
				name: "with a ref prop - no origin configured",
				action: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/actions/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
				expectedResult: &models.Action{Class: "Foo", Schema: map[string]interface{}{
					"name":           "hello world",
					"numericalField": 134,
					"someRef": models.MultipleRef{
						&models.SingleRef{
							Beacon: "weaviate://localhost/actions/85f78e29-5937-4390-a121-5379f262b4e5",
							Href:   "/v1/actions/85f78e29-5937-4390-a121-5379f262b4e5",
						},
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				fakeManager := &fakeManager{
					updateActionReturn: test.action,
				}
				fakeRequestLog := &fakeRequestLog{}
				h := &kindHandlers{manager: fakeManager, requestsLog: fakeRequestLog}
				res := h.updateAction(actions.ActionsUpdateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/actions", nil),
					Body:        test.action,
				}, nil)
				parsed, ok := res.(*actions.ActionsUpdateOK)
				require.True(t, ok)
				assert.Equal(t, test.expectedResult, parsed.Payload)
			})
		}
	})

}

type fakeManager struct {
	getThingReturn     *models.Thing
	getActionReturn    *models.Action
	addThingReturn     *models.Thing
	addActionReturn    *models.Action
	getThingsReturn    []*models.Thing
	getActionsReturn   []*models.Action
	updateThingReturn  *models.Thing
	updateActionReturn *models.Action
}

func (f *fakeManager) AddThing(_ context.Context, _ *models.Principal, thing *models.Thing) (*models.Thing, error) {
	return thing, nil
}

func (f *fakeManager) AddAction(_ context.Context, _ *models.Principal, action *models.Action) (*models.Action, error) {
	return action, nil
}

func (f *fakeManager) ValidateThing(_ context.Context, _ *models.Principal, _ *models.Thing) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) ValidateAction(_ context.Context, _ *models.Principal, _ *models.Action) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) GetThing(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ bool) (*models.Thing, error) {
	return f.getThingReturn, nil
}

func (f *fakeManager) GetAction(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ bool) (*models.Action, error) {
	return f.getActionReturn, nil
}

func (f *fakeManager) GetThings(_ context.Context, _ *models.Principal, _ *int64, _ bool) ([]*models.Thing, error) {
	return f.getThingsReturn, nil
}

func (f *fakeManager) GetActions(_ context.Context, _ *models.Principal, _ *int64, _ bool) ([]*models.Action, error) {
	return f.getActionsReturn, nil
}

func (f *fakeManager) UpdateThing(_ context.Context, _ *models.Principal, _ strfmt.UUID, thing *models.Thing) (*models.Thing, error) {
	return thing, nil
}

func (f *fakeManager) UpdateAction(_ context.Context, _ *models.Principal, _ strfmt.UUID, action *models.Action) (*models.Action, error) {
	return action, nil
}

func (f *fakeManager) MergeThing(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ *models.Thing) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) MergeAction(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ *models.Action) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) DeleteThing(_ context.Context, _ *models.Principal, _ strfmt.UUID) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) DeleteAction(_ context.Context, _ *models.Principal, _ strfmt.UUID) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) AddThingReference(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ string, _ *models.SingleRef) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) AddActionReference(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ string, _ *models.SingleRef) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) UpdateThingReferences(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ string, _ models.MultipleRef) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) UpdateActionReferences(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ string, _ models.MultipleRef) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) DeleteThingReference(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ string, _ *models.SingleRef) error {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) DeleteActionReference(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ string, _ *models.SingleRef) error {
	panic("not implemented") // TODO: Implement
}

type fakeRequestLog struct{}

func (f *fakeRequestLog) Register(_ string, _ string) {}
