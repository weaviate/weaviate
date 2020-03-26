package rest

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnrichObjectsWithLinks(t *testing.T) {
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

}

type fakeManager struct {
	getThingReturn *models.Thing
}

func (f *fakeManager) AddThing(_ context.Context, _ *models.Principal, _ *models.Thing) (*models.Thing, error) {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) AddAction(_ context.Context, _ *models.Principal, _ *models.Action) (*models.Action, error) {
	panic("not implemented") // TODO: Implement
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
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) GetThings(_ context.Context, _ *models.Principal, _ *int64, _ bool) ([]*models.Thing, error) {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) GetActions(_ context.Context, _ *models.Principal, _ *int64, _ bool) ([]*models.Action, error) {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) UpdateThing(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ *models.Thing) (*models.Thing, error) {
	panic("not implemented") // TODO: Implement
}

func (f *fakeManager) UpdateAction(_ context.Context, _ *models.Principal, _ strfmt.UUID, _ *models.Action) (*models.Action, error) {
	panic("not implemented") // TODO: Implement
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
