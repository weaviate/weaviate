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

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/objects"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	uco "github.com/weaviate/weaviate/usecases/objects"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// conditionalFakeManager is a stub objectsManager that controls the outcome of
// AddObject for conditional write handler tests. All other methods panic so
// that accidental calls are surfaced immediately as test failures.
type conditionalFakeManager struct {
	addObjectReturn *models.Object
	addObjectErr    error
}

func (f *conditionalFakeManager) AddObject(_ context.Context, _ *models.Principal,
	obj *models.Object, _ *additional.ReplicationProperties,
) (*models.Object, error) {
	if f.addObjectReturn != nil {
		return f.addObjectReturn, f.addObjectErr
	}
	return obj, f.addObjectErr
}

func (f *conditionalFakeManager) ValidateObject(context.Context, *models.Principal,
	*models.Object, *additional.ReplicationProperties,
) error {
	panic("not implemented")
}

func (f *conditionalFakeManager) GetObject(context.Context, *models.Principal,
	string, strfmt.UUID, additional.Properties, *additional.ReplicationProperties, string,
) (*models.Object, error) {
	panic("not implemented")
}

func (f *conditionalFakeManager) DeleteObject(context.Context, *models.Principal,
	string, strfmt.UUID, *additional.ReplicationProperties, string,
) error {
	panic("not implemented")
}

func (f *conditionalFakeManager) UpdateObject(context.Context, *models.Principal,
	string, strfmt.UUID, *models.Object, *additional.ReplicationProperties,
) (*models.Object, error) {
	panic("not implemented")
}

func (f *conditionalFakeManager) HeadObject(context.Context, *models.Principal,
	string, strfmt.UUID, *additional.ReplicationProperties, string,
) (bool, *uco.Error) {
	panic("not implemented")
}

func (f *conditionalFakeManager) GetObjects(context.Context, *models.Principal,
	*int64, *int64, *string, *string, *string, additional.Properties, string,
) ([]*models.Object, error) {
	panic("not implemented")
}

func (f *conditionalFakeManager) Query(context.Context, *models.Principal,
	*uco.QueryParams,
) ([]*models.Object, *uco.Error) {
	panic("not implemented")
}

func (f *conditionalFakeManager) MergeObject(context.Context, *models.Principal,
	*models.Object, *additional.ReplicationProperties,
) *uco.Error {
	panic("not implemented")
}

func (f *conditionalFakeManager) AddObjectReference(context.Context, *models.Principal,
	*uco.AddReferenceInput, *additional.ReplicationProperties, string,
) *uco.Error {
	panic("not implemented")
}

func (f *conditionalFakeManager) UpdateObjectReferences(context.Context, *models.Principal,
	*uco.PutReferenceInput, *additional.ReplicationProperties, string,
) *uco.Error {
	panic("not implemented")
}

func (f *conditionalFakeManager) DeleteObjectReference(context.Context, *models.Principal,
	*uco.DeleteReferenceInput, *additional.ReplicationProperties, string,
) *uco.Error {
	panic("not implemented")
}

func (f *conditionalFakeManager) GetObjectsClass(context.Context, *models.Principal,
	strfmt.UUID,
) (*models.Class, error) {
	panic("not implemented")
}

func (f *conditionalFakeManager) GetObjectClassFromName(context.Context, *models.Principal,
	string,
) (*models.Class, error) {
	panic("not implemented")
}

// TestConditionalWriteRESTHandler verifies the three outcome paths of the
// addObject handler when conditional writes are active:
//
//  1. insert_if_not_exists on an EXISTING UUID: manager returns
//     ErrPreconditionFailed with the OnlyIfNotExists reason → handler must
//     return HTTP 200 with outcome=skipped.
//
//  2. insert_if_not_exists on a NEW UUID (no precondition failure): manager
//     returns (object, nil) with ?condition=insert_if_not_exists → handler
//     must return HTTP 201 with outcome=inserted.
//
//  3. A genuine precondition conflict (OnlyIfExists on a non-existent object):
//     manager returns ErrPreconditionFailed with a non-skip reason → handler
//     must return HTTP 409 Conflict.
func TestConditionalWriteRESTHandler(t *testing.T) {
	existingUUID := strfmt.UUID("00000000-0000-0000-0000-000000000001")
	newUUID := strfmt.UUID("00000000-0000-0000-0000-000000000002")

	t.Run("insert_if_not_exists on existing UUID returns 200 + outcome=skipped", func(t *testing.T) {
		// The manager simulates: UUID exists, OnlyIfNotExists condition fires.
		// This test catches the regression where the handler would forward the
		// ErrPreconditionFailed to a 500 or 422 instead of the idempotent 200.
		mgr := &conditionalFakeManager{
			addObjectErr: &uco.ErrPreconditionFailed{
				ObjectID: string(existingUUID),
				Reason:   "object already exists (OnlyIfNotExists condition failed)",
			},
		}
		h := &objectHandlers{
			manager:             mgr,
			metricRequestsTotal: &fakeMetricRequestsTotal{},
		}
		req := httptest.NewRequest(http.MethodPost,
			"/v1/objects?condition=insert_if_not_exists", nil)
		rr := httptest.NewRecorder()

		resp := h.addObject(objects.ObjectsCreateParams{
			HTTPRequest: req,
			Body:        &models.Object{Class: "Venue", ID: existingUUID},
		}, nil)
		resp.WriteResponse(rr, nil)

		assert.Equal(t, http.StatusOK, rr.Code,
			"insert_if_not_exists on existing UUID must return 200 OK (idempotent no-op per §6.4.1)")

		var body conditionalWriteResponse
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
		assert.Equal(t, "skipped", body.ConditionalResult.Outcome,
			"outcome must be skipped when UUID already exists")
	})

	t.Run("insert_if_not_exists on new UUID returns 201 + outcome=inserted", func(t *testing.T) {
		// The manager simulates: UUID is free, object gets created successfully.
		// This test catches the regression where the handler returns 200 instead
		// of 201 Created for a conditional insert that actually writes.
		mgr := &conditionalFakeManager{
			addObjectReturn: &models.Object{Class: "Venue", ID: newUUID},
			addObjectErr:    nil,
		}
		h := &objectHandlers{
			manager:             mgr,
			metricRequestsTotal: &fakeMetricRequestsTotal{},
		}
		req := httptest.NewRequest(http.MethodPost,
			"/v1/objects?condition=insert_if_not_exists", nil)
		rr := httptest.NewRecorder()

		resp := h.addObject(objects.ObjectsCreateParams{
			HTTPRequest: req,
			Body:        &models.Object{Class: "Venue", ID: newUUID},
		}, nil)
		resp.WriteResponse(rr, nil)

		assert.Equal(t, http.StatusCreated, rr.Code,
			"insert_if_not_exists on new UUID must return 201 Created")

		var body conditionalWriteResponse
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
		assert.Equal(t, "inserted", body.ConditionalResult.Outcome,
			"outcome must be inserted when the object was newly created")
	})

	t.Run("genuine precondition conflict returns 409", func(t *testing.T) {
		// The manager simulates: OnlyIfExists condition fails (object not found).
		// This test catches the regression where genuine conflicts are silently
		// swallowed or mapped to a wrong status code.
		mgr := &conditionalFakeManager{
			addObjectErr: &uco.ErrPreconditionFailed{
				ObjectID: string(newUUID),
				Reason:   "object does not exist (OnlyIfExists condition failed)",
			},
		}
		h := &objectHandlers{
			manager:             mgr,
			metricRequestsTotal: &fakeMetricRequestsTotal{},
		}
		req := httptest.NewRequest(http.MethodPost,
			"/v1/objects?condition=update_if_exists", nil)
		rr := httptest.NewRecorder()

		resp := h.addObject(objects.ObjectsCreateParams{
			HTTPRequest: req,
			Body:        &models.Object{Class: "Venue", ID: newUUID},
		}, nil)
		resp.WriteResponse(rr, nil)

		assert.Equal(t, http.StatusConflict, rr.Code,
			"a failed precondition that is not an idempotent skip must return 409 Conflict")
	})
}

// TestConditionalWriteRESTHandlerMessageText verifies that the 409 Conflict
// body includes the diagnostic message field per synthesis §6.7.
//
// This test catches the absence of message text in the 409 body: the old code
// path (before conditional write support) returned a generic ErrorResponse
// without the conditional_check_failed envelope, so Detail.Message would be
// empty after unmarshalling into conditionalWriteConflictResponse.
func TestConditionalWriteRESTHandlerMessageText(t *testing.T) {
	reason := "object version mismatch: expected 3, actual 7"
	mgr := &conditionalFakeManager{
		addObjectErr: &uco.ErrPreconditionFailed{
			ObjectID:        "00000000-0000-0000-0000-000000000099",
			Reason:          reason,
			ExpectedVersion: 3,
			ActualVersion:   7,
		},
	}
	h := &objectHandlers{
		manager:             mgr,
		metricRequestsTotal: &fakeMetricRequestsTotal{},
	}
	req := httptest.NewRequest(http.MethodPost,
		"/v1/objects?condition=version_match", nil)
	rr := httptest.NewRecorder()

	resp := h.addObject(objects.ObjectsCreateParams{
		HTTPRequest: req,
		Body:        &models.Object{Class: "ResearchItem"},
	}, nil)
	resp.WriteResponse(rr, nil)

	require.Equal(t, http.StatusConflict, rr.Code)

	var body conditionalWriteConflictResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))

	assert.Equal(t, "conditional_check_failed", body.Error,
		"error field must be conditional_check_failed per §6.7")
	assert.NotEmpty(t, body.Detail.Message,
		"detail.message must be present per §6.7 error-message-text contract")
	assert.True(t, strings.Contains(body.Detail.Message, reason),
		"detail.message must contain the diagnostic reason from ErrPreconditionFailed")
}
