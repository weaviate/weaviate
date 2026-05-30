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

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
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
	// Provide expected_version=3 so the version_match switch case can parse it.
	req := httptest.NewRequest(http.MethodPost,
		"/v1/objects?condition=version_match&expected_version=3", nil)
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

// ---------------------------------------------------------------------------
// ETag / If-Match (Phase-2) REST handler tests
// ---------------------------------------------------------------------------

// etagFakeManager supports GetObject (for ETag) and UpdateObject (for If-Match).
// Other methods panic as in conditionalFakeManager.
type etagFakeManager struct {
	getObjectReturn *models.Object
	getObjectErr    error
	updateObjectReturn *models.Object
	updateObjectErr    error
	// capturedCtx captures the context passed to UpdateObject so tests can
	// inspect the Conditional that the handler injected.
	capturedCtx context.Context
}

func (f *etagFakeManager) AddObject(_ context.Context, _ *models.Principal,
	obj *models.Object, _ *additional.ReplicationProperties,
) (*models.Object, error) {
	panic("not implemented")
}
func (f *etagFakeManager) ValidateObject(context.Context, *models.Principal,
	*models.Object, *additional.ReplicationProperties,
) error {
	panic("not implemented")
}
func (f *etagFakeManager) GetObject(_ context.Context, _ *models.Principal,
	_ string, _ strfmt.UUID, _ additional.Properties, _ *additional.ReplicationProperties, _ string,
) (*models.Object, error) {
	return f.getObjectReturn, f.getObjectErr
}
func (f *etagFakeManager) DeleteObject(context.Context, *models.Principal,
	string, strfmt.UUID, *additional.ReplicationProperties, string,
) error {
	panic("not implemented")
}
func (f *etagFakeManager) UpdateObject(ctx context.Context, _ *models.Principal,
	_ string, _ strfmt.UUID, _ *models.Object, _ *additional.ReplicationProperties,
) (*models.Object, error) {
	f.capturedCtx = ctx
	return f.updateObjectReturn, f.updateObjectErr
}
func (f *etagFakeManager) HeadObject(context.Context, *models.Principal,
	string, strfmt.UUID, *additional.ReplicationProperties, string,
) (bool, *uco.Error) {
	panic("not implemented")
}
func (f *etagFakeManager) GetObjects(context.Context, *models.Principal,
	*int64, *int64, *string, *string, *string, additional.Properties, string,
) ([]*models.Object, error) {
	panic("not implemented")
}
func (f *etagFakeManager) Query(context.Context, *models.Principal,
	*uco.QueryParams,
) ([]*models.Object, *uco.Error) {
	panic("not implemented")
}
func (f *etagFakeManager) MergeObject(context.Context, *models.Principal,
	*models.Object, *additional.ReplicationProperties,
) *uco.Error {
	panic("not implemented")
}
func (f *etagFakeManager) AddObjectReference(context.Context, *models.Principal,
	*uco.AddReferenceInput, *additional.ReplicationProperties, string,
) *uco.Error {
	panic("not implemented")
}
func (f *etagFakeManager) UpdateObjectReferences(context.Context, *models.Principal,
	*uco.PutReferenceInput, *additional.ReplicationProperties, string,
) *uco.Error {
	panic("not implemented")
}
func (f *etagFakeManager) DeleteObjectReference(context.Context, *models.Principal,
	*uco.DeleteReferenceInput, *additional.ReplicationProperties, string,
) *uco.Error {
	panic("not implemented")
}
func (f *etagFakeManager) GetObjectsClass(context.Context, *models.Principal,
	strfmt.UUID,
) (*models.Class, error) {
	panic("not implemented")
}
func (f *etagFakeManager) GetObjectClassFromName(context.Context, *models.Principal,
	string,
) (*models.Class, error) {
	panic("not implemented")
}

// TestETagOnGetObject verifies that the getObject REST handler returns an
// etagResponder with the correct version when the object has a version in
// Additional.
//
// The test exercises the handler and verifies the ETag header is set before
// WriteResponse writes the body - the header is set during WriteResponse
// via etagResponder. We use a JSON producer to avoid the swagger nil-producer
// panic.
//
// Causal link: this test catches a missing etagResponder wrapper. Without it,
// etagResponder.version is 0 and rr.Header().Get("ETag") is empty.
func TestETagOnGetObject(t *testing.T) {
	objectVersion := uint64(5)
	obj := &models.Object{
		Class: "ResearchItem",
		ID:    "00000000-0000-0000-0000-000000000001",
		Additional: models.AdditionalProperties{
			"version": objectVersion,
		},
	}
	mgr := &etagFakeManager{getObjectReturn: obj}
	h := &objectHandlers{
		manager:             mgr,
		metricRequestsTotal: &fakeMetricRequestsTotal{},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/objects/ResearchItem/00000000-0000-0000-0000-000000000001", nil)
	rr := httptest.NewRecorder()

	resp := h.getObject(objects.ObjectsClassGetParams{
		HTTPRequest: req,
		ClassName:   "ResearchItem",
		ID:          "00000000-0000-0000-0000-000000000001",
	}, nil)

	// Use a JSON producer so that ObjectsClassGetOK.WriteResponse can serialize
	// the payload. The test asserts only on the ETag header, not the body.
	resp.WriteResponse(rr, runtime.JSONProducer())

	assert.Equal(t, `"5"`, rr.Header().Get("ETag"),
		"ETag header must be set to the quoted version")
}

// TestIfMatchOnPutObjectVersionMismatch verifies that the updateObject handler
// parses the If-Match header, injects IfVersion into the Conditional context,
// and maps ErrPreconditionFailed to HTTP 412 Precondition Failed.
//
// Causal link: this test catches a missing If-Match parse or a missing 412
// mapping. Without the parse, IfVersion is never set; without the 412 mapping,
// the handler returns 500. Either failure causes the status-code assertion to fail.
func TestIfMatchOnPutObjectVersionMismatch(t *testing.T) {
	mismatchErr := &uco.ErrPreconditionFailed{
		ObjectID:        "00000000-0000-0000-0000-000000000001",
		Reason:          "object version mismatch: expected 3, actual 7",
		ExpectedVersion: 3,
		ActualVersion:   7,
	}
	mgr := &etagFakeManager{updateObjectErr: mismatchErr}
	h := &objectHandlers{
		manager:             mgr,
		metricRequestsTotal: &fakeMetricRequestsTotal{},
	}

	req := httptest.NewRequest(http.MethodPut, "/v1/objects/ResearchItem/00000000-0000-0000-0000-000000000001", nil)
	req.Header.Set("If-Match", `"3"`)
	rr := httptest.NewRecorder()

	resp := h.updateObject(objects.ObjectsClassPutParams{
		HTTPRequest: req,
		ClassName:   "ResearchItem",
		ID:          "00000000-0000-0000-0000-000000000001",
		Body:        &models.Object{Class: "ResearchItem"},
	}, nil)
	resp.WriteResponse(rr, nil)

	require.Equal(t, http.StatusPreconditionFailed, rr.Code,
		"version mismatch on PUT with If-Match must return 412 Precondition Failed")

	var body map[string]interface{}
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
	assert.Equal(t, "precondition_failed", body["error"],
		"error field must be precondition_failed")
}

// TestVersionMatchConditionQueryParam verifies that ?condition=version_match&
// expected_version=N in addObject parses correctly and maps a version-mismatch
// ErrPreconditionFailed to 409 Conflict (not 200 OK nor 500 ISE).
//
// Causal link: without the version_match switch case, the conditional is never
// set; without the 409 mapping, the handler would return 500 for mismatch.
func TestVersionMatchConditionQueryParam(t *testing.T) {
	reason := "object version mismatch: expected 2, actual 5"
	mgr := &conditionalFakeManager{
		addObjectErr: &uco.ErrPreconditionFailed{
			ObjectID:        "00000000-0000-0000-0000-000000000002",
			Reason:          reason,
			ExpectedVersion: 2,
			ActualVersion:   5,
		},
	}
	h := &objectHandlers{
		manager:             mgr,
		metricRequestsTotal: &fakeMetricRequestsTotal{},
	}

	req := httptest.NewRequest(http.MethodPost,
		"/v1/objects?condition=version_match&expected_version=2", nil)
	rr := httptest.NewRecorder()

	resp := h.addObject(objects.ObjectsCreateParams{
		HTTPRequest: req,
		Body:        &models.Object{Class: "ResearchItem"},
	}, nil)
	resp.WriteResponse(rr, nil)

	require.Equal(t, http.StatusConflict, rr.Code,
		"version mismatch via ?condition=version_match must return 409 Conflict")
}

// TestIfMatchOnPutObjectSuccess verifies that a successful PUT with If-Match
// returns 200 OK with an ETag: "N+1" header reflecting the new version.
//
// Causal link: without the ETag on success path in updateObject, the header
// is absent and require.Equal(t, `"6"`, ...) fails.
func TestIfMatchOnPutObjectSuccess(t *testing.T) {
	updatedObj := &models.Object{
		Class: "ResearchItem",
		ID:    "00000000-0000-0000-0000-000000000001",
		Additional: models.AdditionalProperties{
			"version": uint64(6), // N+1 after write
		},
	}
	mgr := &etagFakeManager{updateObjectReturn: updatedObj}
	h := &objectHandlers{
		manager:             mgr,
		metricRequestsTotal: &fakeMetricRequestsTotal{},
	}

	req := httptest.NewRequest(http.MethodPut, "/v1/objects/ResearchItem/00000000-0000-0000-0000-000000000001", nil)
	req.Header.Set("If-Match", `"5"`)
	rr := httptest.NewRecorder()

	resp := h.updateObject(objects.ObjectsClassPutParams{
		HTTPRequest: req,
		ClassName:   "ResearchItem",
		ID:          "00000000-0000-0000-0000-000000000001",
		Body:        &models.Object{Class: "ResearchItem"},
	}, nil)
	resp.WriteResponse(rr, runtime.JSONProducer())

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, `"6"`, rr.Header().Get("ETag"),
		"ETag on successful PUT must be the new version N+1")
}

// logrusHook captures log entries at or above a threshold level so tests can
// assert that specific log levels were (or were not) emitted.
type logrusHook struct {
	entries []*logrus.Entry
}

func (h *logrusHook) Levels() []logrus.Level { return logrus.AllLevels }
func (h *logrusHook) Fire(e *logrus.Entry) error {
	h.entries = append(h.entries, e)
	return nil
}

// TestObjectsRequestsTotalLogErrorClassifiesPreconditionAsUserError verifies
// that objectsRequestsTotal.logError treats ErrPreconditionFailed as a
// user-driven outcome and does NOT emit an ERROR-level log line.
//
// This test catches the production log-noise bug: under a hot-key
// insert_if_not_exists workload the old classifier fell through to the
// default branch (logServerError) which called .Error("unexpected error"),
// flooding error dashboards with false positives on every conditional skip.
func TestObjectsRequestsTotalLogErrorClassifiesPreconditionAsUserError(t *testing.T) {
	hook := &logrusHook{}
	logger := logrus.New()
	logger.AddHook(hook)
	logger.SetLevel(logrus.DebugLevel)

	rt := &objectsRequestsTotal{
		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{
			metrics:   nil, // no prometheus metrics needed
			api:       "rest",
			queryType: "objects",
			logger:    logger,
		},
	}

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "insert_if_not_exists skip (OnlyIfNotExists condition failed)",
			err: &uco.ErrPreconditionFailed{
				ObjectID: "00000000-0000-0000-0000-000000000001",
				Reason:   "object already exists (OnlyIfNotExists condition failed)",
			},
		},
		{
			name: "update_if_exists conflict (OnlyIfExists condition failed)",
			err: &uco.ErrPreconditionFailed{
				ObjectID: "00000000-0000-0000-0000-000000000002",
				Reason:   "object does not exist (OnlyIfExists condition failed)",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hook.entries = nil
			rt.logError("TestClass", tc.err)

			for _, e := range hook.entries {
				if e.Level <= logrus.ErrorLevel {
					t.Errorf("logError emitted %s-level log for ErrPreconditionFailed; want no error log. Message: %q",
						e.Level, e.Message)
				}
			}
		})
	}
}
