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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/objects"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	uco "github.com/weaviate/weaviate/usecases/objects"
)

// A memory-guard shed must render as HTTP 429, not 500.
func TestObjectWriteMemoryShedRendersTooManyRequests(t *testing.T) {
	newHandler := func(f *fakeManager) *objectHandlers {
		return &objectHandlers{
			manager:             f,
			logger:              logrus.New(),
			metricRequestsTotal: &fakeMetricRequestsTotal{},
		}
	}

	// the exact shapes the usecase layer produces when the guard rejects a write
	patchErr := &uco.Error{
		Msg:  enterrors.ErrNotEnoughMemory.Error(),
		Code: uco.StatusInternalServerError,
		Err:  enterrors.ErrNotEnoughMemory,
	}
	addErr := fmt.Errorf("cannot process add object: %w", enterrors.ErrNotEnoughMemory)
	updateErr := fmt.Errorf("cannot process update object: %w", enterrors.ErrNotEnoughMemory)
	deleteErr := fmt.Errorf("cannot process delete object: %w", enterrors.ErrNotEnoughMemory)

	body := &models.Object{
		Class:      "MyClass",
		ID:         "85f78e29-5937-4390-a121-5379f262b4e5",
		Properties: map[string]interface{}{"name": "hello world"},
	}

	tests := []struct {
		name string
		call func() middleware.Responder
	}{
		{
			// PATCH is the only journey whose usecase returns a typed *uco.Error
			name: "patch object",
			call: func() middleware.Responder {
				h := newHandler(&fakeManager{patchObjectReturn: patchErr})
				return h.patchObject(objects.ObjectsClassPatchParams{
					HTTPRequest: httptest.NewRequest(http.MethodPatch, "/v1/objects/MyClass/123", nil),
					ClassName:   "MyClass",
					ID:          "123",
					Body:        body,
				}, nil)
			},
		},
		{
			// POST /v1/objects
			name: "add object",
			call: func() middleware.Responder {
				h := newHandler(&fakeManager{addObjectErr: addErr})
				return h.addObject(objects.ObjectsCreateParams{
					HTTPRequest: httptest.NewRequest(http.MethodPost, "/v1/objects", nil),
					Body:        body,
				}, nil)
			},
		},
		{
			// PUT /v1/objects/{class}/{id}
			name: "update object",
			call: func() middleware.Responder {
				h := newHandler(&fakeManager{updateObjectErr: updateErr})
				return h.updateObject(objects.ObjectsClassPutParams{
					HTTPRequest: httptest.NewRequest(http.MethodPut, "/v1/objects/MyClass/123", nil),
					ClassName:   "MyClass",
					ID:          "123",
					Body:        body,
				}, nil)
			},
		},
		{
			// DELETE /v1/objects/{class}/{id}
			name: "delete object",
			call: func() middleware.Responder {
				h := newHandler(&fakeManager{deleteObjectReturn: deleteErr})
				return h.deleteObject(objects.ObjectsClassDeleteParams{
					HTTPRequest: httptest.NewRequest(http.MethodDelete, "/v1/objects/MyClass/123", nil),
					ClassName:   "MyClass",
					ID:          "123",
				}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			res := tt.call()
			require.NotNil(t, res)
			res.WriteResponse(rec, runtime.JSONProducer())

			assert.NotEqual(t, http.StatusInternalServerError, rec.Code,
				"a memory-guard shed must not be indistinguishable from a server fault: "+
					"500 is classified retryable by the replica client, so it invites retries "+
					"against a node that has no memory")
			require.Equal(t, http.StatusTooManyRequests, rec.Code,
				"a memory-guard shed must surface as HTTP 429 so callers can back off deliberately")
		})
	}
}
