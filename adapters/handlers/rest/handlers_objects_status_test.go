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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/objects"
	"github.com/weaviate/weaviate/entities/models"
	uco "github.com/weaviate/weaviate/usecases/objects"
)

// TestObjectHandlers_UnprocessableEntityArms asserts each endpoint whose manager
// now answers StatusUnprocessableEntity writes 422 to the wire. All six, because
// an endpoint whose switch has no 422 arm falls to default and answers 500.
func TestObjectHandlers_UnprocessableEntityArms(t *testing.T) {
	refused := &uco.Error{Code: uco.StatusUnprocessableEntity, Msg: "namespace is suspended"}

	newHandlers := func(m *fakeManager) *objectHandlers {
		return &objectHandlers{
			manager:             m,
			logger:              &logrus.Logger{},
			metricRequestsTotal: &fakeMetricRequestsTotal{},
		}
	}

	tests := []struct {
		name string
		call func() middleware.Responder
	}{
		{
			name: "HEAD /v1/objects/{class}/{id}",
			call: func() middleware.Responder {
				h := newHandlers(&fakeManager{headObjectErr: refused})
				return h.headObject(objects.ObjectsClassHeadParams{
					HTTPRequest: httptest.NewRequest("HEAD", "/v1/objects/alpha:Movies/123", nil),
					ClassName:   "alpha:Movies", ID: "123",
				}, nil)
			},
		},
		{
			name: "PATCH /v1/objects/{class}/{id}",
			call: func() middleware.Responder {
				h := newHandlers(&fakeManager{patchObjectReturn: refused})
				return h.patchObject(objects.ObjectsClassPatchParams{
					HTTPRequest: httptest.NewRequest("PATCH", "/v1/objects/alpha:Movies/123", nil),
					ClassName:   "alpha:Movies", ID: "123",
					Body: &models.Object{Class: "alpha:Movies"},
				}, nil)
			},
		},
		{
			name: "GET /v1/objects (the object list)",
			call: func() middleware.Responder {
				h := newHandlers(&fakeManager{queryErr: refused})
				class := "alpha:Movies"
				return h.query(objects.ObjectsListParams{
					HTTPRequest: httptest.NewRequest("GET", "/v1/objects?class=alpha:Movies", nil),
					Class:       &class,
				}, nil)
			},
		},
		{
			name: "POST /v1/objects/{class}/{id}/references/{prop}",
			call: func() middleware.Responder {
				h := newHandlers(&fakeManager{addRefErr: refused})
				return h.addObjectReference(objects.ObjectsClassReferencesCreateParams{
					HTTPRequest: httptest.NewRequest("POST", "/v1/objects/alpha:Movies/123/references/related", nil),
					ClassName:   "alpha:Movies", ID: "123", PropertyName: "related",
					Body: &models.SingleRef{Beacon: "weaviate://localhost/Animal/123"},
				}, nil)
			},
		},
		{
			name: "PUT /v1/objects/{class}/{id}/references/{prop}",
			call: func() middleware.Responder {
				h := newHandlers(&fakeManager{putRefErr: refused})
				return h.putObjectReferences(objects.ObjectsClassReferencesPutParams{
					HTTPRequest: httptest.NewRequest("PUT", "/v1/objects/alpha:Movies/123/references/related", nil),
					ClassName:   "alpha:Movies", ID: "123", PropertyName: "related",
				}, nil)
			},
		},
		{
			name: "DELETE /v1/objects/{class}/{id}/references/{prop}",
			call: func() middleware.Responder {
				h := newHandlers(&fakeManager{deleteRefErr: refused})
				return h.deleteObjectReference(objects.ObjectsClassReferencesDeleteParams{
					HTTPRequest: httptest.NewRequest("DELETE", "/v1/objects/alpha:Movies/123/references/related", nil),
					ClassName:   "alpha:Movies", ID: "123", PropertyName: "related",
					Body: &models.SingleRef{Beacon: "weaviate://localhost/Animal/123"},
				}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tt.call().WriteResponse(rec, runtime.JSONProducer())
			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code,
				"the endpoint must have a 422 arm; without one the code falls to default and answers 500")
		})
	}
}
