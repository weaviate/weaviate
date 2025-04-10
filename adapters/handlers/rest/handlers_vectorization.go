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
	"time"

	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/vectorization"
	"github.com/weaviate/weaviate/entities/models"
)

func setupVectorizationHandlers(api *operations.WeaviateAPI) {
	h := &vectorizationHandlers{}

	api.VectorizationVectorizationStartHandler = vectorization.VectorizationStartHandlerFunc(h.start)
	api.VectorizationVectorizationGetStatusHandler = vectorization.VectorizationGetStatusHandlerFunc(h.getStatus)
	api.VectorizationVectorizationCancelHandler = vectorization.VectorizationCancelHandlerFunc(h.cancel)
}

type vectorizationHandlers struct{}

func (h *vectorizationHandlers) start(params vectorization.VectorizationStartParams, principal *models.Principal) middleware.Responder {
	return vectorization.NewVectorizationStartOK()
}

func (h *vectorizationHandlers) getStatus(params vectorization.VectorizationGetStatusParams, principal *models.Principal) middleware.Responder {
	return vectorization.NewVectorizationGetStatusOK().
		WithPayload(&models.VectorizationStatusResponse{
			Status:       "STARTED",
			CreatedAt:    strfmt.DateTime(time.Now()),
			TenantFilter: ptr("production-.*"),
		})
}

func (h *vectorizationHandlers) cancel(params vectorization.VectorizationCancelParams, principal *models.Principal) middleware.Responder {
	return vectorization.NewVectorizationCancelOK()
}

func ptr[T any](v T) *T {
	return &v
}
