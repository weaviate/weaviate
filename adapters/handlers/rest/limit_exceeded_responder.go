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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// newUsageLimitPayload converts a *usagelimits.LimitExceededError into the
// generated *models.UsageLimitExceededResponse used as the body of every
// HTTP 429 response in the affected write endpoints (single object create
// and replace, batch objects, class create, tenant create).
//
// The structured fields (`errorCode`, `limit`, `value`) are stable wire
// contract; the `message` is rendered from the operator-overridable
// USAGE_LIMITS_ERROR_MESSAGE template and may differ across deployments.
func newUsageLimitPayload(le *usagelimits.LimitExceededError) *models.UsageLimitExceededResponse {
	return &models.UsageLimitExceededResponse{
		ErrorCode: usagelimits.ErrorCode,
		Limit:     string(le.Limit),
		Value:     le.Value,
		Message:   le.Error(),
	}
}
