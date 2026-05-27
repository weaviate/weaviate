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
	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/restrictions"
)

func newRestrictionViolationPayload(v *restrictions.ViolationError) *models.RestrictionViolationResponse {
	return &models.RestrictionViolationResponse{
		ErrorCode:   restrictions.ErrorCode,
		Restriction: string(v.Restriction),
		Value:       v.Value,
		Allowed:     append([]string(nil), v.Allowed...),
		Message:     v.Error(),
	}
}

// restrictionViolationFromErr wraps a non-restriction error into the
// same RestrictionViolationResponse shape so handlers can return both
// 422 cases through the same swagger-generated payload type.
func restrictionViolationFromErr(principal *models.Principal, err error) *models.RestrictionViolationResponse {
	return cerrors.ErrRestrictionViolation(principal, err)
}
