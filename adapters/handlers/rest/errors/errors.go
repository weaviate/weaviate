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

package errors

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// ErrPayloadFromSingleErr builds a single-message ErrorResponse with the
// principal's own namespace prefix stripped from err. Pass nil for global
// callers to leave the message unchanged.
func ErrPayloadFromSingleErr(principal *models.Principal, err error) *models.ErrorResponse {
	return &models.ErrorResponse{Error: []*models.ErrorResponseErrorItems0{{
		Message: namespacing.StripErrorMessage(principal, err.Error()),
	}}}
}
