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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	uco "github.com/weaviate/weaviate/usecases/objects"
)

// memoryShedResponder must classify every wrapping shape of a memory-guard rejection.
func TestMemoryShedResponder(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantShed bool
	}{
		{
			name:     "bare not enough memory sentinel",
			err:      enterrors.ErrNotEnoughMemory,
			wantShed: true,
		},
		{
			name:     "not enough memory mappings sentinel",
			err:      enterrors.ErrNotEnoughMappings,
			wantShed: true,
		},
		{
			// POST /v1/objects, PUT and DELETE /v1/objects/{class}/{id}
			name:     "single object usecase shed",
			err:      uco.NewMemoryShedError("cannot process add object", enterrors.ErrNotEnoughMemory),
			wantShed: true,
		},
		{
			// PATCH /v1/objects/{class}/{id}: a typed error still carrying a 500
			name: "legacy typed error still carrying 500",
			err: &uco.Error{
				Msg:  enterrors.ErrNotEnoughMemory.Error(),
				Code: uco.StatusInternalServerError,
				Err:  enterrors.ErrNotEnoughMemory,
			},
			wantShed: true,
		},
		{
			// POST /v1/batch/objects, from db.BatchPutObjects
			name:     "batch put shed",
			err:      fmt.Errorf("cannot process batch: %w", enterrors.ErrNotEnoughMemory),
			wantShed: true,
		},
		{
			// DELETE /v1/batch/objects, from db.BatchDeleteObjects
			name:     "batch delete shed",
			err:      fmt.Errorf("cannot process batch delete object: %w", enterrors.ErrNotEnoughMemory),
			wantShed: true,
		},
		{
			// control: must keep falling through to the handler's own classification
			name: "unrelated error is not a shed",
			err:  fmt.Errorf("segment corrupted"),
		},
		{
			name: "user input error is not a shed",
			err:  uco.NewErrInvalidUserInput("invalid object"),
		},
		{
			name: "nil error is not a shed",
			err:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := memoryShedResponder(nil, tt.err)
			if !tt.wantShed {
				assert.Nil(t, res,
					"only a memory-guard rejection may be turned into a 429; "+
						"everything else must keep its own classification")
				return
			}

			require.NotNil(t, res, "a memory-guard rejection must be rendered as a load-shed")

			rec := httptest.NewRecorder()
			res.WriteResponse(rec, runtime.JSONProducer())
			assert.Equal(t, http.StatusTooManyRequests, rec.Code)
		})
	}
}
