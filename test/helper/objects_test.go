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

package helper

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
)

func TestErrorDetail(t *testing.T) {
	item := func(msg string) *models.ErrorResponseErrorItems0 {
		return &models.ErrorResponseErrorItems0{Message: msg}
	}
	carrier := func(items ...*models.ErrorResponseErrorItems0) *objects.ObjectsCreateInternalServerError {
		return &objects.ObjectsCreateInternalServerError{
			Payload: &models.ErrorResponse{Error: items},
		}
	}

	nilPayload := &objects.ObjectsCreateInternalServerError{}
	noItems := carrier()
	onlyNilItems := carrier(nil, nil)
	onlyEmptyItems := carrier(item(""), item(""))
	single := carrier(item("import into non-existing index for AutoCreated"))
	multiple := carrier(item("first"), item("second"))
	nilAmongItems := carrier(nil, item("second"), nil)
	emptyAmongItems := carrier(item(""), item("second"), item(""))
	wrapped := fmt.Errorf("put object: %w", single)

	restrictionList := &schema.SchemaObjectsCreateUnprocessableEntity{
		Payload: &models.RestrictionViolationResponse{
			Error: []*models.RestrictionViolationResponseErrorItems0{
				{Message: "class name ns1:Movie already exists"},
			},
		},
	}
	restrictionStructured := &schema.SchemaObjectsCreateUnprocessableEntity{
		Payload: &models.RestrictionViolationResponse{
			ErrorCode: "CONFIG_NOT_ALLOWED", Restriction: "vector_index_type",
			Value: "flat", Allowed: []string{"hnsw"},
			Message: "vector index type flat is not allowed",
		},
	}
	restrictionNilPayload := &schema.SchemaObjectsCreateUnprocessableEntity{}
	usageLimit := &objects.ObjectsCreateTooManyRequests{
		Payload: &models.UsageLimitExceededResponse{
			ErrorCode: "USAGE_LIMIT_EXCEEDED", Limit: "objects", Value: 1000,
			Message: "object limit of 1000 exceeded",
		},
	}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "nil error",
			err:  nil,
			want: "<nil>",
		},
		{
			name: "plain error carries no payload, so it renders unchanged",
			err:  errors.New("boom"),
			want: "boom",
		},
		{
			name: "carrier with a nil payload falls back to the error string",
			err:  nilPayload,
			want: nilPayload.Error(),
		},
		{
			name: "carrier with no message items falls back to the error string",
			err:  noItems,
			want: noItems.Error(),
		},
		{
			name: "carrier whose items are all nil falls back to the error string",
			err:  onlyNilItems,
			want: onlyNilItems.Error(),
		},
		{
			name: "single server message replaces the error string",
			err:  single,
			want: "import into non-existing index for AutoCreated",
		},
		{
			name: "multiple server messages are joined",
			err:  multiple,
			want: "first; second",
		},
		{
			name: "nil items are skipped, real ones kept",
			err:  nilAmongItems,
			want: "second",
		},
		{
			name: "an empty message stays visible as a placeholder",
			err:  emptyAmongItems,
			want: "<empty>; second; <empty>",
		},
		{
			name: "items that all carry an empty message render as placeholders",
			err:  onlyEmptyItems,
			want: "<empty>; <empty>",
		},
		{
			name: "carrier reached through a wrapped error",
			err:  wrapped,
			want: "import into non-existing index for AutoCreated",
		},
		{
			name: "schema 422 message list is unpacked like the plain one",
			err:  restrictionList,
			want: "class name ns1:Movie already exists",
		},
		{
			name: "schema 422 without a message list falls back to the error string",
			err:  restrictionStructured,
			want: restrictionStructured.Error(),
		},
		{
			name: "schema 422 with a nil payload falls back to the error string",
			err:  restrictionNilPayload,
			want: restrictionNilPayload.Error(),
		},
		{
			name: "429 payload has no message list, so it falls back",
			err:  usageLimit,
			want: usageLimit.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ErrorDetail(tt.err))
		})
	}
}

// The payload shapes without a message list take the err.Error() fallback. That
// is acceptable because every field they carry is a scalar, which %+v prints in
// full -- unlike the pointer slice ErrorDetail exists to unpack. Each payload
// below sets every field it has, and each of those values is asserted.
func TestErrorDetailFallbackKeepsScalarPayloadsReadable(t *testing.T) {
	restrictionStructured := &schema.SchemaObjectsCreateUnprocessableEntity{
		Payload: &models.RestrictionViolationResponse{
			Allowed:     []string{"hnsw", "flat"},
			ErrorCode:   "CONFIG_NOT_ALLOWED",
			Message:     "vector index type dynamic is not allowed",
			Restriction: "vector_index_type",
			Value:       "dynamic",
		},
	}
	usageLimit := &objects.ObjectsCreateTooManyRequests{
		Payload: &models.UsageLimitExceededResponse{
			ErrorCode: "USAGE_LIMIT_EXCEEDED",
			Limit:     "objects",
			Message:   "object limit of 1000 exceeded",
			Value:     1000,
		},
	}

	tests := []struct {
		name string
		err  error
		want []string
	}{
		{
			name: "schema 422 structured fields",
			err:  restrictionStructured,
			want: []string{
				"Allowed:[hnsw flat]",
				"ErrorCode:CONFIG_NOT_ALLOWED",
				"Message:vector index type dynamic is not allowed",
				"Restriction:vector_index_type",
				"Value:dynamic",
			},
		},
		{
			name: "429 usage limit",
			err:  usageLimit,
			want: []string{
				"ErrorCode:USAGE_LIMIT_EXCEEDED",
				"Limit:objects",
				"Message:object limit of 1000 exceeded",
				"Value:1000",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ErrorDetail(tt.err)
			for _, want := range tt.want {
				assert.Contains(t, got, want)
			}
		})
	}
}
