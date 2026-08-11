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
			name: "carrier whose items all carry an empty message falls back to the error string",
			err:  onlyEmptyItems,
			want: onlyEmptyItems.Error(),
		},
		{
			name: "single server message is appended",
			err:  single,
			want: single.Error() + ": import into non-existing index for AutoCreated",
		},
		{
			name: "multiple server messages are joined",
			err:  multiple,
			want: multiple.Error() + ": first; second",
		},
		{
			name: "nil items are skipped, real ones kept",
			err:  nilAmongItems,
			want: nilAmongItems.Error() + ": second",
		},
		{
			name: "empty messages are skipped, real ones kept",
			err:  emptyAmongItems,
			want: emptyAmongItems.Error() + ": second",
		},
		{
			name: "carrier reached through a wrapped error",
			err:  wrapped,
			want: wrapped.Error() + ": import into non-existing index for AutoCreated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ErrorDetail(tt.err))
		})
	}
}
