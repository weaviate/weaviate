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

package clusterapi

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// A class the sender knows about but this node does not means this node is behind, so the caller
// gets a not-ready answer it can fail over from instead of a 422 it will treat as its own fault.
func TestLocalIndexMissingStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{
			name: "the class has not been applied here yet",
			err:  enterrors.NewErrUnprocessable(fmt.Errorf("local index %q not found", "Product_v2")),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "wrapped on its way out",
			err:  fmt.Errorf("search: %w", enterrors.NewErrUnprocessable(errors.New(`local index "Product_v2" not found`))),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "a genuinely unprocessable request stays 422",
			err:  enterrors.NewErrUnprocessable(errors.New("vector lengths don't match")),
			want: http.StatusUnprocessableEntity,
		},
		{
			name: "a missing shard is not a missing index",
			err:  enterrors.NewErrUnprocessable(errors.New("shard not found")),
			want: http.StatusUnprocessableEntity,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, localIndexMissingStatus(test.err))
		})
	}
}
