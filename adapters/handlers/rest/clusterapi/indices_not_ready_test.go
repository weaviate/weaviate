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

	clusterTypes "github.com/weaviate/weaviate/cluster/types"
)

// A node behind on schema must read as unavailable, not as a fault: 500 is retryable, so the caller
// would spend the MAX_RETRIES ladder on a node that has already said it cannot answer yet.
func TestOperationStatus(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{
			name: "the sender asked for a version this node has not applied",
			err:  fmt.Errorf("wait for schema version 55: %w", clusterTypes.ErrDeadlineExceeded),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "wrapped per shard",
			err:  fmt.Errorf("shard %q: wait for schema version 55: deadline exceeded", "S1"),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "the sentinel on its own",
			err:  fmt.Errorf("something: %w", clusterTypes.ErrDeadlineExceeded),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "a class this node does not have yet",
			err:  errors.New(`local index "Product_v2" not found`),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "a real failure stays an internal error",
			err:  errors.New("write to disk: no space left on device"),
			want: http.StatusInternalServerError,
		},
		{name: "no error", err: nil, want: http.StatusInternalServerError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, operationStatus(test.err))
		})
	}
}
