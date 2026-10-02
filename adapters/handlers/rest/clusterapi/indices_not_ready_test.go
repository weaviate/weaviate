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
	enterrors "github.com/weaviate/weaviate/entities/errors"
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
			name: "wrapped per shard, as Index does",
			err:  fmt.Errorf("shard %q: wait for schema version 55: %w", "S1", clusterTypes.ErrDeadlineExceeded),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "the same text without the cause is not classified",
			err:  errors.New("shard \"S1\": wait for schema version 55: deadline exceeded"),
			want: http.StatusInternalServerError,
		},
		{
			name: "the sentinel on its own",
			err:  fmt.Errorf("something: %w", clusterTypes.ErrDeadlineExceeded),
			want: http.StatusServiceUnavailable,
		},
		{
			name: "a class this node does not have yet",
			err:  enterrors.ErrLocalIndexNotFound{Index: "Product_v2"},
			want: http.StatusServiceUnavailable,
		},
		{
			name: "reached through the unprocessable wrapper the shards layer adds",
			err:  enterrors.NewErrUnprocessable(enterrors.ErrLocalIndexNotFound{Index: "Product_v2"}),
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

// A batch fails whole when this node is behind, so it answers unavailable rather than a per-object
// error list the caller would read as a partial success.
func TestBatchNotCaughtUp(t *testing.T) {
	lagging := fmt.Errorf("wait for schema version 55: %w", clusterTypes.ErrDeadlineExceeded)

	tests := []struct {
		name string
		errs []error
		want bool
	}{
		{name: "no errors", errs: []error{nil, nil}},
		{name: "every object blocked by the wait", errs: []error{lagging, lagging}, want: true},
		{name: "the wait plus a real failure stays per object", errs: []error{lagging, errors.New("disk full")}},
		{name: "a partial success stays per object", errs: []error{nil, errors.New("invalid vector")}},
		{name: "blocked with gaps", errs: []error{nil, lagging, nil}, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := batchNotCaughtUp(test.errs)
			if !test.want {
				assert.Nil(t, got)
				return
			}
			assert.ErrorIs(t, got, clusterTypes.ErrDeadlineExceeded)
		})
	}
}
