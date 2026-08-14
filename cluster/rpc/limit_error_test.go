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

package rpc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// TestFromRPCError_LimitExceeded pins the cluster-RPC round-trip of a usage-limit
// rejection (toRPCError encode → fromRPCError decode): a follower that forwards
// AddTenants to the leader recovers the full typed *LimitExceededError —
// message, Limit and Value all intact, so the REST 429 payload isn't degraded —
// while every other error passes through untouched.
func TestFromRPCError_LimitExceeded(t *testing.T) {
	limitErr := usagelimits.NewLimitExceededError(
		"Free-tier limit of {value} {limit} reached", usagelimits.LimitTenants, 10)

	tests := []struct {
		name      string
		in        error
		wantLimit *usagelimits.LimitExceededError // nil ⇒ expect pass-through
	}{
		{
			name:      "limit rejection survives the RPC round-trip with limit/value",
			in:        toRPCError(limitErr),
			wantLimit: limitErr,
		},
		{
			name: "non-limit error passes through unchanged",
			in:   errors.New("some other failure"),
		},
		{
			name: "nil passes through",
			in:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fromRPCError(tt.in)

			le, ok := usagelimits.AsLimitExceeded(got)
			if tt.wantLimit != nil {
				require.True(t, ok, "expected a typed *LimitExceededError, got %v", got)
				assert.Equal(t, tt.wantLimit.Error(), le.Error(), "message must survive")
				assert.Equal(t, tt.wantLimit.Limit, le.Limit, "limit name must survive")
				assert.Equal(t, tt.wantLimit.Value, le.Value, "limit value must survive")
				return
			}
			assert.False(t, ok, "non-limit error must not become a limit error")
			assert.Equal(t, tt.in, got, "non-limit error must pass through unchanged")
		})
	}
}
