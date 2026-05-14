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

package replica

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWrapRouteStale_SurvivesFmtWrap pins the contract the coordinator
// abort path relies on: a StatusRouteStale Error wrapped through
// wrapRouteStale must remain detectable via errors.As(*routeStaleErr)
// even after additional fmt.Errorf("%w") wrapping done by
// broadcast/abort. Non-route-stale errors must NOT match.
func TestWrapRouteStale_SurvivesFmtWrap(t *testing.T) {
	cases := []struct {
		name        string
		input       error
		wantStale   bool
		wantApplied uint64
	}{
		{
			name:        "StatusRouteStale Error wraps to *routeStaleErr",
			input:       &Error{Code: StatusRouteStale, Msg: "shard \"t0\": stale routing", Applied: 42},
			wantStale:   true,
			wantApplied: 42,
		},
		{
			name:      "StatusShardNotFound does not wrap to *routeStaleErr",
			input:     &Error{Code: StatusShardNotFound, Msg: "missing shard"},
			wantStale: false,
		},
		{
			name:      "plain error does not wrap to *routeStaleErr",
			input:     errors.New("connection refused"),
			wantStale: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := wrapRouteStale("host-A", tc.input)
			assert.Error(t, wrapped)

			// Survives a further layer of fmt-wrapping, exactly as the
			// broadcast abort path does.
			outer := fmt.Errorf("errors: replica host-A; %s; broadcast: %w: %w",
				wrapped.Error(), ErrReplicas, wrapped)
			var rs *routeStaleErr
			gotStale := errors.As(outer, &rs)
			assert.Equal(t, tc.wantStale, gotStale)
			if tc.wantStale {
				assert.Equal(t, tc.wantApplied, rs.Applied)
			}
		})
	}
}
