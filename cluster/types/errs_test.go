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

package types

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

func TestIsNoLeader(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "ErrNotLeader", err: ErrNotLeader, want: true},
		{name: "ErrLeaderNotFound", err: ErrLeaderNotFound, want: true},
		{name: "raft.ErrNotLeader", err: raft.ErrNotLeader, want: true},
		{name: "raft.ErrLeadershipLost", err: raft.ErrLeadershipLost, want: true},
		// The forwarded path wraps, and leaderErr decorates ErrLeaderNotFound
		// with the unreachable node names.
		{name: "wrapped ErrLeaderNotFound", err: fmt.Errorf("apply: %w, can not resolve nodes [n1]", ErrLeaderNotFound), want: true},
		{name: "wrapped raft.ErrLeadershipLost", err: fmt.Errorf("execute: %w", raft.ErrLeadershipLost), want: true},
		{name: "joined ErrNotLeader", err: errors.Join(errors.New("rpc error"), ErrNotLeader), want: true},
		// Neighbours that must not be mistaken for a leadership problem.
		{name: "ErrNotOpen", err: ErrNotOpen, want: false},
		{name: "ErrNotFound", err: ErrNotFound, want: false},
		{name: "ErrDeadlineExceeded", err: ErrDeadlineExceeded, want: false},
		{name: "unrelated", err: errors.New("disk on fire"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsNoLeader(tc.err))
		})
	}
}
