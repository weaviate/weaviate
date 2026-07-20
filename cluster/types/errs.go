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

	"github.com/hashicorp/raft"
)

var (
	// ErrNotLeader is returned when an operation can't be completed on a
	// follower or candidate node.
	ErrNotLeader      = errors.New("node is not the leader")
	ErrLeaderNotFound = errors.New("leader not found")
	ErrNotOpen        = errors.New("store not open")
	ErrUnknownCommand = errors.New("unknown command")
	// ErrDeadlineExceeded represents an error returned when the deadline for waiting for a specific update is exceeded.
	ErrDeadlineExceeded = errors.New("deadline exceeded for waiting for update")
	ErrNotFound         = errors.New("not found")
)

// IsNoLeader reports whether err means the operation could not reach a
// leader. ErrNotLeader/ErrLeaderNotFound come back from a forwarded call;
// hashicorp's two are returned raw by a leader-local apply that exhausted
// its retries.
func IsNoLeader(err error) bool {
	return errors.Is(err, ErrNotLeader) ||
		errors.Is(err, ErrLeaderNotFound) ||
		errors.Is(err, raft.ErrNotLeader) ||
		errors.Is(err, raft.ErrLeadershipLost)
}
