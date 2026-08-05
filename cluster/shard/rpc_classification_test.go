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

package shard_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/shard"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/entities/storagestate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestToRPCError pins the server-side error→code mapping. The codes are
// load-bearing: the client-side classifier decides retryable-vs-permanent for
// forwarded applies purely from them, so any leadership/availability error
// falling through to codes.Internal becomes a client-visible failure.
func TestToRPCError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "store ErrNotLeader", err: shard.ErrNotLeader, want: codes.ResourceExhausted},
		{name: "store ErrLeadershipLost", err: shard.ErrLeadershipLost, want: codes.ResourceExhausted},
		{name: "types ErrNotLeader", err: types.ErrNotLeader, want: codes.ResourceExhausted},
		{name: "types ErrLeaderNotFound", err: types.ErrLeaderNotFound, want: codes.ResourceExhausted},
		{name: "wrapped ErrLeadershipLost", err: fmt.Errorf("apply: %w", shard.ErrLeadershipLost), want: codes.ResourceExhausted},
		{name: "ErrProposalBackpressure", err: shard.ErrProposalBackpressure, want: codes.Unavailable},
		{name: "ErrNotStarted", err: shard.ErrNotStarted, want: codes.Unavailable},
		{name: "ErrAlreadyClosed", err: shard.ErrAlreadyClosed, want: codes.Unavailable},
		{name: "types ErrNotOpen", err: types.ErrNotOpen, want: codes.Unavailable},
		{name: "schema ErrMTDisabled", err: schema.ErrMTDisabled, want: codes.FailedPrecondition},
		// ErrCommandTooLarge must NOT map to ResourceExhausted (gRPC's
		// conventional too-large code): that is NotLeaderRPCCode here, which
		// the client classifies as retryable — the exact budget burn the
		// sentinel exists to prevent.
		{name: "ErrCommandTooLarge", err: shard.ErrCommandTooLarge, want: codes.InvalidArgument},
		{name: "wrapped ErrCommandTooLarge", err: fmt.Errorf("apply: %w", shard.ErrCommandTooLarge), want: codes.InvalidArgument},
		// Admission rejections (reject-fast): the client must see the reason
		// with a non-retryable code — FailedPrecondition is never in the
		// client classifier's retryable set.
		{name: "read-only with reason", err: storagestate.ErrStatusReadOnlyWithReason("resource pressure"), want: codes.FailedPrecondition},
		{name: "wrapped read-only", err: fmt.Errorf("apply: %w", storagestate.ErrStatusReadOnly), want: codes.FailedPrecondition},
		{name: "ErrClassDropped", err: shard.ErrClassDropped, want: codes.FailedPrecondition},
		{name: "wrapped ErrClassDropped", err: fmt.Errorf("apply: %w", shard.ErrClassDropped), want: codes.FailedPrecondition},
		{name: "not found text", err: fmt.Errorf("object xyz %w", types.ErrNotFound), want: codes.NotFound},
		{name: "generic error", err: errors.New("disk exploded"), want: codes.Internal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := shard.ToRPCError(tc.err)
			require.Error(t, got)
			require.Equal(t, tc.want, status.Code(got))
		})
	}
	require.NoError(t, shard.ToRPCError(nil))
}

// TestIsRetryableApplyErr pins the single client-side classification table:
// local sentinels, per-attempt timeouts, and the typed codes produced by
// toRPCError.
func TestIsRetryableApplyErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "ErrNotLeader", err: shard.ErrNotLeader, want: true},
		{name: "ErrLeadershipLost", err: shard.ErrLeadershipLost, want: true},
		{name: "ErrProposalBackpressure", err: shard.ErrProposalBackpressure, want: true},
		{name: "ErrNoLeaderFound", err: shard.ErrNoLeaderFound, want: true},
		{name: "wrapped ErrNoLeaderFound", err: fmt.Errorf("%w: create RPC client", shard.ErrNoLeaderFound), want: true},
		{name: "ErrLeaderElectionTimeout", err: shard.ErrLeaderElectionTimeout, want: true},
		{name: "attempt deadline", err: context.DeadlineExceeded, want: true},
		{name: "wrapped attempt deadline", err: fmt.Errorf("apply: %w", context.DeadlineExceeded), want: true},
		{name: "status not-leader code", err: status.Error(codes.ResourceExhausted, "not the leader"), want: true},
		{name: "status unavailable", err: status.Error(codes.Unavailable, "backpressure"), want: true},
		{name: "status deadline", err: status.Error(codes.DeadlineExceeded, "slow"), want: true},
		{name: "status internal", err: status.Error(codes.Internal, "leadership lost"), want: false},
		{name: "status not found", err: status.Error(codes.NotFound, "store not found"), want: false},
		{name: "ErrCommandTooLarge", err: shard.ErrCommandTooLarge, want: false},
		{name: "wrapped ErrCommandTooLarge", err: fmt.Errorf("apply: %w", shard.ErrCommandTooLarge), want: false},
		{name: "status invalid argument", err: status.Error(codes.InvalidArgument, "command exceeds max raft entry size"), want: false},
		// Admission rejections: non-retryable on the local-leader route; the
		// forwarded route agrees via codes.FailedPrecondition.
		{name: "read-only with reason", err: storagestate.ErrStatusReadOnlyWithReason("resource pressure"), want: false},
		{name: "ErrClassDropped", err: shard.ErrClassDropped, want: false},
		{name: "wrapped ErrClassDropped", err: fmt.Errorf("apply: %w", shard.ErrClassDropped), want: false},
		{name: "status failed precondition", err: status.Error(codes.FailedPrecondition, "store is read-only due to: resource pressure"), want: false},
		{name: "generic error", err: errors.New("marshal failed"), want: false},
		{name: "context canceled", err: context.Canceled, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shard.IsRetryableApplyErr(tc.err))
		})
	}
}

// TestRetryClassification_SurvivesRPCBoundary pins the end-to-end contract
// that was broken in production: every transient condition a leader-side
// Store.Apply can raise must still classify as retryable AFTER crossing the
// RPC boundary through toRPCError. (Before typed codes, "leadership lost"
// crossed as codes.Internal and the import client saw it.)
func TestRetryClassification_SurvivesRPCBoundary(t *testing.T) {
	transient := []error{
		shard.ErrNotLeader,
		shard.ErrLeadershipLost,
		shard.ErrProposalBackpressure,
		shard.ErrNotStarted,
		shard.ErrAlreadyClosed,
	}
	for _, err := range transient {
		t.Run(err.Error(), func(t *testing.T) {
			crossed := shard.ToRPCError(fmt.Errorf("apply on remote leader: %w", err))
			require.Truef(t, shard.IsRetryableApplyErr(crossed),
				"%v crossed the RPC boundary as %v and became permanent", err, crossed)
		})
	}
}

// TestPermanentClassification_SurvivesRPCBoundary pins the inverse contract:
// a permanent condition must stay permanent after crossing toRPCError. An
// oversized command that crossed as a retryable code would burn the caller's
// whole retry budget on a command that can never commit.
func TestPermanentClassification_SurvivesRPCBoundary(t *testing.T) {
	permanent := []error{
		shard.ErrCommandTooLarge,
		storagestate.ErrStatusReadOnlyWithReason("resource pressure"),
		shard.ErrClassDropped,
	}
	for _, err := range permanent {
		t.Run(err.Error(), func(t *testing.T) {
			crossed := shard.ToRPCError(fmt.Errorf("apply on remote leader: %w", err))
			require.Falsef(t, shard.IsRetryableApplyErr(crossed),
				"%v crossed the RPC boundary as %v and became retryable", err, crossed)
		})
	}
}
