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

package distributedtask

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// All permanent sentinels round-trip through wrapPermanent → errors.Is
// for both the specific sentinel and the umbrella sentinel.
func TestWrapPermanent_ErrorsIsClassification(t *testing.T) {
	cases := []struct {
		name     string
		sentinel error
	}{
		{"task-not-running", ErrTaskNotRunning},
		{"task-not-exist", ErrTaskDoesNotExist},
		{"unit-already-terminal", ErrUnitAlreadyTerminal},
		{"unit-wrong-node", ErrUnitWrongNode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := wrapPermanent(tc.sentinel, "human message goes here")
			require.True(t, errors.Is(err, tc.sentinel),
				"specific sentinel must be reachable via errors.Is")
			require.True(t, errors.Is(err, ErrPermanentRejection),
				"umbrella sentinel must be reachable via errors.Is for every permanent error")
		})
	}
}

// wrapPermanent embeds the on-wire marker prefix so the receiving side
// can re-hydrate via extractMarkerID. The human-readable phrase is
// preserved AFTER the marker so legacy substring matching on old
// receivers keeps working (cross-version compat).
func TestWrapPermanent_MarkerAndLegacyPhrasingCoexist(t *testing.T) {
	err := wrapPermanent(ErrTaskNotRunning, "task ns/Foo/3 is no longer running")
	msg := err.Error()

	require.Contains(t, msg, "[dtm-perm/task-not-running]",
		"on-wire marker must be present so a new follower can re-hydrate the sentinel")
	require.Contains(t, msg, "is no longer running",
		"legacy phrase must survive so an old follower's substring matcher still works")
}

// A typed sentinel that has been chained through fmt.Errorf("...: %w",
// ...) (as the apply path does in raft_distributed_tasks_apply_endpoints.go)
// must still be classifiable via errors.Is. This is the in-process path.
func TestErrorsIs_SurvivesFmtErrorfWrapping(t *testing.T) {
	inner := errTaskNotRunning("ns", "Foo", 3)
	wrapped := fmt.Errorf("executing command: %w", inner)

	require.True(t, errors.Is(wrapped, ErrTaskNotRunning))
	require.True(t, errors.Is(wrapped, ErrPermanentRejection))
}

// ToRPCError returns a status.Error with PermanentRejectionRPCCode for
// permanent rejections, and nil for everything else (signalling "not
// a DTM permanent rejection; the generic toRPCError path should handle
// it").
func TestToRPCError(t *testing.T) {
	t.Run("nil → nil", func(t *testing.T) {
		require.Nil(t, ToRPCError(nil))
	})
	t.Run("permanent rejection → FailedPrecondition", func(t *testing.T) {
		err := errTaskNotRunning("ns", "Foo", 3)
		got := ToRPCError(err)
		require.NotNil(t, got)
		st, ok := status.FromError(got)
		require.True(t, ok, "must be a gRPC status error")
		require.Equal(t, PermanentRejectionRPCCode, st.Code())
		require.Contains(t, st.Message(), "[dtm-perm/task-not-running]")
	})
	t.Run("non-permanent → nil (caller falls through to generic toRPCError)", func(t *testing.T) {
		require.Nil(t, ToRPCError(errors.New("network blip")))
		require.Nil(t, ToRPCError(errors.New("raft applyTimeout")))
	})
}

// RehydratePermanentRejection reconstructs the specific sentinel from
// the (status.Code, message) pair after a gRPC round-trip. Every
// sentinel must survive.
func TestRehydratePermanentRejection_RoundTripsEverySentinel(t *testing.T) {
	cases := []struct {
		name     string
		sentinel error
	}{
		{"task-not-running", ErrTaskNotRunning},
		{"task-not-exist", ErrTaskDoesNotExist},
		{"unit-already-terminal", ErrUnitAlreadyTerminal},
		{"unit-wrong-node", ErrUnitWrongNode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate the leader's side: wrap → ToRPCError.
			leaderErr := wrapPermanent(tc.sentinel, "any human message")
			onWire := ToRPCError(leaderErr)
			require.NotNil(t, onWire)

			// Simulate the follower's side: status.FromError happens
			// implicitly inside RehydratePermanentRejection.
			rehydrated := RehydratePermanentRejection(onWire)
			require.True(t, errors.Is(rehydrated, tc.sentinel),
				"after rehydration, the specific sentinel must be reachable via errors.Is")
			require.True(t, errors.Is(rehydrated, ErrPermanentRejection),
				"after rehydration, the umbrella sentinel must be reachable via errors.Is")
		})
	}
}

// Forward-compat: a future build of the leader may report a sentinel id
// the current follower does not know. The follower must still classify
// the error as permanent (umbrella sentinel) rather than retrying.
func TestRehydratePermanentRejection_UnknownMarkerStillClassifiedPermanent(t *testing.T) {
	onWire := status.Error(PermanentRejectionRPCCode, "[dtm-perm/future-sentinel-id] some message")

	rehydrated := RehydratePermanentRejection(onWire)
	require.True(t, errors.Is(rehydrated, ErrPermanentRejection),
		"unknown future sentinel ids must still classify as permanent (umbrella sentinel)")
}

// Backward-compat: a status error with the permanent code but WITHOUT
// the marker prefix is still classified as permanent (it could come
// from a peer running an intermediate build).
func TestRehydratePermanentRejection_NoMarkerStillPermanent(t *testing.T) {
	onWire := status.Error(PermanentRejectionRPCCode, "task ns/Foo/3 is no longer running")

	rehydrated := RehydratePermanentRejection(onWire)
	require.True(t, errors.Is(rehydrated, ErrPermanentRejection))
}

// Non-permanent gRPC errors (e.g. Internal) pass through unchanged so
// callers can apply their normal classification.
func TestRehydratePermanentRejection_PassesThroughNonPermanentErrors(t *testing.T) {
	cases := []error{
		status.Error(codes.Internal, "raft applyTimeout"),
		status.Error(codes.Unavailable, "leader lost"),
		errors.New("not a gRPC error at all"),
		nil,
	}
	for _, in := range cases {
		out := RehydratePermanentRejection(in)
		if in == nil {
			require.Nil(t, out)
			continue
		}
		require.False(t, errors.Is(out, ErrPermanentRejection),
			"non-permanent errors must NOT be reclassified as permanent")
	}
}

// extractMarkerID parses the on-wire prefix; covers happy path, no
// marker, and malformed marker.
func TestExtractMarkerID(t *testing.T) {
	cases := map[string]string{
		"[dtm-perm/task-not-running] hello":                  "task-not-running",
		"[dtm-perm/unit-terminal] longer message with [text": "unit-terminal",
		"no marker at all":                                   "",
		"[dtm-perm/] empty id":                               "",
		"[dtm-perm/no-closing-bracket and rest":              "",
	}
	for in, want := range cases {
		require.Equal(t, want, extractMarkerID(in), "input: %q", in)
	}
}

// TestErrorsIs_SurvivesRealGRPCRoundTrip verifies that a permanent
// sentinel stays classifiable via errors.Is even after a real gRPC
// round-trip. We register a generic UnknownServiceHandler so we don't
// have to generate proto stubs for the test — the handler short-
// circuits before unmarshalling, returning the programmed error
// directly.
func TestErrorsIs_SurvivesRealGRPCRoundTrip(t *testing.T) {
	leaderErr := wrapPermanent(ErrUnitAlreadyTerminal,
		"unit u1 in task ns/Foo/3 is already terminal")

	// Server side: a stream handler that immediately returns the wrapped
	// error (after running it through ToRPCError, just like the real
	// cluster/rpc/server.go path).
	handler := func(srv any, stream grpc.ServerStream) error {
		return ToRPCError(leaderErr)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer(grpc.UnknownServiceHandler(handler))
	go func() { _ = srv.Serve(lis) }()
	defer srv.GracefulStop()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	// Invoke an arbitrary unary method; the server's UnknownServiceHandler
	// catches it and returns our programmed error before unmarshalling
	// would run, so passing a no-op proto.Message is sufficient.
	err = conn.Invoke(context.Background(), "/Test/Echo", &emptyMsg{}, &emptyMsg{})
	require.Error(t, err)

	// Before re-hydration, the gRPC transport has stripped the Go error
	// type — errors.Is(err, sentinel) would be false here. The contract
	// of RehydratePermanentRejection is to restore that classification.
	rehydrated := RehydratePermanentRejection(err)

	require.True(t, errors.Is(rehydrated, ErrUnitAlreadyTerminal),
		"errors.Is on the specific sentinel must work after a real gRPC round-trip")
	require.True(t, errors.Is(rehydrated, ErrPermanentRejection),
		"errors.Is on the umbrella sentinel must work after a real gRPC round-trip")

	// Sanity: also verify the gRPC status code is the one the wire
	// contract promises.
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, PermanentRejectionRPCCode, st.Code())
}

// emptyMsg is a no-op proto.Message used to satisfy grpc.Invoke's
// marshalling contract for the dummy server in the round-trip test.
type emptyMsg struct{}

func (e *emptyMsg) Reset()         {}
func (e *emptyMsg) String() string { return "" }
func (e *emptyMsg) ProtoMessage()  {}
