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
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/namespaces"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

var ErrAny = errors.New("any error")

func TestClient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("Verify error on invalid raft address", func(t *testing.T) {
		addr := fmt.Sprintf("localhost:%v", 8013)
		c := NewClient(fakes.NewFakeRPCAddressResolver(addr, ErrAny), 1024*1024*1024, false, logrus.StandardLogger())
		_, err := c.Join(ctx, addr, &cmd.JoinPeerRequest{Id: "Node1", Address: addr, Voter: false})
		require.ErrorIs(t, err, ErrAny)
		require.ErrorContains(t, err, "resolve")

		_, err = c.Notify(ctx, addr, &cmd.NotifyPeerRequest{Id: "Node1", Address: addr})
		require.ErrorIs(t, err, ErrAny)
		require.ErrorContains(t, err, "resolve")

		_, err = c.Remove(ctx, addr, &cmd.RemovePeerRequest{Id: "Node1"})
		require.ErrorIs(t, err, ErrAny)
		require.ErrorContains(t, err, "resolve")

		_, err = c.Apply(context.TODO(), addr, &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_DELETE_CLASS, Class: "C"})
		require.ErrorIs(t, err, ErrAny)
		require.ErrorContains(t, err, "resolve")

		_, err = c.Query(ctx, addr, &cmd.QueryRequest{Type: cmd.QueryRequest_TYPE_GET_CLASSES})
		require.ErrorIs(t, err, ErrAny)
		require.ErrorContains(t, err, "resolve")
	})

	t.Run("Verify error on invalid address dial", func(t *testing.T) {
		// invalid control character in URL
		badAddr := string(byte(0))
		c := NewClient(fakes.NewFakeRPCAddressResolver(badAddr, nil), 1024*1024*1024, false, logrus.StandardLogger())

		_, err := c.Join(ctx, badAddr, &cmd.JoinPeerRequest{Id: "Node1", Address: "abc", Voter: false})
		require.ErrorContains(t, err, "dial")

		_, err = c.Notify(ctx, badAddr, &cmd.NotifyPeerRequest{Id: "Node1", Address: badAddr})
		require.ErrorContains(t, err, "dial")

		_, err = c.Remove(ctx, badAddr, &cmd.RemovePeerRequest{Id: "Node1"})
		require.ErrorContains(t, err, "dial")

		_, err = c.Apply(context.TODO(), badAddr, &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_DELETE_CLASS, Class: "C"})
		require.ErrorContains(t, err, "dial")

		_, err = c.Query(ctx, badAddr, &cmd.QueryRequest{Type: cmd.QueryRequest_TYPE_GET_CLASSES})
		require.ErrorContains(t, err, "dial")
	})
}

type mockClusterService struct {
	cmd.UnimplementedClusterServiceServer
	applyErr error
}

func (m *mockClusterService) Query(ctx context.Context, req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	return nil, status.Error(codes.NotFound, "resource not found")
}

func (m *mockClusterService) Apply(ctx context.Context, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error) {
	if m.applyErr != nil {
		return nil, m.applyErr
	}
	return &cmd.ApplyResponse{}, nil
}

func TestClient_Query_ParseError(t *testing.T) {
	ctx := context.Background()

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	cmd.RegisterClusterServiceServer(s, &mockClusterService{})

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()
	defer s.Stop()

	// https://pkg.go.dev/google.golang.org/grpc#WithContextDialer
	// to know what "passthrough" means.
	// tldr; prefix tells gRPC to use the passthrough resolver which directly uses the target as the address
	conn, err := grpc.NewClient("passthrough:bufnet",
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := cmd.NewClusterServiceClient(conn)

	_, err = client.Query(ctx, &cmd.QueryRequest{Type: cmd.QueryRequest_TYPE_RESOLVE_ALIAS})
	require.Error(t, err)

	parsedErr := fromRPCError(err)
	require.Error(t, parsedErr)
	require.ErrorIs(t, parsedErr, schemaUC.ErrNotFound)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}

// TestFromRPCError_SentinelRoundTrip covers the round-trip from
// toRPCError on the server to fromRPCError on the client for every
// namespace and leadership sentinel. After this round-trip a caller must
// be able to errors.Is the returned error to the original sentinel — the
// RPC pair is the only sentinel re-chain point.
func TestFromRPCError_SentinelRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		send error
	}{
		{name: "ErrAlreadyExists", send: namespaces.ErrAlreadyExists},
		{name: "ErrBadRequest", send: namespaces.ErrBadRequest},
		{name: "ErrInvalidState", send: namespaces.ErrInvalidState},
		{name: "ErrInvalidStateTransition", send: namespaces.ErrInvalidStateTransition},
		{name: "ErrNamespaceDeleting", send: namespaces.ErrNamespaceDeleting},
		{name: "ErrNamespaceGone", send: namespaces.ErrNamespaceGone},
		{name: "ErrNamespaceNotEmpty", send: namespaces.ErrNamespaceNotEmpty},
		{name: "ErrNamespaceSuspended", send: namespaces.ErrNamespaceSuspended},
		{name: "ErrCollectionSuspended", send: namespaces.ErrCollectionSuspended},
		{name: "ErrNamespaceResuming", send: namespaces.ErrNamespaceResuming},
		{name: "ErrStateChangedConcurrently", send: namespaces.ErrStateChangedConcurrently},
		{name: "ErrNotFound", send: namespaces.ErrNotFound},
		{name: "ErrNotLeader", send: types.ErrNotLeader},
		{name: "ErrLeaderNotFound", send: types.ErrLeaderNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wireErr := toRPCError(tc.send)
			parsed := fromRPCError(wireErr)
			require.ErrorIs(t, parsed, tc.send)
		})
	}
}

// TestClient_Apply_ReChainsNamespaceSentinels exercises the live
// Client.Apply -> mock server -> client return path. The leader's apply
// can return any namespace sentinel when a follower forwards a
// create-like request; the round-trip must preserve the typed sentinel
// so handler errors.Is checks classify correctly.
func TestClient_Apply_ReChainsNamespaceSentinels(t *testing.T) {
	tests := []struct {
		name string
		send error
	}{
		{name: "ErrAlreadyExists", send: namespaces.ErrAlreadyExists},
		{name: "ErrBadRequest", send: namespaces.ErrBadRequest},
		{name: "ErrInvalidState", send: namespaces.ErrInvalidState},
		{name: "ErrInvalidStateTransition", send: namespaces.ErrInvalidStateTransition},
		{name: "ErrNamespaceDeleting", send: namespaces.ErrNamespaceDeleting},
		{name: "ErrNamespaceGone", send: namespaces.ErrNamespaceGone},
		{name: "ErrNamespaceNotEmpty", send: namespaces.ErrNamespaceNotEmpty},
		{name: "ErrNamespaceSuspended", send: namespaces.ErrNamespaceSuspended},
		{name: "ErrCollectionSuspended", send: namespaces.ErrCollectionSuspended},
		{name: "ErrNamespaceResuming", send: namespaces.ErrNamespaceResuming},
		{name: "ErrNotFound", send: namespaces.ErrNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			lis := bufconn.Listen(1024 * 1024)
			s := grpc.NewServer()
			cmd.RegisterClusterServiceServer(s, &mockClusterService{applyErr: toRPCError(tc.send)})

			go func() {
				_ = s.Serve(lis)
			}()
			defer s.Stop()

			conn, err := grpc.NewClient("passthrough:bufnet",
				grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
					return lis.Dial()
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			require.NoError(t, err)
			defer conn.Close()

			cl := &Client{addrResolver: fakes.NewFakeRPCAddressResolver("bufnet", nil), rpcMessageMaxSize: 1024 * 1024, logger: logrus.StandardLogger()}
			cl.leaderRpcConn = conn
			cl.leaderRaftAddr = "bufnet"

			_, err = cl.Apply(ctx, "bufnet", &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_ADD_CLASS})
			require.Error(t, err)
			require.ErrorIs(t, err, tc.send)
		})
	}
}

// TestFromRPCError_NotFoundDisambiguation asserts that a NotFound code
// re-chains the namespace sentinel when the message identifies it,
// otherwise falls back to schemaUC.ErrNotFound.
func TestFromRPCError_NotFoundDisambiguation(t *testing.T) {
	// Schema-flavour NotFound — message does not mention the namespace sentinel.
	parsed := fromRPCError(toRPCError(schemaUC.ErrNotFound))
	require.ErrorIs(t, parsed, schemaUC.ErrNotFound)

	// Namespace-gone uses the same code but is disambiguated by message.
	parsed = fromRPCError(toRPCError(namespaces.ErrNamespaceGone))
	require.ErrorIs(t, parsed, namespaces.ErrNamespaceGone)

	// namespaces.ErrNotFound also uses NotFound — the message must drive
	// re-chain to the namespace sentinel, not schemaUC.ErrNotFound, so the
	// REST handler's errors.Is mapping returns 404 instead of 500.
	parsed = fromRPCError(toRPCError(namespaces.ErrNotFound))
	require.ErrorIs(t, parsed, namespaces.ErrNotFound)
	require.NotErrorIs(t, parsed, schemaUC.ErrNotFound)
}

// TestFromRPCError_SentinelMessagesMutuallyNonSubstring pins that within
// each code arm that re-chains by message, no sentinel message is a
// substring of another, so fromRPCError can't chain the wrong sentinel.
func TestFromRPCError_SentinelMessagesMutuallyNonSubstring(t *testing.T) {
	type sentinel struct {
		name string
		err  error
	}
	arms := []struct {
		name      string
		sentinels []sentinel
	}{
		{
			name: "FailedPrecondition",
			sentinels: []sentinel{
				{name: "ErrNamespaceSuspended", err: namespaces.ErrNamespaceSuspended},
				{name: "ErrCollectionSuspended", err: namespaces.ErrCollectionSuspended},
				{name: "ErrNamespaceResuming", err: namespaces.ErrNamespaceResuming},
				{name: "ErrNamespaceDeleting", err: namespaces.ErrNamespaceDeleting},
				{name: "ErrNamespaceNotEmpty", err: namespaces.ErrNamespaceNotEmpty},
				{name: "ErrInvalidState", err: namespaces.ErrInvalidState},
				{name: "ErrInvalidStateTransition", err: namespaces.ErrInvalidStateTransition},
				{name: "ErrStateChangedConcurrently", err: namespaces.ErrStateChangedConcurrently},
				{name: "ErrMTDisabled", err: schema.ErrMTDisabled},
			},
		},
		{
			name: "NotLeaderRPCCode",
			sentinels: []sentinel{
				{name: "ErrNotLeader", err: types.ErrNotLeader},
				{name: "ErrLeaderNotFound", err: types.ErrLeaderNotFound},
			},
		},
	}
	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			for _, a := range arm.sentinels {
				for _, b := range arm.sentinels {
					if a.name == b.name {
						continue
					}
					require.NotContains(t, a.err.Error(), b.err.Error(),
						"%s message must not contain %s message", a.name, b.name)
				}
			}
		})
	}
}

// TestFromRPCError_HashicorpLeadershipSentinelsRoundTrip pins that the raw
// hashicorp sentinels a leader-local apply returns survive the hop as a
// not-leader code. Without the types.IsNoLeader arm they render 500 instead
// of the documented 503.
func TestFromRPCError_HashicorpLeadershipSentinelsRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		send error
	}{
		{name: "raft.ErrLeadershipLost", send: raft.ErrLeadershipLost},
		{name: "raft.ErrNotLeader", send: raft.ErrNotLeader},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wire := toRPCError(test.send)
			require.Equal(t, NotLeaderRPCCode, status.Code(wire))

			parsed := fromRPCError(wire)
			require.True(t, types.IsNoLeader(parsed),
				"IsNoLeader must hold after the hop, else the handler renders 500")
		})
	}
}

// TestFromRPCError_UnrecognizedNotLeaderMessagePassesThrough pins that a
// NotLeaderRPCCode status whose message names no leadership sentinel — the
// code is also gRPC's own ResourceExhausted — is returned unchanged.
func TestFromRPCError_UnrecognizedNotLeaderMessagePassesThrough(t *testing.T) {
	wireErr := status.Error(NotLeaderRPCCode, "grpc: received message larger than max")
	parsed := fromRPCError(wireErr)

	require.Equal(t, wireErr, parsed)
	require.NotErrorIs(t, parsed, types.ErrNotLeader)
	require.NotErrorIs(t, parsed, types.ErrLeaderNotFound)
}

// TestDeterministicRejections_NotRetryable checks that these rejections map
// to FailedPrecondition and that the code is not in the Apply/Query retry
// set, so the caller sees them immediately instead of after the full backoff
// schedule. Retrying is the caller's decision, not the transport's.
func TestDeterministicRejections_NotRetryable(t *testing.T) {
	sentinels := []struct {
		name string
		send error
	}{
		{name: "ErrNamespaceResuming", send: namespaces.ErrNamespaceResuming},
		{name: "ErrStateChangedConcurrently", send: namespaces.ErrStateChangedConcurrently},
	}
	for _, tc := range sentinels {
		t.Run(tc.name, func(t *testing.T) {
			st, ok := status.FromError(toRPCError(tc.send))
			require.True(t, ok)
			require.Equal(t, codes.FailedPrecondition, st.Code())
		})
	}
	require.NotContains(t, serviceConfig, "FAILED_PRECONDITION")
}
