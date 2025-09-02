//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/fakes"
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
}

func (m *mockClusterService) Query(ctx context.Context, req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	return nil, status.Error(codes.NotFound, "resource not found")
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

	dialCtx := context.Background()
	conn, err := grpc.DialContext(dialCtx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
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
