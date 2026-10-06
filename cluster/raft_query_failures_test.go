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

package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/cluster/utils"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type blockingClusterService struct {
	cmd.UnimplementedClusterServiceServer
	started chan struct{}
}

func (b *blockingClusterService) Query(ctx context.Context, _ *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	b.started <- struct{}{}
	<-ctx.Done()
	return nil, ctx.Err()
}

func realGRPCErrors(t *testing.T) (inFlightAbort, closedBeforeCall error) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	svc := &blockingClusterService{started: make(chan struct{}, 1)}
	cmd.RegisterClusterServiceServer(srv, svc)
	enterrors.GoWrapper(func() { _ = srv.Serve(lis) }, logrus.New())
	t.Cleanup(srv.Stop)

	dial := func() *grpc.ClientConn {
		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		return conn
	}

	conn := dial()
	errCh := make(chan error, 1)
	enterrors.GoWrapper(func() {
		_, err := cmd.NewClusterServiceClient(conn).Query(context.Background(), &cmd.QueryRequest{})
		errCh <- err
	}, logrus.New())
	select {
	case <-svc.started:
	case <-time.After(10 * time.Second):
		t.Fatal("query never reached the server")
	}
	require.NoError(t, conn.Close())
	select {
	case inFlightAbort = <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight query was not aborted by Close")
	}

	conn = dial()
	require.NoError(t, conn.Close())
	_, closedBeforeCall = cmd.NewClusterServiceClient(conn).Query(context.Background(), &cmd.QueryRequest{})
	return inFlightAbort, closedBeforeCall
}

func TestLeaderQueryFailureReason(t *testing.T) {
	inFlightAbort, closedBeforeCall := realGRPCErrors(t)
	require.Error(t, inFlightAbort)
	require.Error(t, closedBeforeCall)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name        string
		ctx         context.Context
		err         error
		leaderKnown bool
		want        string
	}{
		{"leader lookup exhausted", context.Background(), types.ErrLeaderNotFound, false, leaderQueryNoLeader},
		{"leader lookup exhausted, unresolved nodes", context.Background(), fmt.Errorf("%w, can not resolve nodes [n2]", types.ErrLeaderNotFound), false, leaderQueryNoLeader},
		{"leader lookup, caller canceled", canceledCtx, context.Canceled, false, leaderQueryCtxCanceled},
		{"leader lookup, caller deadline", context.Background(), context.DeadlineExceeded, false, leaderQueryCtxCanceled},
		{"in-flight rpc aborted by conn swap", context.Background(), inFlightAbort, true, leaderQueryConnClosed},
		{"rpc on already-closed conn", context.Background(), closedBeforeCall, true, leaderQueryConnClosed},
		{"conn closing wrapped by caller", context.Background(), fmt.Errorf("failed to execute query: %w", inFlightAbort), true, leaderQueryConnClosed},
		{"rpc, caller canceled (grpc status)", canceledCtx, status.Error(codes.Canceled, "context canceled"), true, leaderQueryCtxCanceled},
		{"rpc, caller deadline (grpc status)", context.Background(), fmt.Errorf("x: %w", context.DeadlineExceeded), true, leaderQueryCtxCanceled},
		{"rpc, leader unavailable", context.Background(), status.Error(codes.Unavailable, "connection refused"), true, leaderQueryLeaderError},
		{"rpc, leader stepped down", context.Background(), errors.Join(status.Error(codes.ResourceExhausted, "node is not the leader"), types.ErrNotLeader), true, leaderQueryLeaderError},
		{"rpc, dial failure", context.Background(), errors.New("resolve address: unknown node"), true, leaderQueryLeaderError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, leaderQueryFailureReason(tt.ctx, tt.err, tt.leaderKnown))
		})
	}
}

func TestQueryFailureNoLeaderObservability(t *testing.T) {
	tests := []struct {
		name       string
		cancelled  bool
		wantReason string
	}{
		{"no leader after retries", false, leaderQueryNoLeader},
		{"caller context canceled", true, leaderQueryCtxCanceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
			m.store.cfg.ElectionTimeout = 20 * time.Millisecond
			srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
			logger, hook := logrustest.NewNullLogger()
			srv.log = logger

			ctx := context.Background()
			if tt.cancelled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			queryType := cmd.QueryRequest_TYPE_GET_SHARD_OWNER
			counter := monitoring.GetMetrics().SchemaLeaderQueryFailures.
				WithLabelValues(queryType.String(), tt.wantReason)
			before := testutil.ToFloat64(counter)

			_, err := srv.Query(ctx, &cmd.QueryRequest{Type: queryType})
			require.Error(t, err)

			assert.Equal(t, before+1, testutil.ToFloat64(counter))
			entry := hook.LastEntry()
			require.NotNil(t, entry)
			assert.Equal(t, logrus.WarnLevel, entry.Level)
			assert.Contains(t, entry.Message, "query: failed to find leader after retries: ")
			assert.Contains(t, entry.Message, err.Error())
			assert.Equal(t, queryType.String(), entry.Data["query_type"])
			assert.Equal(t, tt.wantReason, entry.Data["reason"])
		})
	}
}
