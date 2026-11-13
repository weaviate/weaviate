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

package grpcweb

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/config"
)

// deadlineRecorder records the deadline of the last call it served.
type deadlineRecorder struct {
	pb.UnimplementedWeaviateServer
	deadline    time.Time
	hasDeadline bool
}

func (d *deadlineRecorder) record(ctx context.Context) {
	d.deadline, d.hasDeadline = ctx.Deadline()
}

func (d *deadlineRecorder) Search(ctx context.Context, _ *pb.SearchRequest) (*pb.SearchReply, error) {
	d.record(ctx)
	return &pb.SearchReply{}, nil
}

func (d *deadlineRecorder) Aggregate(ctx context.Context, _ *pb.AggregateRequest) (*pb.AggregateReply, error) {
	d.record(ctx)
	return &pb.AggregateReply{}, nil
}

func (d *deadlineRecorder) TenantsGet(ctx context.Context, _ *pb.TenantsGetRequest) (*pb.TenantsGetReply, error) {
	d.record(ctx)
	return &pb.TenantsGetReply{}, nil
}

// TestGrpcWebQueryDeadline pins that a grpc-web Search or Aggregate call
// reaches the gRPC service with the write timeout of the server that accepted
// it as its deadline, unless the client asks for a shorter one, and that other
// calls, and calls to a server without a write timeout, keep the client's.
func TestGrpcWebQueryDeadline(t *testing.T) {
	const writeTimeout = time.Hour

	tests := []struct {
		name          string
		method        string
		writeTimeout  time.Duration
		clientTimeout time.Duration
		wantTimeout   time.Duration // 0 for no deadline
	}{
		{name: "aggregate", method: pb.Weaviate_Aggregate_FullMethodName, writeTimeout: writeTimeout, wantTimeout: writeTimeout},
		{name: "search", method: pb.Weaviate_Search_FullMethodName, writeTimeout: writeTimeout, wantTimeout: writeTimeout},
		{
			name: "aggregate with a shorter client timeout", method: pb.Weaviate_Aggregate_FullMethodName,
			writeTimeout: writeTimeout, clientTimeout: time.Minute, wantTimeout: time.Minute,
		},
		{name: "other call", method: pb.Weaviate_TenantsGet_FullMethodName, writeTimeout: writeTimeout},
		{name: "aggregate without a write timeout", method: pb.Weaviate_Aggregate_FullMethodName},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := &deadlineRecorder{}
			grpcServer := grpc.NewServer()
			pb.RegisterWeaviateServer(grpcServer, svc)
			handler, err := NewHandler(grpcServer, &state.State{
				ServerConfig: &config.WeaviateConfig{Config: config.Config{}},
			}, &Tracker{})
			require.NoError(t, err)

			srv := httptest.NewUnstartedServer(Mount("/v1/grpc-web", handler, http.NotFoundHandler(), func() bool { return true }))
			srv.Config.WriteTimeout = tt.writeTimeout
			srv.Start()
			t.Cleanup(srv.Close)

			req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/grpc-web"+tt.method, strings.NewReader("{}"))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Connect-Protocol-Version", "1")
			if tt.clientTimeout > 0 {
				req.Header.Set("Connect-Timeout-Ms", strconv.FormatInt(tt.clientTimeout.Milliseconds(), 10))
			}
			sent := time.Now()
			res, err := srv.Client().Do(req)
			require.NoError(t, err)
			res.Body.Close()
			require.Equal(t, http.StatusOK, res.StatusCode)

			if tt.wantTimeout == 0 {
				require.False(t, svc.hasDeadline)
				return
			}
			require.True(t, svc.hasDeadline)
			assert.WithinRange(t, svc.deadline, sent.Add(tt.wantTimeout), time.Now().Add(tt.wantTimeout))
		})
	}
}
