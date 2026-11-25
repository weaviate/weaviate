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

package rest

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	grpcconn "github.com/weaviate/weaviate/grpc/conn"
	usecasescluster "github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestClusterGRPCDialOptionsWithPublicGRPCTLS(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, &holdingHealth{})

	protocols := http.Protocols{}
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	httpServer := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("content-type"), "application/grpc") {
				grpcServer.ServeHTTP(w, r)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}),
		Protocols: &protocols,
	}
	go func() { _ = httpServer.Serve(lis) }()
	t.Cleanup(func() {
		_ = httpServer.Close()
		grpcServer.Stop()
	})

	appState := &state.State{
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{
				GRPC: config.GRPC{
					CertFile:        "/etc/weaviate/public-grpc.crt",
					KeyFile:         "/etc/weaviate/public-grpc.key",
					MaxMsgSize:      1024 * 1024,
					MaxOpenConns:    4,
					IdleConnTimeout: time.Minute,
				},
				Cluster: usecasescluster.Config{},
			},
		},
	}

	logger, _ := test.NewNullLogger()
	connMgr, err := grpcconn.NewConnManager(
		appState.ServerConfig.Config.GRPC.MaxOpenConns,
		appState.ServerConfig.Config.GRPC.IdleConnTimeout,
		prometheus.NewRegistry(),
		logger,
		clusterGRPCDialOptions(appState)...,
	)
	require.NoError(t, err)
	t.Cleanup(connMgr.Close)

	conn, err := connMgr.GetConn(lis.Addr().String())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
}
