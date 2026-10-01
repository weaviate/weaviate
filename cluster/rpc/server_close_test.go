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
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestServerCloseIsBounded(t *testing.T) {
	tests := []struct {
		name string
		// setup puts the server into the state Close has to cope with and
		// returns once it is there.
		setup    func(t *testing.T, addr string, handlerEntered <-chan struct{})
		maxClose time.Duration
	}{
		{
			name: "idle client",
			setup: func(t *testing.T, addr string, _ <-chan struct{}) {
				client := NewClient(fakes.NewFakeRPCAddressResolver(addr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				t.Cleanup(client.Close)
				_, err := client.Query(context.Background(), addr, &cmd.QueryRequest{Type: cmd.QueryRequest_TYPE_GET_SCHEMA})
				require.NoError(t, err)
			},
			maxClose: gracefulStopTimeout / 2,
		},
		{
			name: "handler that does not return",
			setup: func(t *testing.T, addr string, handlerEntered <-chan struct{}) {
				client := NewClient(fakes.NewFakeRPCAddressResolver(addr, nil), raftGrpcMessageMaxSize, false, logrus.StandardLogger())
				t.Cleanup(client.Close)
				go client.Apply(context.Background(), addr, &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_ADD_CLASS, Class: "C"})
				select {
				case <-handlerEntered:
				case <-time.After(5 * time.Second):
					t.Fatal("handler was never called")
				}
			},
			maxClose: gracefulStopTimeout + time.Second,
		},
		{
			name: "peer that never acknowledges the drain",
			setup: func(t *testing.T, addr string, _ <-chan struct{}) {
				conn, err := net.Dial("tcp", addr)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })
				_, err = conn.Write([]byte(http2.ClientPreface))
				require.NoError(t, err)
				fr := http2.NewFramer(conn, conn)
				require.NoError(t, fr.WriteSettings())
				// A ping ack means the server is serving this connection. After
				// it the peer goes silent.
				require.NoError(t, fr.WritePing(false, [8]byte{1}))
				for {
					f, err := fr.ReadFrame()
					require.NoError(t, err)
					if p, ok := f.(*http2.PingFrame); ok && p.IsAck() {
						return
					}
				}
			},
			maxClose: gracefulStopTimeout + time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := fmt.Sprintf("localhost:%v", utils.MustGetFreeTCPPort())
			handlerEntered := make(chan struct{}, 1)
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			executor := &MockExecutor{ef: func() error {
				handlerEntered <- struct{}{}
				<-release
				return nil
			}}
			logger, _ := logrustest.NewNullLogger()
			sm := monitoring.NewGRPCServerMetrics("rpc_test", prometheus.NewPedanticRegistry())
			server := NewServer(&MockMembers{leader: addr}, executor, addr, raftGrpcMessageMaxSize, false, sm, logger)
			require.NoError(t, server.Open())

			tt.setup(t, addr, handlerEntered)

			closed := make(chan struct{})
			start := time.Now()
			go func() {
				server.Close()
				close(closed)
			}()
			select {
			case <-closed:
				require.Less(t, time.Since(start), tt.maxClose)
			case <-time.After(tt.maxClose):
				t.Fatalf("Close did not return within %s", tt.maxClose)
			}
		})
	}
}
