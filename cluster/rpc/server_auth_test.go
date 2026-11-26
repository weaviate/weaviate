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
	"sync/atomic"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/fakes"
)

type countingPeers struct{ calls atomic.Int32 }

func (p *countingPeers) Join(string, string, bool) error { p.calls.Add(1); return nil }
func (p *countingPeers) Notify(string, string) error     { p.calls.Add(1); return nil }
func (p *countingPeers) Remove(string) error             { p.calls.Add(1); return nil }
func (p *countingPeers) Leader() string                  { return "" }

type countingFSM struct{ calls atomic.Int32 }

func (f *countingFSM) Execute(context.Context, *cmd.ApplyRequest) (uint64, error) {
	f.calls.Add(1)
	return 1, nil
}

func (f *countingFSM) Query(context.Context, *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	f.calls.Add(1)
	return &cmd.QueryResponse{}, nil
}

func TestClusterServiceBasicAuth(t *testing.T) {
	right := cluster.BasicAuth{Username: "weaviate", Password: "secret"}
	rightSendOnly := cluster.BasicAuth{Username: "weaviate", Password: "secret", SendOnly: true}
	wrong := cluster.BasicAuth{Username: "weaviate", Password: "wrong"}
	wrongSendOnly := cluster.BasicAuth{Username: "weaviate", Password: "wrong", SendOnly: true}

	rpcs := map[string]func(ctx context.Context, cl *Client, addr string) error{
		"JoinPeer": func(ctx context.Context, cl *Client, addr string) error {
			_, err := cl.Join(ctx, addr, &cmd.JoinPeerRequest{Id: "attacker", Address: "10.0.0.66:8300", Voter: true})
			return err
		},
		"RemovePeer": func(ctx context.Context, cl *Client, addr string) error {
			_, err := cl.Remove(ctx, addr, &cmd.RemovePeerRequest{Id: "node-1"})
			return err
		},
		"NotifyPeer": func(ctx context.Context, cl *Client, addr string) error {
			_, err := cl.Notify(ctx, addr, &cmd.NotifyPeerRequest{Id: "attacker", Address: "10.0.0.66:8300"})
			return err
		},
		"Apply": func(ctx context.Context, cl *Client, addr string) error {
			_, err := cl.Apply(ctx, addr, &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_CREATE_USER_WITH_KEY})
			return err
		},
		"Query": func(ctx context.Context, cl *Client, addr string) error {
			_, err := cl.Query(ctx, addr, &cmd.QueryRequest{})
			return err
		},
	}

	tests := []struct {
		server  cluster.BasicAuth
		client  cluster.BasicAuth
		name    string
		allowed bool
	}{
		{name: "off server/no creds client", server: cluster.BasicAuth{}, client: cluster.BasicAuth{}, allowed: true},
		{name: "off server/right client", server: cluster.BasicAuth{}, client: right, allowed: true},
		{name: "off server/wrong client", server: cluster.BasicAuth{}, client: wrong, allowed: true},
		{name: "send only server/no creds client", server: rightSendOnly, client: cluster.BasicAuth{}, allowed: true},
		{name: "send only server/right client", server: rightSendOnly, client: right, allowed: true},
		{name: "send only server/wrong client", server: rightSendOnly, client: wrong, allowed: true},
		{name: "verify server/no creds client", server: right, client: cluster.BasicAuth{}},
		{name: "verify server/wrong client", server: right, client: wrong},
		{name: "verify server/wrong send only client", server: right, client: wrongSendOnly},
		{name: "verify server/right client", server: right, client: right, allowed: true},
		{name: "verify server/right send only client", server: right, client: rightSendOnly, allowed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			addr := fmt.Sprintf("127.0.0.1:%d", utils.MustGetFreeTCPPort())
			peers, fsm := &countingPeers{}, &countingFSM{}
			server := NewServer(peers, fsm, addr, raftGrpcMessageMaxSize, false, tt.server, nil, logger)
			require.NoError(t, server.Open())
			t.Cleanup(server.Close)
			client := NewClient(fakes.NewFakeRPCAddressResolver(addr, nil), raftGrpcMessageMaxSize, false, tt.client, logger)
			t.Cleanup(client.Close)

			for name, call := range rpcs {
				t.Run(name, func(t *testing.T) {
					before := peers.calls.Load() + fsm.calls.Load()
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					err := call(ctx, client, addr)
					after := peers.calls.Load() + fsm.calls.Load()
					if tt.allowed {
						require.NoError(t, err)
						assert.Equal(t, before+1, after)
						return
					}
					require.Error(t, err)
					assert.Equal(t, codes.Unauthenticated, status.Code(err))
					assert.Equal(t, before, after)
				})
			}
		})
	}
}

func TestServiceConfigNeverRetriesUnauthenticated(t *testing.T) {
	assert.NotContains(t, serviceConfig, "UNAUTHENTICATED")
}
