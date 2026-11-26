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

package test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/weaviate/weaviate/client/cluster"
	"github.com/weaviate/weaviate/client/users"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	grpcconn "github.com/weaviate/weaviate/grpc/conn"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	adminUser   = "admin"
	adminKey    = "admin-key"
	clusterUser = "cluster-user"
	clusterPass = "cluster-pass"
	attackerKey = "attacker-key-123"
)

var (
	verifyEnv = map[string]string{
		"CLUSTER_BASIC_AUTH_USERNAME":  clusterUser,
		"CLUSTER_BASIC_AUTH_PASSWORD":  clusterPass,
		"CLUSTER_BASIC_AUTH_SEND_ONLY": "false",
	}
	sendOnlyEnv = map[string]string{
		"CLUSTER_BASIC_AUTH_USERNAME":  clusterUser,
		"CLUSTER_BASIC_AUTH_PASSWORD":  clusterPass,
		"CLUSTER_BASIC_AUTH_SEND_ONLY": "true",
	}
)

type testCluster struct {
	t       *testing.T
	ctx     context.Context
	compose *docker.DockerCompose
	classes []string
}

func startCluster(ctx context.Context, t *testing.T, env map[string]string) *testCluster {
	t.Helper()
	builder := docker.New().With3NodeCluster().
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithRBAC().
		WithRbacRoots(adminUser).
		WithDbUsers().
		WithWeaviateDataVolumes()
	for k, v := range env {
		builder = builder.WithWeaviateEnv(k, v)
	}
	compose, err := builder.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := compose.Terminate(context.Background()); err != nil {
			t.Errorf("failed to terminate test containers: %v", err)
		}
	})
	return &testCluster{t: t, ctx: ctx, compose: compose}
}

func (c *testCluster) node(n int) *docker.DockerContainer {
	return c.compose.GetWeaviateNode(n + 1)
}

func (c *testCluster) replace(t *testing.T, n int, env map[string]string) {
	t.Helper()
	require.NoError(t, c.compose.ReplaceNodeWithEnv(c.ctx, n, env))
	info, err := c.node(n).Container().Inspect(c.ctx)
	require.NoError(t, err)
	for k, v := range env {
		require.Contains(t, info.Config.Env, k+"="+v)
	}
	c.requireLeader(t)
}

func (c *testCluster) requireLeader(t *testing.T) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for n := range 3 {
			helper.SetupClient(c.node(n).URI())
			resp, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(adminKey))
			if !assert.NoError(ct, err) {
				return
			}
			require.Len(ct, resp.Payload.Statistics, 3)
			leaders := 0
			leaderIDs := map[string]struct{}{}
			for _, st := range resp.Payload.Statistics {
				if st.Raft != nil && st.Raft.State == "Leader" {
					leaders++
				}
				leaderIDs[fmt.Sprint(st.LeaderID)] = struct{}{}
			}
			assert.Equalf(ct, 1, leaders, "leaders seen via weaviate-%d", n)
			assert.Lenf(ct, leaderIDs, 1, "leader ids seen via weaviate-%d: %v", n, leaderIDs)
		}
	}, 90*time.Second, time.Second, "cluster has no single agreed leader")
}

func (c *testCluster) schemaRound(t *testing.T) {
	t.Helper()
	for n := range 3 {
		class := fmt.Sprintf("RaftAuthRound%d", len(c.classes))
		helper.SetupClient(c.node(n).URI())
		helper.CreateClassAuth(t, &models.Class{
			Class:      class,
			Vectorizer: "none",
			Properties: []*models.Property{{Name: "name", DataType: schema.DataTypeText.PropString()}},
		}, adminKey)
		c.classes = append(c.classes, class)
	}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for m := range 3 {
			helper.SetupClient(c.node(m).URI())
			for _, class := range c.classes {
				_, err := helper.GetClassWithoutAssert(t, class, adminKey)
				assert.NoErrorf(ct, err, "class %s on weaviate-%d", class, m)
			}
		}
	}, 60*time.Second, 500*time.Millisecond, "schema did not replicate to every node")
	c.requireLeader(t)
}

func createAttackerUserLogData(t *testing.T) []byte {
	t.Helper()
	sub, err := json.Marshal(cmd.CreateUserWithKeyRequest{
		UserId:             "attacker",
		ApiKeyFirstLetters: attackerKey[:3],
		WeakHash:           sha256.Sum256([]byte(attackerKey)),
		CreatedAt:          time.Now(),
	})
	require.NoError(t, err)
	data, err := proto.Marshal(&cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_CREATE_USER_WITH_KEY, SubCommand: sub})
	require.NoError(t, err)
	return data
}

func attackRaftPort(t *testing.T, raftAddr string) (probeErr, injectErr error) {
	t.Helper()
	trans, err := raft.NewTCPTransport("127.0.0.1:0", nil, 1, 5*time.Second, io.Discard)
	require.NoError(t, err)
	defer func() { require.NoError(t, trans.Close()) }()
	target := raft.ServerAddress(raftAddr)

	var probe raft.AppendEntriesResponse
	probeErr = trans.AppendEntries("victim", target, &raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{Addr: []byte(trans.LocalAddr())},
		Term:      0,
	}, &probe)
	term, next := probe.Term+1, probe.LastLog+1
	if probeErr != nil {
		term, next = 1<<20, 1<<20
	}
	var resp raft.AppendEntriesResponse
	injectErr = trans.AppendEntries("victim", target, &raft.AppendEntriesRequest{
		RPCHeader:         raft.RPCHeader{Addr: []byte(trans.LocalAddr())},
		Term:              term,
		PrevLogEntry:      0,
		LeaderCommitIndex: next,
		Entries:           []*raft.Log{{Index: next, Term: term, Type: raft.LogCommand, Data: createAttackerUserLogData(t)}},
	}, &resp)
	return probeErr, injectErr
}

func clusterServiceClient(t *testing.T, addr string, opts ...grpc.DialOption) cmd.ClusterServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return cmd.NewClusterServiceClient(conn)
}

func (c *testCluster) requireRaftPlaneRejectsAttacker(t *testing.T) {
	t.Helper()
	for n := range 3 {
		node := c.node(n)
		require.NotEmpty(t, node.RaftURI())
		require.NotEmpty(t, node.RaftRPCURI())

		probeErr, injectErr := attackRaftPort(t, node.RaftURI())
		assert.Errorf(t, probeErr, "raft probe accepted by weaviate-%d", n)
		assert.Errorf(t, injectErr, "raft inject accepted by weaviate-%d", n)

		ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
		anon := clusterServiceClient(t, node.RaftRPCURI())
		_, err := anon.Apply(ctx, &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_CREATE_USER_WITH_KEY, SubCommand: []byte("{}")})
		assert.Equalf(t, codes.Unauthenticated, status.Code(err), "Apply without creds on weaviate-%d: %v", n, err)
		_, err = anon.JoinPeer(ctx, &cmd.JoinPeerRequest{Id: "attacker", Address: "10.255.255.1:8300", Voter: true})
		assert.Equalf(t, codes.Unauthenticated, status.Code(err), "JoinPeer without creds on weaviate-%d: %v", n, err)

		wrong := clusterServiceClient(t, node.RaftRPCURI(), grpc.WithUnaryInterceptor(grpcconn.BasicAuthUnaryInterceptor(grpcconn.BasicAuthHeader(clusterUser, "wrong"))))
		_, err = wrong.JoinPeer(ctx, &cmd.JoinPeerRequest{Id: "attacker", Address: "10.255.255.1:8300", Voter: true})
		assert.Equalf(t, codes.Unauthenticated, status.Code(err), "JoinPeer with wrong creds on weaviate-%d: %v", n, err)

		right := clusterServiceClient(t, node.RaftRPCURI(), grpc.WithUnaryInterceptor(grpcconn.BasicAuthUnaryInterceptor(grpcconn.BasicAuthHeader(clusterUser, clusterPass))))
		_, err = right.Query(ctx, &cmd.QueryRequest{Type: cmd.QueryRequest_TYPE_GET_CLASSES})
		assert.NotEqualf(t, codes.Unauthenticated, status.Code(err), "Query with right creds on weaviate-%d: %v", n, err)
		cancel()

		helper.SetupClient(node.URI())
		_, err = helper.Client(t).Users.GetOwnInfo(users.NewGetOwnInfoParams(), helper.CreateAuth(attackerKey))
		var unauthorized *users.GetOwnInfoUnauthorized
		assert.ErrorAsf(t, err, &unauthorized, "attacker key on weaviate-%d", n)
	}
}
