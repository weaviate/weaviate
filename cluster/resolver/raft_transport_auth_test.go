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

package resolver

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"io"
	"testing"
	"time"

	raftImpl "github.com/hashicorp/raft"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
	clusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
)

const serverTerm = uint64(7)

func newAuthTestTransport(t *testing.T, auth cluster.BasicAuth) *raftImpl.NetworkTransport {
	t.Helper()
	logger, _ := test.NewNullLogger()
	r := NewRaft(RaftConfig{
		ClusterStateReader: clusterMocks.NewMockNodeSelector(),
		LocalName:          "node-1",
	})
	trans, err := r.NewTCPTransport("127.0.0.1:0", nil, 3, 2*time.Second, logger)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, trans.Close()) })
	return trans
}

func serveRecording(t *testing.T, trans *raftImpl.NetworkTransport) <-chan any {
	t.Helper()
	logger, _ := test.NewNullLogger()
	delivered := make(chan any, 16)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	enterrors.GoWrapper(func() {
		for {
			select {
			case <-done:
				return
			case rpc := <-trans.Consumer():
				delivered <- rpc.Command
				switch rpc.Command.(type) {
				case *raftImpl.InstallSnapshotRequest:
					if _, err := io.Copy(io.Discard, rpc.Reader); err != nil {
						rpc.Respond(nil, err)
						continue
					}
					rpc.Respond(&raftImpl.InstallSnapshotResponse{Term: serverTerm, Success: true}, nil)
				default:
					rpc.Respond(&raftImpl.AppendEntriesResponse{Term: serverTerm, Success: true}, nil)
				}
			}
		}
	}, logger)
	return delivered
}

func newAttackerTransport(t *testing.T) *raftImpl.NetworkTransport {
	t.Helper()
	attacker, err := raftImpl.NewTCPTransport("127.0.0.1:0", nil, 1, 2*time.Second, io.Discard)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, attacker.Close()) })
	return attacker
}

func createUserWithKeyLogData(t *testing.T) []byte {
	t.Helper()
	sub, err := json.Marshal(cmd.CreateUserWithKeyRequest{
		UserId:             "attacker",
		ApiKeyFirstLetters: "atta",
		WeakHash:           sha256.Sum256([]byte("attacker-key-123")),
		CreatedAt:          time.Now(),
	})
	require.NoError(t, err)
	data, err := proto.Marshal(&cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_CREATE_USER_WITH_KEY, SubCommand: sub})
	require.NoError(t, err)
	return data
}

func TestRaftTransport_UnauthenticatedAppendEntriesNeverReachesConsumer(t *testing.T) {
	server := newAuthTestTransport(t, cluster.BasicAuth{Username: "u", Password: "p"})
	delivered := serveRecording(t, server)
	attacker := newAttackerTransport(t)
	target := server.LocalAddr()

	var probe raftImpl.AppendEntriesResponse
	errProbe := attacker.AppendEntries("node-1", target, &raftImpl.AppendEntriesRequest{
		RPCHeader: raftImpl.RPCHeader{Addr: []byte(attacker.LocalAddr())},
		Term:      0,
	}, &probe)
	assert.Error(t, errProbe)

	n := uint64(10)
	var resp raftImpl.AppendEntriesResponse
	errInject := attacker.AppendEntries("node-1", target, &raftImpl.AppendEntriesRequest{
		RPCHeader:         raftImpl.RPCHeader{Addr: []byte(attacker.LocalAddr())},
		Term:              probe.Term + 1,
		PrevLogEntry:      0,
		LeaderCommitIndex: n + 1,
		Entries: []*raftImpl.Log{{
			Index: n + 1,
			Term:  probe.Term + 1,
			Type:  raftImpl.LogCommand,
			Data:  createUserWithKeyLogData(t),
		}},
	}, &resp)
	assert.Error(t, errInject)
	assert.False(t, resp.Success)
	assert.Empty(t, delivered, "unauthenticated AppendEntries reached the raft consumer")
}

func TestRaftTransport_UnauthenticatedInstallSnapshotNeverReachesConsumer(t *testing.T) {
	server := newAuthTestTransport(t, cluster.BasicAuth{Username: "u", Password: "p"})
	delivered := serveRecording(t, server)
	attacker := newAttackerTransport(t)

	snapshot := []byte(`{"attacker":"snapshot"}`)
	var resp raftImpl.InstallSnapshotResponse
	err := attacker.InstallSnapshot("node-1", server.LocalAddr(), &raftImpl.InstallSnapshotRequest{
		RPCHeader:          raftImpl.RPCHeader{Addr: []byte(attacker.LocalAddr())},
		SnapshotVersion:    raftImpl.SnapshotVersionMax,
		Term:               serverTerm + 1,
		LastLogIndex:       100,
		LastLogTerm:        serverTerm + 1,
		Configuration:      []byte{},
		ConfigurationIndex: 1,
		Size:               int64(len(snapshot)),
	}, &resp, bytes.NewReader(snapshot))
	assert.Error(t, err)
	assert.False(t, resp.Success)
	assert.Empty(t, delivered, "unauthenticated InstallSnapshot reached the raft consumer")
}
