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

package db

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

const (
	localHostAddr  = "10.0.0.1:7100"
	remoteHostAddr = "10.0.0.2:7100"
	replLocalNode  = "node-local"
)

// stubResolver resolves only the local node, to a configurable address.
// An empty localAddr simulates a node that is not yet resolvable.
type stubResolver struct {
	localAddr string
}

func (r stubResolver) NodeHostname(nodeName string) (string, bool) {
	if nodeName == replLocalNode && r.localAddr != "" {
		return r.localAddr, true
	}
	return "", false
}
func (r stubResolver) NodeCount() int            { return 1 }
func (r stubResolver) AllHostnames() []string    { return nil }
func (r stubResolver) NodeAddress(string) string { return "" }
func (r stubResolver) AllOtherClusterMembers(int) map[string]string {
	return nil
}

// recordingRemote is a replica.Client whose tested methods record the host
// they were dispatched to. Untested methods stay nil (embedded interface) and
// panic if called, which also guards against unexpected fan-out.
type recordingRemote struct {
	replica.Client
	calledHost string
}

func (r *recordingRemote) FetchObject(_ context.Context, host, _, _ string,
	_ strfmt.UUID, _ search.SelectProperties, _ additional.Properties, _ int,
) (replica.Replica, error) {
	r.calledHost = host
	return replica.Replica{ID: "from-remote"}, nil
}

func (r *recordingRemote) PutObject(_ context.Context, host, _, _, _ string,
	_ *storobj.Object, _ uint64,
) (replica.SimpleResponse, error) {
	r.calledHost = host
	return replica.SimpleResponse{Errors: []replicaerrors.Error{{Msg: "from-remote"}}}, nil
}

func (r *recordingRemote) Commit(_ context.Context, host, _, _, _ string, resp any) error {
	r.calledHost = host
	if target, ok := resp.(*replica.SimpleResponse); ok {
		target.Errors = []replicaerrors.Error{{Msg: "from-remote"}}
	}
	return nil
}

func (r *recordingRemote) Abort(_ context.Context, host, _, _, _ string) (replica.SimpleResponse, error) {
	r.calledHost = host
	return replica.SimpleResponse{Errors: []replicaerrors.Error{{Msg: "from-remote"}}}, nil
}

// newRemoteOnlyClient builds a client whose local *DB is nil. This is safe as
// long as the test only exercises remote (non-local) hosts: the local branch
// is never taken, so *DB is never dereferenced.
func newRemoteOnlyClient(localAddr string) (*routingReplicationClient, *recordingRemote) {
	remote := &recordingRemote{}
	c := newRoutingReplicationClient(remote, nil, stubResolver{localAddr: localAddr}, replLocalNode)
	return c, remote
}

func TestLocalReplicationClient_isLocal(t *testing.T) {
	tests := []struct {
		name      string
		localAddr string
		host      string
		want      bool
	}{
		{name: "matches local host", localAddr: localHostAddr, host: localHostAddr, want: true},
		{name: "different host is remote", localAddr: localHostAddr, host: remoteHostAddr, want: false},
		{name: "empty host is never local", localAddr: localHostAddr, host: "", want: false},
		{name: "unresolved local falls back to remote", localAddr: "", host: localHostAddr, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newRemoteOnlyClient(tt.localAddr)
			assert.Equal(t, tt.want, c.isLocal(tt.host))
		})
	}
}

// Once the local address becomes resolvable it is memoised and reused.
func TestLocalReplicationClient_localHostMemoised(t *testing.T) {
	c, _ := newRemoteOnlyClient(localHostAddr)
	require.Equal(t, localHostAddr, c.localHost())
	require.NotNil(t, c.cachedHost.Load())
	require.Equal(t, localHostAddr, *c.cachedHost.Load())
}

// A still-unresolvable local address must not be cached, so a later successful
// resolution is still observed.
func TestLocalReplicationClient_localHostNotCachedWhenUnresolved(t *testing.T) {
	c, _ := newRemoteOnlyClient("")
	require.Equal(t, "", c.localHost())
	require.Nil(t, c.cachedHost.Load())
}

// For a non-local host every method must delegate to the wrapped network
// client (and never touch the local *DB, which is nil here).
func TestLocalReplicationClient_remoteDelegation(t *testing.T) {
	t.Run("FetchObject", func(t *testing.T) {
		c, remote := newRemoteOnlyClient(localHostAddr)
		rep, err := c.FetchObject(context.Background(), remoteHostAddr, "C", "S", "id", nil, additional.Properties{}, 0)
		require.NoError(t, err)
		assert.Equal(t, strfmt.UUID("from-remote"), rep.ID)
		assert.Equal(t, remoteHostAddr, remote.calledHost)
	})

	t.Run("PutObject", func(t *testing.T) {
		c, remote := newRemoteOnlyClient(localHostAddr)
		resp, err := c.PutObject(context.Background(), remoteHostAddr, "C", "S", "req-1", nil, 0)
		require.NoError(t, err)
		require.Len(t, resp.Errors, 1)
		assert.Equal(t, "from-remote", resp.Errors[0].Msg)
		assert.Equal(t, remoteHostAddr, remote.calledHost)
	})

	t.Run("Commit", func(t *testing.T) {
		c, remote := newRemoteOnlyClient(localHostAddr)
		var resp replica.SimpleResponse
		err := c.Commit(context.Background(), remoteHostAddr, "C", "S", "req-1", &resp)
		require.NoError(t, err)
		require.Len(t, resp.Errors, 1)
		assert.Equal(t, "from-remote", resp.Errors[0].Msg)
		assert.Equal(t, remoteHostAddr, remote.calledHost)
	})

	t.Run("Abort", func(t *testing.T) {
		c, remote := newRemoteOnlyClient(localHostAddr)
		resp, err := c.Abort(context.Background(), remoteHostAddr, "C", "S", "req-1")
		require.NoError(t, err)
		require.Len(t, resp.Errors, 1)
		assert.Equal(t, "from-remote", resp.Errors[0].Msg)
		assert.Equal(t, remoteHostAddr, remote.calledHost)
	})
}

func TestAssignCommitResponse(t *testing.T) {
	simple := replica.SimpleResponse{Errors: []replicaerrors.Error{{Msg: "x"}}}
	batch := replica.DeleteBatchResponse{Batch: []replica.UUID2Error{
		{UUID: "u", Error: replicaerrors.Error{Msg: "boom"}},
	}}

	t.Run("SimpleResponse value into *SimpleResponse", func(t *testing.T) {
		var got replica.SimpleResponse
		require.NoError(t, assignCommitResponse(simple, &got, "r"))
		assert.Equal(t, simple, got)
	})
	t.Run("SimpleResponse pointer into *SimpleResponse", func(t *testing.T) {
		var got replica.SimpleResponse
		require.NoError(t, assignCommitResponse(&simple, &got, "r"))
		assert.Equal(t, simple, got)
	})
	// DeleteObjects commit path — regressed before this was supported.
	t.Run("DeleteBatchResponse value into *DeleteBatchResponse", func(t *testing.T) {
		var got replica.DeleteBatchResponse
		require.NoError(t, assignCommitResponse(batch, &got, "r"))
		assert.Equal(t, batch, got)
	})
	// A precheck error arrives as a SimpleResponse even on the delete path.
	t.Run("shard error on delete path surfaces", func(t *testing.T) {
		require.Error(t, assignCommitResponse(simple, &replica.DeleteBatchResponse{}, "r"))
	})
	t.Run("nil result is request-not-found", func(t *testing.T) {
		err := assignCommitResponse(nil, &replica.SimpleResponse{}, "req-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
	t.Run("non-pointer container errors", func(t *testing.T) {
		require.Error(t, assignCommitResponse(simple, replica.SimpleResponse{}, "r"))
	})
	t.Run("incompatible result errors", func(t *testing.T) {
		require.Error(t, assignCommitResponse(42, &replica.SimpleResponse{}, "r"))
	})
}

func TestToSimpleResponse(t *testing.T) {
	want := replica.SimpleResponse{Errors: []replicaerrors.Error{{Msg: "x"}}}
	t.Run("value", func(t *testing.T) {
		got, err := toSimpleResponse(want)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})
	t.Run("pointer", func(t *testing.T) {
		got, err := toSimpleResponse(&want)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})
	t.Run("unexpected type", func(t *testing.T) {
		_, err := toSimpleResponse("nope")
		require.Error(t, err)
	})
}
