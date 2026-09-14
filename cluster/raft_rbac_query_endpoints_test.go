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
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// oldLeaderErr is the exact error a leader without TYPE_GET_ROLES_FOR_USER_LIST returns over gRPC.
var oldLeaderErr = status.Error(codes.Internal, "unknown command type 35: consider upgrading to newer version")

// subjectsSharingAnID returns a db user, an oidc user and a group with one id, and the
// role each holds keyed by conv.SubjectKey. A lookup that reuses one subject's fields for
// another returns the wrong role.
func subjectsSharingAnID() ([]authorization.Subject, map[string][]string) {
	subjects := []authorization.Subject{
		{ID: "alice", AuthType: authentication.AuthTypeDb},
		{ID: "alice", AuthType: authentication.AuthTypeOIDC},
		{ID: "alice", AuthType: authentication.AuthTypeOIDC, IsGroup: true},
	}
	roles := map[string][]string{
		"db:alice":    {"db-role"},
		"oidc:alice":  {"oidc-role"},
		"group:alice": {"group-role"},
	}
	return subjects, roles
}

func roleNames(roles map[string]map[string][]authorization.Policy) map[string][]string {
	out := make(map[string][]string, len(roles))
	for key, subjectRoles := range roles {
		out[key] = slices.Sorted(maps.Keys(subjectRoles))
	}
	return out
}

func TestGetRolesForSubjectsWithFallback(t *testing.T) {
	subjects, names := subjectsSharingAnID()
	policy := []authorization.Policy{{Resource: authorization.Cluster(), Domain: authorization.ClusterDomain, Verb: authorization.READ}}
	roles := map[string]map[string][]authorization.Policy{}
	for key, subjectRoles := range names {
		roles[key] = map[string][]authorization.Policy{subjectRoles[0]: policy}
	}
	// singleRoles holds what each subject's singular lookup answers, keyed by the whole subject.
	singleRoles := map[authorization.Subject]map[string][]authorization.Policy{
		subjects[0]: roles["db:alice"],
		subjects[1]: roles["oidc:alice"],
		subjects[2]: roles["group:alice"],
	}
	payload, err := json.Marshal(cmd.QueryGetRolesForSubjectsResponse{Roles: roles})
	require.NoError(t, err)

	type answer struct {
		resp *cmd.QueryResponse
		err  error
	}
	plural := answer{resp: &cmd.QueryResponse{Payload: payload}}
	oldLeader := answer{err: oldLeaderErr}

	tests := []struct {
		name     string
		subjects []authorization.Subject
		// answers are returned by successive queries. The lookup runs once per answer.
		answers     []answer
		failSubject *authorization.Subject
		wantRoles   map[string]map[string][]authorization.Policy
		wantErr     bool
		wantSingle  []authorization.Subject
		wantWarns   int
	}{
		{
			name:      "leader answers the plural query",
			subjects:  subjects,
			answers:   []answer{plural},
			wantRoles: roles,
		},
		{
			name:       "leader without the query type is asked per subject",
			subjects:   subjects,
			answers:    []answer{oldLeader},
			wantRoles:  roles,
			wantSingle: subjects,
			wantWarns:  1,
		},
		{
			name:       "wrapped unknown query type error still falls back",
			subjects:   subjects,
			answers:    []answer{{err: fmt.Errorf("query leader: %w", oldLeaderErr)}},
			wantRoles:  roles,
			wantSingle: subjects,
			wantWarns:  1,
		},
		{
			name:     "leader answering ErrUnknownCommand falls back",
			subjects: subjects,
			answers: []answer{{err: errors.Join(
				status.Error(codes.Unimplemented, "unknown command type 35"), types.ErrUnknownCommand)}},
			wantRoles:  roles,
			wantSingle: subjects,
			wantWarns:  1,
		},
		{
			name:     "leader fault is returned without falling back",
			subjects: subjects,
			answers:  []answer{{err: status.Error(codes.Internal, "raft: leader lost")}},
			wantErr:  true,
		},
		{
			name:     "missing leader is returned without falling back",
			subjects: subjects,
			answers:  []answer{{err: types.ErrLeaderNotFound}},
			wantErr:  true,
		},
		{
			name:     "unreadable payload is an error",
			subjects: subjects,
			answers:  []answer{{resp: &cmd.QueryResponse{Payload: []byte("not json")}}},
			wantErr:  true,
		},
		{
			name:        "one subject failing in the fallback fails the lookup",
			subjects:    subjects,
			answers:     []answer{oldLeader},
			failSubject: &subjects[1],
			wantErr:     true,
			wantSingle:  subjects[:2],
			wantWarns:   1,
		},
		{
			name:       "a leader change between calls is noticed on the next call",
			subjects:   subjects,
			answers:    []answer{oldLeader, plural},
			wantRoles:  roles,
			wantSingle: subjects,
			wantWarns:  1,
		},
		{
			name:      "zero subjects issue no query",
			wantRoles: map[string]map[string][]authorization.Policy{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			queries := 0
			query := func(req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
				require.Less(t, queries, len(tt.answers), "unexpected query")
				require.Equal(t, cmd.QueryRequest_TYPE_GET_ROLES_FOR_USER_LIST, req.Type)
				var sub cmd.QueryGetRolesForSubjectsRequest
				require.NoError(t, json.Unmarshal(req.SubCommand, &sub))
				require.Equal(t, tt.subjects, sub.Subjects)
				a := tt.answers[queries]
				queries++
				return a.resp, a.err
			}
			var singleCalls []authorization.Subject
			single := func(s authorization.Subject) (map[string][]authorization.Policy, error) {
				singleCalls = append(singleCalls, s)
				if tt.failSubject != nil && s == *tt.failSubject {
					return nil, errors.New("leader lost")
				}
				return singleRoles[s], nil
			}

			var got map[string]map[string][]authorization.Policy
			var err error
			for range max(1, len(tt.answers)) {
				got, err = getRolesForSubjectsWithFallback(query, single, tt.subjects, logger)
			}

			require.Equal(t, len(tt.answers), queries)
			require.Equal(t, tt.wantSingle, singleCalls)
			require.Len(t, hook.AllEntries(), tt.wantWarns)
			for _, entry := range hook.AllEntries() {
				require.Equal(t, logrus.WarnLevel, entry.Level)
			}
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantRoles, got)
		})
	}
}

// storeLeaderClient answers a follower's queries from store, and answers
// TYPE_GET_ROLES_FOR_USER_LIST the way a leader without that type does while lacksUserList is set.
type storeLeaderClient struct {
	store         *Store
	lacksUserList bool
	sent          []cmd.QueryRequest_Type
}

func (c *storeLeaderClient) Query(_ context.Context, _ string, req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	c.sent = append(c.sent, req.Type)
	if c.lacksUserList && req.Type == cmd.QueryRequest_TYPE_GET_ROLES_FOR_USER_LIST {
		return nil, oldLeaderErr
	}
	return c.store.Query(req)
}

func (c *storeLeaderClient) Apply(context.Context, string, *cmd.ApplyRequest) (*cmd.ApplyResponse, error) {
	return nil, errors.New("not implemented")
}

func (c *storeLeaderClient) Remove(context.Context, string, *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error) {
	return nil, errors.New("not implemented")
}

func (c *storeLeaderClient) Join(context.Context, string, *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error) {
	return nil, errors.New("not implemented")
}

// followInMemoryLeader makes store a follower of an in-memory single-node leader, so
// Raft.Query sends every query through the Raft's client.
func followInMemoryLeader(t *testing.T, store *Store) {
	t.Helper()
	config := func(id string) *raft.Config {
		c := raft.DefaultConfig()
		c.LocalID = raft.ServerID(id)
		c.Logger = hclog.NewNullLogger()
		c.HeartbeatTimeout = 50 * time.Millisecond
		c.ElectionTimeout = 50 * time.Millisecond
		c.LeaderLeaseTimeout = 50 * time.Millisecond
		c.CommitTimeout = 5 * time.Millisecond
		return c
	}
	leaderAddr, leaderTransport := raft.NewInmemTransport("leader")
	followerAddr, followerTransport := raft.NewInmemTransport("follower")
	leaderTransport.Connect(followerAddr, followerTransport)
	followerTransport.Connect(leaderAddr, leaderTransport)

	leaderConfig := config("leader")
	leaderStore := raft.NewInmemStore()
	leaderSnapshots := raft.NewInmemSnapshotStore()
	require.NoError(t, raft.BootstrapCluster(leaderConfig, leaderStore, leaderStore, leaderSnapshots, leaderTransport,
		raft.Configuration{Servers: []raft.Server{{ID: "leader", Address: leaderAddr}}}))
	leader, err := raft.NewRaft(leaderConfig, &raft.MockFSM{}, leaderStore, leaderStore, leaderSnapshots, leaderTransport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = leader.Shutdown().Error() })

	followerStore := raft.NewInmemStore()
	follower, err := raft.NewRaft(config("follower"), &raft.MockFSM{}, followerStore, followerStore,
		raft.NewInmemSnapshotStore(), followerTransport)
	require.NoError(t, err)
	t.Cleanup(func() { _ = follower.Shutdown().Error() })

	require.Eventually(t, func() bool { return leader.State() == raft.Leader }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, leader.AddNonvoter("follower", followerAddr, 0, 0).Error())
	require.Eventually(t, func() bool {
		addr, _ := follower.LeaderWithID()
		return addr == leaderAddr
	}, 5*time.Second, 10*time.Millisecond)
	store.raft = follower
}

func TestRaftGetRolesForSubjectsOnFollower(t *testing.T) {
	subjects, wantRoles := subjectsSharingAnID()
	m := NewMockStore(t, "Node-1", 0)
	rbacStores := newRolesAndUsersStores(t, usecasesNamespaces.NewMockExisterInState(t, nil))
	rbacStores.assignRoles(t, wantRoles)
	m.store.authZManager = rbacStores.authZManager
	followInMemoryLeader(t, m.store)
	leader := &storeLeaderClient{store: m.store}
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, leader)
	require.False(t, m.store.IsLeader())

	userList, user := cmd.QueryRequest_TYPE_GET_ROLES_FOR_USER_LIST, cmd.QueryRequest_TYPE_GET_ROLES_FOR_USER
	// The rounds run in order against one Raft, so the last one fails if the fallback is remembered.
	rounds := []struct {
		name          string
		lacksUserList bool
		wantSent      []cmd.QueryRequest_Type
	}{
		{name: "leader answers the plural query", wantSent: []cmd.QueryRequest_Type{userList}},
		{
			name:          "leader without the query type is asked per subject",
			lacksUserList: true,
			wantSent:      []cmd.QueryRequest_Type{userList, user, user, user},
		},
		{name: "leader that learned the query type answers it again", wantSent: []cmd.QueryRequest_Type{userList}},
	}
	for _, round := range rounds {
		leader.lacksUserList, leader.sent = round.lacksUserList, nil
		got, err := srv.GetRolesForSubjects(subjects)
		require.NoError(t, err, round.name)
		require.Equal(t, wantRoles, roleNames(got), round.name)
		require.Equal(t, round.wantSent, leader.sent, round.name)
	}
}
