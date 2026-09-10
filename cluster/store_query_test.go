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
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/dbuser"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/fakes"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

// newDynUserQueryStore backs a Store with a real DBUser holding one seeded user
// and returns that user's id and identifier. NewMockStore leaves the controller
// nil, so every query would return empty and the assertions would pass vacuously.
func newDynUserQueryStore(t *testing.T) (*Store, string, string) {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	nsController := usecasesNamespaces.NewController(logger)
	dynUser, err := apikey.NewDBUser(t.TempDir(), false, logger, nsController)
	require.NoError(t, err)

	const userID = "seeded-user"
	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, dynUser.CreateUser(userID, hash, identifier, "", "", time.Now()))

	cfg := Config{
		WorkDir:                t.TempDir(),
		NodeID:                 "node-1",
		Host:                   "localhost",
		RaftPort:               0,
		Voter:                  true,
		BootstrapExpect:        1,
		HeartbeatTimeout:       time.Second,
		ElectionTimeout:        time.Second,
		SnapshotInterval:       2 * time.Second,
		SnapshotThreshold:      125,
		DB:                     fakes.NewMockSchemaExecutor(),
		Parser:                 fakes.NewMockParser(),
		NodeSelector:           mocks.NewMockNodeSelector("localhost"),
		Logger:                 logger,
		ConsistencyWaitTimeout: 50 * time.Millisecond,
		NamespacesController:   nsController,
		DynamicUserController:  dynUser,
		TelemetryEnabled:       true,
	}
	s := NewFSM(cfg, nil, prometheus.NewPedanticRegistry())
	return &s, userID, identifier
}

func TestQueryUserIdentifierExistsDispatch(t *testing.T) {
	t.Run("existing identifier reports exists true", func(t *testing.T) {
		store, _, identifier := newDynUserQueryStore(t)

		sub, err := json.Marshal(cmd.QueryUserIdentifierExistsRequest{UserIdentifier: identifier})
		require.NoError(t, err)

		resp, err := store.Query(&cmd.QueryRequest{
			Type:       cmd.QueryRequest_TYPE_USER_IDENTIFIER_EXISTS,
			SubCommand: sub,
		})
		require.NoError(t, err)

		var out cmd.QueryUserIdentifierExistsResponse
		require.NoError(t, json.Unmarshal(resp.Payload, &out))
		require.True(t, out.Exists, "seeded identifier must report exists=true; false means the dispatch is misrouted to GetUsers")
	})

	t.Run("unknown identifier reports exists false", func(t *testing.T) {
		store, _, _ := newDynUserQueryStore(t)

		sub, err := json.Marshal(cmd.QueryUserIdentifierExistsRequest{UserIdentifier: "no-such-identifier"})
		require.NoError(t, err)

		resp, err := store.Query(&cmd.QueryRequest{
			Type:       cmd.QueryRequest_TYPE_USER_IDENTIFIER_EXISTS,
			SubCommand: sub,
		})
		require.NoError(t, err)

		var out cmd.QueryUserIdentifierExistsResponse
		require.NoError(t, json.Unmarshal(resp.Payload, &out))
		require.False(t, out.Exists)
	})
}

// attachFollowerRaft gives store a real raft instance that is not the leader.
// The node is never bootstrapped, so its configuration is empty, it can never
// win an election and Barrier always reports raft.ErrNotLeader.
func attachFollowerRaft(t *testing.T, store *Store) {
	t.Helper()
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID("node-1")
	cfg.Logger = hclog.NewNullLogger()
	inmem := raft.NewInmemStore()
	_, transport := raft.NewInmemTransport("")

	r, err := raft.NewRaft(cfg, store, inmem, inmem, raft.NewInmemSnapshotStore(), transport)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Shutdown().Error()) })

	store.raft = r
}

func TestQueryExportUsersDispatch(t *testing.T) {
	t.Run("seeded user is returned with an exported credential", func(t *testing.T) {
		store, userID, identifier := newDynUserQueryStore(t)

		sub, err := json.Marshal(cmd.QueryExportUsersRequest{})
		require.NoError(t, err)

		resp, err := store.Query(&cmd.QueryRequest{
			Type:       cmd.QueryRequest_TYPE_EXPORT_USERS,
			SubCommand: sub,
		})
		require.NoError(t, err)

		var out cmd.QueryExportUsersResponse
		require.NoError(t, json.Unmarshal(resp.Payload, &out))

		rec, ok := out.Users[userID]
		require.True(t, ok, "seeded user must appear in the export")
		require.Equal(t, dbuser.ExportStatusExported, rec.Status)
		require.Equal(t, identifier, rec.UserIdentifier)
		require.NotNil(t, rec.SecureHash)
	})

	t.Run("a node that is not the leader refuses to serve the export", func(t *testing.T) {
		store, _, _ := newDynUserQueryStore(t)
		attachFollowerRaft(t, store)

		sub, err := json.Marshal(cmd.QueryExportUsersRequest{})
		require.NoError(t, err)

		_, err = store.Query(&cmd.QueryRequest{
			Type:       cmd.QueryRequest_TYPE_EXPORT_USERS,
			SubCommand: sub,
		})
		require.ErrorContains(t, err, "verify leader before export")
		require.ErrorIs(t, err, raft.ErrNotLeader)
	})

	t.Run("a targeted export skips the leadership check", func(t *testing.T) {
		// Import reads one record at a time through this path and the apply
		// re-validates it, so the quorum round-trip is reserved for the full roster.
		store, userID, _ := newDynUserQueryStore(t)
		attachFollowerRaft(t, store)

		sub, err := json.Marshal(cmd.QueryExportUsersRequest{UserIds: []string{userID}})
		require.NoError(t, err)

		resp, err := store.Query(&cmd.QueryRequest{
			Type:       cmd.QueryRequest_TYPE_EXPORT_USERS,
			SubCommand: sub,
		})
		require.NoError(t, err)

		var out cmd.QueryExportUsersResponse
		require.NoError(t, json.Unmarshal(resp.Payload, &out))
		require.Contains(t, out.Users, userID)
	})

	t.Run("other user queries do not verify leadership", func(t *testing.T) {
		// Only the roster export pays the quorum round-trip; per-user queries sit
		// on the login path.
		store, _, identifier := newDynUserQueryStore(t)
		attachFollowerRaft(t, store)

		sub, err := json.Marshal(cmd.QueryUserIdentifierExistsRequest{UserIdentifier: identifier})
		require.NoError(t, err)

		resp, err := store.Query(&cmd.QueryRequest{
			Type:       cmd.QueryRequest_TYPE_USER_IDENTIFIER_EXISTS,
			SubCommand: sub,
		})
		require.NoError(t, err)

		var out cmd.QueryUserIdentifierExistsResponse
		require.NoError(t, json.Unmarshal(resp.Payload, &out))
		require.True(t, out.Exists)
	})
}
