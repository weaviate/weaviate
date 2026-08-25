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

// newDynUserQueryStore builds a Store whose dynUserManager is backed by a real
// *apikey.DBUser with one seeded user. NewMockStore is not usable here: it leaves
// DynamicUserController nil, so the manager answers every query with an empty
// payload and the dispatch assertions would pass without testing anything. It
// returns the store plus the seeded user's id and identifier.
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
}
