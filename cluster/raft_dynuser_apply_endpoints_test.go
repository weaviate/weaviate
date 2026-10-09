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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
)

// startDynUserRaft starts a single-node Raft whose FSM writes users to a real
// DBUser, not NewMockStore's nil one, and creates userID in it.
func startDynUserRaft(t *testing.T, userID string) (*Raft, context.Context, *apikey.DBUser, func()) {
	t.Helper()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	dynUser, err := apikey.NewDBUser(t.TempDir(), false, m.logger, m.cfg.NamespacesController)
	require.NoError(t, err)
	m.cfg.DynamicUserController = dynUser
	s := NewFSM(m.cfg, nil, prometheus.NewPedanticRegistry())
	s.schemaManager.SetReplicationFSM(m.replicationFSM)
	m.store = &s
	srv, ctx, cleanup := startSingleNodeRaft(t, m)

	_, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, srv.CreateUser(ctx, userID, hash, identifier, "", "", time.Now(), time.Time{}))
	return srv, ctx, dynUser, cleanup
}

func TestRaftSetUserExpiration_UnknownUser(t *testing.T) {
	srv, ctx, dynUser, cleanup := startDynUserRaft(t, "u1")
	defer cleanup()
	before, err := dynUser.GetUsers()
	require.NoError(t, err)

	err = srv.SetUserExpiration(ctx, "missing", time.Now().Add(time.Hour))
	require.ErrorContains(t, err, "user missing does not exist")

	after, err := dynUser.GetUsers()
	require.NoError(t, err)
	assert.Equal(t, before, after, "a refused set must change no user")
}

// TestRaftUpdateUserRefusesUnknownField pins that the leader refuses an update
// carrying a field this binary does not know before appending it. A refusal in
// the apply would fail with ErrBadRequest's message instead.
func TestRaftUpdateUserRefusesUnknownField(t *testing.T) {
	const userID = "u1"
	srv, ctx, dynUser, cleanup := startDynUserRaft(t, userID)
	defer cleanup()

	stored := time.Now().Add(time.Hour).UTC()
	require.NoError(t, dynUser.UpdateUser(userID, apikey.UserUpdate{ExpiresAt: &stored}))
	subCommand, err := json.Marshal(map[string]any{
		"UserId":     userID,
		"ExpiresAt":  stored.Add(time.Hour),
		"AllowedIPs": []string{"10.0.0.0/8"},
	})
	require.NoError(t, err)

	_, err = srv.Execute(ctx, &cmd.ApplyRequest{Type: cmd.ApplyRequest_TYPE_UPDATE_USER, SubCommand: subCommand})
	require.ErrorContains(t, err, `unmarshal update-user subcommand: json: unknown field "AllowedIPs"`)

	users, err := dynUser.GetUsers(userID)
	require.NoError(t, err)
	assert.Truef(t, stored.Equal(users[userID].ExpiresAt), "stored %v, want %v", users[userID].ExpiresAt, stored)
}
