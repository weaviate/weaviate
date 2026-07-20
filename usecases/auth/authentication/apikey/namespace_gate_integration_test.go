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

//go:build integrationTest

package apikey

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

// TestNamespaceGate_SuspendResumeDelete drives a real controller, DB-user store
// and API-key wrapper through the namespace lifecycle, asserting that a user's
// key tracks its namespace state while a second namespace's user is unaffected.
func TestNamespaceGate_SuspendResumeDelete(t *testing.T) {
	logger, _ := test.NewNullLogger()

	ctrl := namespaces.NewController(logger)
	require.NoError(t, ctrl.Create(cmd.Namespace{Name: "customer1", HomeNodes: []string{"node-1"}}, 1))
	require.NoError(t, ctrl.Create(cmd.Namespace{Name: "other", HomeNodes: []string{"node-1"}}, 2))

	wrapper, err := New(config.Config{
		Persistence:    config.Persistence{DataPath: t.TempDir()},
		Authentication: config.Authentication{DBUsers: config.DbUsers{Enabled: true}},
	}, logger, ctrl)
	require.NoError(t, err)

	key1 := createUser(t, wrapper, "u1", "customer1")
	key2 := createUser(t, wrapper, "u2", "other")

	login := func(key string) error {
		_, err := wrapper.ValidateAndExtract(key, nil)
		return err
	}

	// Both namespaces active: both keys authenticate.
	require.NoError(t, login(key1))
	require.NoError(t, login(key2))

	steps := []struct {
		target   cmd.NamespaceState
		index    uint64
		wantBody string // "" means login must succeed
	}{
		{target: cmd.NamespaceStateSuspended, index: 1, wantBody: "unauthorized: instance suspended"},
		{target: cmd.NamespaceStateResuming, index: 2, wantBody: "unauthorized: instance resuming, retry shortly"},
		{target: cmd.NamespaceStateActive, index: 3, wantBody: ""},
		{target: cmd.NamespaceStateDeleting, index: 4, wantBody: "unauthorized: instance unavailable"},
	}

	for _, step := range steps {
		require.NoError(t, ctrl.ChangeState("customer1", step.target, step.index))

		err := login(key1)
		if step.wantBody == "" {
			require.NoError(t, err, "state %s must authenticate", step.target)
		} else {
			require.EqualError(t, err, step.wantBody, "state %s", step.target)
		}

		// The untouched namespace's user authenticates throughout.
		require.NoError(t, login(key2), "state %s must not affect the other namespace", step.target)
	}
}

func createUser(t *testing.T, w *ApiKey, userId, namespace string) (apiKey string) {
	t.Helper()
	apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
	require.NoError(t, err)
	require.NoError(t, w.Dynamic.CreateUser(userId, hash, identifier, "", namespace, time.Now()))
	return apiKey
}
