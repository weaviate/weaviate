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

package apikey

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey/keys"
	"github.com/weaviate/weaviate/usecases/config"

	"github.com/sirupsen/logrus/hooks/test"
)

func TestInvalidApiKey(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()

	testCases := []struct {
		staticEnabled bool
		dbEnabled     bool
	}{
		{staticEnabled: true, dbEnabled: false},
		{staticEnabled: false, dbEnabled: true},
		{staticEnabled: true, dbEnabled: true},
	}

	for _, testCase := range testCases {
		t.Run("staticEnabled="+boolToStr(testCase.staticEnabled)+",dbEnabled="+boolToStr(testCase.dbEnabled), func(t *testing.T) {
			conf := config.Config{
				Persistence: config.Persistence{DataPath: t.TempDir()},
				Authentication: config.Authentication{
					DBUsers: config.DbUsers{Enabled: testCase.dbEnabled},
					APIKey:  config.StaticAPIKey{Enabled: testCase.staticEnabled, AllowedKeys: []string{"valid-key"}, Users: []string{"user1"}},
				},
			}
			wrapper, err := New(conf, logger, activeExister{})
			require.NoError(t, err)

			_, err = wrapper.ValidateAndExtract("invalid-key", nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "unauthorized: invalid api key")
		})
	}
}

func TestValidStaticKey(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()

	testCases := []struct {
		staticEnabled bool
		dbEnabled     bool
		expectError   bool
	}{
		{staticEnabled: true, dbEnabled: false, expectError: false},
		{staticEnabled: false, dbEnabled: true, expectError: true},
		{staticEnabled: true, dbEnabled: true, expectError: false},
	}

	for _, testCase := range testCases {
		t.Run("staticEnabled="+boolToStr(testCase.staticEnabled)+",dbEnabled="+boolToStr(testCase.dbEnabled), func(t *testing.T) {
			conf := config.Config{
				Persistence: config.Persistence{DataPath: t.TempDir()},
				Authentication: config.Authentication{
					DBUsers: config.DbUsers{Enabled: testCase.dbEnabled},
					APIKey:  config.StaticAPIKey{Enabled: testCase.staticEnabled, AllowedKeys: []string{"valid-key"}, Users: []string{"user1"}},
				},
			}
			wrapper, err := New(conf, logger, activeExister{})
			require.NoError(t, err)

			principal, err := wrapper.ValidateAndExtract("valid-key", nil)
			if testCase.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "unauthorized: invalid api key")
			} else {
				require.NoError(t, err)
				require.NotNil(t, principal)
			}
		})
	}
}

func TestValidDynamicKey(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()

	testCases := []struct {
		staticEnabled bool
		dbEnabled     bool
		expectError   bool
	}{
		{staticEnabled: true, dbEnabled: false, expectError: true},
		{staticEnabled: false, dbEnabled: true, expectError: false},
		{staticEnabled: true, dbEnabled: true, expectError: false},
	}

	for _, testCase := range testCases {
		t.Run("staticEnabled="+boolToStr(testCase.staticEnabled)+",dbEnabled="+boolToStr(testCase.dbEnabled), func(t *testing.T) {
			conf := config.Config{
				Persistence: config.Persistence{DataPath: t.TempDir()},
				Authentication: config.Authentication{
					DBUsers: config.DbUsers{Enabled: testCase.dbEnabled},
					APIKey:  config.StaticAPIKey{Enabled: testCase.staticEnabled, AllowedKeys: []string{"valid-key"}, Users: []string{"user1"}},
				},
			}
			wrapper, err := New(conf, logger, activeExister{})
			require.NoError(t, err)

			userId := "id"

			apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
			require.NoError(t, err)

			require.NoError(t, wrapper.Dynamic.CreateUser(userId, hash, identifier, "", "", time.Now()))

			principal, err := wrapper.ValidateAndExtract(apiKey, nil)
			if testCase.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "unauthorized: invalid api key")
			} else {
				require.NoError(t, err)
				require.NotNil(t, principal)
			}
		})
	}
}

// TestValidateAndExtract_NeutralNamespaceMessage pins that a key rejected for
// its namespace state renders the neutral copy at the 401 boundary, leaking
// neither the concept nor the namespace name, while a genuine bad key keeps its
// detail.
func TestValidateAndExtract_NeutralNamespaceMessage(t *testing.T) {
	t.Parallel()

	logger, _ := test.NewNullLogger()
	const nsName = "customer1"

	tests := []struct {
		name       string
		state      cmd.NamespaceState
		found      bool
		wrongKey   bool
		wantExact  string
		wantDetail bool
	}{
		{name: "suspended", state: cmd.NamespaceStateSuspended, found: true, wantExact: "unauthorized: instance suspended"},
		{name: "resuming", state: cmd.NamespaceStateResuming, found: true, wantExact: "unauthorized: instance resuming, retry shortly"},
		{name: "deleting", state: cmd.NamespaceStateDeleting, found: true, wantExact: "unauthorized: instance unavailable"},
		{name: "missing", state: cmd.NamespaceStateActive, found: false, wantExact: "unauthorized: instance unavailable"},
		{name: "bad key keeps detail", state: cmd.NamespaceStateSuspended, found: true, wrongKey: true, wantDetail: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf := config.Config{
				Persistence:    config.Persistence{DataPath: t.TempDir()},
				Authentication: config.Authentication{DBUsers: config.DbUsers{Enabled: true}},
			}
			wrapper, err := New(conf, logger, &countingExister{state: tc.state, found: tc.found})
			require.NoError(t, err)

			apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
			require.NoError(t, err)
			require.NoError(t, wrapper.Dynamic.CreateUser("u1", hash, identifier, "", nsName, time.Now()))

			token := apiKey
			if tc.wrongKey {
				// A valid, well-formed key for an unregistered identifier is
				// rejected during key validation, before the namespace guard.
				token, _, _, err = keys.CreateApiKeyAndHash()
				require.NoError(t, err)
			}

			_, err = wrapper.ValidateAndExtract(token, nil)
			require.Error(t, err)
			if tc.wantDetail {
				require.Contains(t, err.Error(), "unauthorized: invalid api key")
				require.NotContains(t, err.Error(), "instance")
				return
			}
			require.EqualError(t, err, tc.wantExact)
			require.NotContains(t, err.Error(), "namespace")
			require.NotContains(t, err.Error(), nsName)
		})
	}
}

func boolToStr(enabled bool) string {
	if enabled {
		return "true"
	}
	return "false"
}
