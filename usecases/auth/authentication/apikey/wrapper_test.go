//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package apikey

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
			wrapper, err := New(conf, logger)
			require.NoError(t, err)

			_, err = wrapper.ValidateAndExtract(context.Background(), "invalid-key", nil)
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
			wrapper, err := New(conf, logger)
			require.NoError(t, err)

			principal, err := wrapper.ValidateAndExtract(context.Background(), "valid-key", nil)
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
			wrapper, err := New(conf, logger)
			require.NoError(t, err)

			userId := "id"

			apiKey, hash, identifier, err := keys.CreateApiKeyAndHash()
			require.NoError(t, err)

			require.NoError(t, wrapper.Dynamic.CreateUser(userId, hash, identifier, "", time.Now()))

			principal, err := wrapper.ValidateAndExtract(context.Background(), apiKey, nil)
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

func boolToStr(enabled bool) string {
	if enabled {
		return "true"
	}
	return "false"
}
