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

package modstgs3

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setEnvVars sets environment variables and returns a cleanup function
// that restores the original values.
func setEnvVars(t *testing.T, vars map[string]string) {
	t.Helper()
	originals := make(map[string]string)
	wasSet := make(map[string]bool)
	for k, v := range vars {
		if orig, ok := os.LookupEnv(k); ok {
			originals[k] = orig
			wasSet[k] = true
		}
		os.Setenv(k, v)
	}
	t.Cleanup(func() {
		for k := range vars {
			if wasSet[k] {
				os.Setenv(k, originals[k])
			} else {
				os.Unsetenv(k)
			}
		}
	})
}

// clearEnvVars unsets environment variables and returns a cleanup function
// that restores the original values.
func clearEnvVars(t *testing.T, keys []string) {
	t.Helper()
	originals := make(map[string]string)
	wasSet := make(map[string]bool)
	for _, k := range keys {
		if orig, ok := os.LookupEnv(k); ok {
			originals[k] = orig
			wasSet[k] = true
		}
		os.Unsetenv(k)
	}
	t.Cleanup(func() {
		for _, k := range keys {
			if wasSet[k] {
				os.Setenv(k, originals[k])
			}
		}
	})
}

func TestResolveCredentials_EnvVarsPreferred(t *testing.T) {
	// When AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set,
	// resolveCredentials should return env-based credentials
	// without attempting IAM metadata lookup.
	setEnvVars(t, map[string]string{
		"AWS_ACCESS_KEY_ID":     "test-key",
		"AWS_SECRET_ACCESS_KEY": "test-secret",
	})

	config := &clientConfig{}
	creds, err := resolveCredentials(config, "us-east-1")
	require.NoError(t, err)
	require.NotNil(t, creds)

	val, err := creds.GetWithContext(nil)
	require.NoError(t, err)
	assert.Equal(t, "test-key", val.AccessKeyID)
	assert.Equal(t, "test-secret", val.SecretAccessKey)
}

func TestResolveCredentials_LegacyEnvVarsPreferred(t *testing.T) {
	// Legacy AWS_ACCESS_KEY / AWS_SECRET_KEY should also be recognized.
	clearEnvVars(t, []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"})
	setEnvVars(t, map[string]string{
		"AWS_ACCESS_KEY": "legacy-key",
		"AWS_SECRET_KEY": "legacy-secret",
	})

	config := &clientConfig{}
	creds, err := resolveCredentials(config, "us-east-1")
	require.NoError(t, err)
	require.NotNil(t, creds)

	val, err := creds.GetWithContext(nil)
	require.NoError(t, err)
	assert.Equal(t, "legacy-key", val.AccessKeyID)
	assert.Equal(t, "legacy-secret", val.SecretAccessKey)
}

func TestResolveCredentials_RoleARNTriggersAssumeRole(t *testing.T) {
	// When RoleARN is set, resolveCredentials should attempt STS AssumeRole.
	// Without valid base credentials or a reachable STS endpoint, this will
	// fail — which is the expected behavior we're testing.
	clearEnvVars(t, []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
		"AWS_ACCESS_KEY", "AWS_SECRET_KEY",
	})

	config := &clientConfig{
		RoleARN:    "arn:aws:iam::123456789012:role/TestRole",
		ExternalID: "ext-123",
	}

	_, err := resolveCredentials(config, "us-east-1")
	// Should fail because no base credentials are available for the STS call
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IAM credentials for STS AssumeRole")
}

func TestResolveCredentials_RoleARNWithEnvCreds(t *testing.T) {
	// When RoleARN is set and env credentials are available, the function
	// should create STS AssumeRole credentials (construction succeeds,
	// actual STS call is deferred to first use).
	setEnvVars(t, map[string]string{
		"AWS_ACCESS_KEY_ID":     "test-key",
		"AWS_SECRET_ACCESS_KEY": "test-secret",
	})

	config := &clientConfig{
		RoleARN:    "arn:aws:iam::123456789012:role/TestRole",
		ExternalID: "ext-123",
	}

	creds, err := resolveCredentials(config, "us-east-1")
	require.NoError(t, err)
	require.NotNil(t, creds)
}

func TestNewSTSAssumeRoleCredentials_EndpointDefaults(t *testing.T) {
	tests := []struct {
		name             string
		stsEndpoint      string
		region           string
		expectedEndpoint string
	}{
		{
			name:             "explicit endpoint",
			stsEndpoint:      "https://custom-sts.example.com",
			region:           "us-east-1",
			expectedEndpoint: "https://custom-sts.example.com",
		},
		{
			name:             "regional default",
			stsEndpoint:      "",
			region:           "eu-west-1",
			expectedEndpoint: "https://sts.eu-west-1.amazonaws.com",
		},
		{
			name:             "global fallback",
			stsEndpoint:      "",
			region:           "",
			expectedEndpoint: "https://sts.amazonaws.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnvVars(t, map[string]string{
				"AWS_ACCESS_KEY_ID":     "test-key",
				"AWS_SECRET_ACCESS_KEY": "test-secret",
			})

			config := &clientConfig{
				RoleARN:     "arn:aws:iam::123456789012:role/TestRole",
				STSEndpoint: tt.stsEndpoint,
			}

			creds, err := newSTSAssumeRoleCredentials(config, tt.region)
			require.NoError(t, err)
			require.NotNil(t, creds)
			// We can't inspect the endpoint directly, but we verify
			// construction succeeds with each combination.
		})
	}
}

func TestNewSTSAssumeRoleCredentials_SessionNameDefault(t *testing.T) {
	setEnvVars(t, map[string]string{
		"AWS_ACCESS_KEY_ID":     "test-key",
		"AWS_SECRET_ACCESS_KEY": "test-secret",
	})

	// Without explicit session name, should default to "weaviate-backup-s3"
	config := &clientConfig{
		RoleARN: "arn:aws:iam::123456789012:role/TestRole",
	}

	creds, err := newSTSAssumeRoleCredentials(config, "us-east-1")
	require.NoError(t, err)
	require.NotNil(t, creds)
}

func TestNewSTSAssumeRoleCredentials_CustomSessionName(t *testing.T) {
	setEnvVars(t, map[string]string{
		"AWS_ACCESS_KEY_ID":     "test-key",
		"AWS_SECRET_ACCESS_KEY": "test-secret",
	})

	config := &clientConfig{
		RoleARN:         "arn:aws:iam::123456789012:role/TestRole",
		RoleSessionName: "my-custom-session",
	}

	creds, err := newSTSAssumeRoleCredentials(config, "us-east-1")
	require.NoError(t, err)
	require.NotNil(t, creds)
}

func TestNewSTSAssumeRoleCredentials_NoBaseCreds(t *testing.T) {
	// Without any base credentials, STS AssumeRole should fail fast.
	clearEnvVars(t, []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
		"AWS_ACCESS_KEY", "AWS_SECRET_KEY",
	})

	config := &clientConfig{
		RoleARN:    "arn:aws:iam::123456789012:role/TestRole",
		ExternalID: "ext-123",
	}

	_, err := newSTSAssumeRoleCredentials(config, "us-east-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IAM credentials for STS AssumeRole")
}
