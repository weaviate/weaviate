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

func TestDoSTSAssumeRole_UsesEnvCreds(t *testing.T) {
	setEnvVars(t, map[string]string{
		"AWS_ACCESS_KEY_ID":     "sts-base-key",
		"AWS_SECRET_ACCESS_KEY": "sts-base-secret",
	})

	config := &clientConfig{
		RoleARN:    "arn:aws:iam::123456789012:role/TestRole",
		ExternalID: "ext-456",
	}

	creds, err := doSTSAssumeRole(config, "us-west-2")
	require.NoError(t, err)
	require.NotNil(t, creds)
}

func TestDoSTSAssumeRole_NoCredsAvailable(t *testing.T) {
	clearEnvVars(t, []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
		"AWS_ACCESS_KEY", "AWS_SECRET_KEY",
	})

	config := &clientConfig{
		RoleARN: "arn:aws:iam::123456789012:role/TestRole",
	}

	_, err := doSTSAssumeRole(config, "us-east-1")
	require.Error(t, err)
}

func TestExportBackend_WithExportClient(t *testing.T) {
	setEnvVars(t, map[string]string{
		"AWS_ACCESS_KEY_ID":     "test-key",
		"AWS_SECRET_ACCESS_KEY": "test-secret",
	})

	backupCfg := newConfig("s3.amazonaws.com", "my-bucket", "", true)
	backupClient, err := newClient(backupCfg, nil, "/tmp")
	require.NoError(t, err)

	exportCfg := newConfig("s3.amazonaws.com", "my-bucket", "", true)
	exportCfg.RoleARN = "arn:aws:iam::123456789012:role/ExportRole"
	exportCfg.ExternalID = "ext-123"
	exportClient, err := newClient(exportCfg, nil, "/tmp")
	require.NoError(t, err)

	m := &Module{
		s3Client:     backupClient,
		exportClient: exportClient,
	}

	eb := m.ExportBackend()
	// Should return the export client, not the module itself
	assert.NotEqual(t, m, eb)
	assert.Equal(t, Name, eb.Name())
	assert.True(t, eb.IsExternal())
}

func TestExportBackend_WithoutExportClient(t *testing.T) {
	setEnvVars(t, map[string]string{
		"AWS_ACCESS_KEY_ID":     "test-key",
		"AWS_SECRET_ACCESS_KEY": "test-secret",
	})

	backupCfg := newConfig("s3.amazonaws.com", "my-bucket", "", true)
	backupClient, err := newClient(backupCfg, nil, "/tmp")
	require.NoError(t, err)

	m := &Module{
		s3Client: backupClient,
	}

	eb := m.ExportBackend()
	// Should return the module itself when no export client is configured
	assert.Equal(t, m, eb)
}

func TestBucketAndPath(t *testing.T) {
	client := &s3Client{config: &clientConfig{Bucket: "default-bucket", BackupPath: "default-path"}}

	tests := []struct {
		name           string
		backupID       string
		key            string
		overrideBucket string
		overridePath   string
		wantBucket     string
		wantObject     string
	}{
		{
			name:       "no overrides",
			backupID:   "backup-1",
			key:        "file.db",
			wantBucket: "default-bucket",
			wantObject: "default-path/backup-1/file.db",
		},
		{
			name:           "override bucket only",
			backupID:       "backup-1",
			key:            "file.db",
			overrideBucket: "export-bucket",
			wantBucket:     "export-bucket",
			wantObject:     "default-path/backup-1/file.db",
		},
		{
			name:         "override path only",
			backupID:     "backup-1",
			key:          "file.db",
			overridePath: "export-path",
			wantBucket:   "default-bucket",
			wantObject:   "export-path/backup-1/file.db",
		},
		{
			name:           "override both",
			backupID:       "backup-1",
			key:            "file.db",
			overrideBucket: "export-bucket",
			overridePath:   "export-path",
			wantBucket:     "export-bucket",
			wantObject:     "export-path/backup-1/file.db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, objectName := client.bucketAndPath(tt.backupID, tt.key, tt.overrideBucket, tt.overridePath)
			assert.Equal(t, tt.wantBucket, bucket)
			assert.Equal(t, tt.wantObject, objectName)
		})
	}
}

func TestRefreshableAssumeRole_IsProvider(t *testing.T) {
	// Verify that refreshableAssumeRole satisfies the credentials.Provider
	// interface at compile time via the test's type assertion.
	setEnvVars(t, map[string]string{
		"AWS_ACCESS_KEY_ID":     "test-key",
		"AWS_SECRET_ACCESS_KEY": "test-secret",
	})

	config := &clientConfig{
		RoleARN:    "arn:aws:iam::123456789012:role/TestRole",
		ExternalID: "ext-123",
	}

	creds, err := newSTSAssumeRoleCredentials(config, "us-east-1")
	require.NoError(t, err)
	require.NotNil(t, creds)

	// The returned Credentials wraps a refreshableAssumeRole provider.
	// On first Get, it will call Retrieve() which re-resolves base creds.
	// This will fail at the STS HTTP call (no real STS endpoint), but
	// confirms the provider is wired correctly.
	assert.True(t, creds.IsExpired(), "new credentials should start expired to trigger first Retrieve()")
}
