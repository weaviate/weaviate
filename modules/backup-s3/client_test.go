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
	"context"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitialize_SkipAccessCheck(t *testing.T) {
	// Validation runs before the SkipAccessCheck short-circuit: a valid bucket
	// skips the probe, an empty one still errors.
	tests := []struct {
		name    string
		bucket  string
		wantErr string
	}{
		{name: "valid bucket skips probe", bucket: "my-bucket"},
		{name: "empty bucket still validates", bucket: "", wantErr: "bucket must not be empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &s3Client{config: &clientConfig{Bucket: tt.bucket, SkipAccessCheck: true}}
			err := c.Initialize(context.Background(), "backup-1", "", "")
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

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

func TestExportBackend_AlwaysReturnsExportClient(t *testing.T) {
	setEnvVars(t, map[string]string{
		"AWS_ACCESS_KEY_ID":     "test-key",
		"AWS_SECRET_ACCESS_KEY": "test-secret",
	})

	backupCfg := newConfig("s3.amazonaws.com", "my-bucket", "backup-path", true)
	backupClient, err := newClient(backupCfg, nil, "/tmp")
	require.NoError(t, err)

	exportCfg := newConfig("s3.amazonaws.com", "my-bucket", "", true)
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

	// Export backend must NOT inherit the backup path
	exportBackend := eb.(*exportS3Backend)
	assert.Empty(t, exportBackend.config.BackupPath, "export backend should have empty BackupPath")
}

func TestBucketAndPath(t *testing.T) {
	tests := []struct {
		name           string
		configBucket   string
		configPath     string
		overrideBucket string
		overridePath   string
		wantBucket     string
		wantObject     string
		wantErr        string
	}{
		{
			name:         "no overrides",
			configBucket: "default-bucket",
			configPath:   "default-path",
			wantBucket:   "default-bucket",
			wantObject:   "default-path/backup-1/file.db",
		},
		{
			name:           "override bucket only",
			configBucket:   "default-bucket",
			configPath:     "default-path",
			overrideBucket: "export-bucket",
			wantBucket:     "export-bucket",
			wantObject:     "default-path/backup-1/file.db",
		},
		{
			name:         "override path only",
			configBucket: "default-bucket",
			configPath:   "default-path",
			overridePath: "export-path",
			wantBucket:   "default-bucket",
			wantObject:   "export-path/backup-1/file.db",
		},
		{
			name:           "override both",
			configBucket:   "default-bucket",
			configPath:     "default-path",
			overrideBucket: "export-bucket",
			overridePath:   "export-path",
			wantBucket:     "export-bucket",
			wantObject:     "export-path/backup-1/file.db",
		},
		{
			name:         "empty config bucket without override returns error",
			configBucket: "",
			configPath:   "path",
			wantErr:      "bucket must not be empty",
		},
		{
			name:           "empty config bucket with override succeeds",
			configBucket:   "",
			configPath:     "path",
			overrideBucket: "override-bucket",
			wantBucket:     "override-bucket",
			wantObject:     "path/backup-1/file.db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &s3Client{config: &clientConfig{Bucket: tt.configBucket, BackupPath: tt.configPath}}
			bucket, objectName, err := client.bucketAndPath("backup-1", "file.db", tt.overrideBucket, tt.overridePath)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantBucket, bucket)
				assert.Equal(t, tt.wantObject, objectName)
			}
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

func TestMakeObjectName(t *testing.T) {
	tests := []struct {
		name         string
		backupPath   string
		overridePath string
		parts        []string
		expected     string
	}{
		{
			name:       "no override uses BackupPath",
			backupPath: "base/path",
			parts:      []string{"backup-id", "file.json"},
			expected:   "base/path/backup-id/file.json",
		},
		{
			name:         "override replaces BackupPath entirely",
			backupPath:   "base/path",
			overridePath: "override/path",
			parts:        []string{"backup-id", "file.json"},
			expected:     "override/path/backup-id/file.json",
		},
		{
			name:     "empty BackupPath no override",
			parts:    []string{"backup-id", "key"},
			expected: "backup-id/key",
		},
		{
			name:         "override with empty BackupPath",
			overridePath: "tenant-id",
			parts:        []string{"export-1"},
			expected:     "tenant-id/export-1",
		},
		{
			name:         "same override and BackupPath do not double",
			backupPath:   "tenant-id",
			overridePath: "tenant-id",
			parts:        []string{"export-1", "meta.json"},
			expected:     "tenant-id/export-1/meta.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &s3Client{config: &clientConfig{BackupPath: tt.backupPath}}
			got := c.makeObjectName(tt.overridePath, tt.parts...)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestHomeDir(t *testing.T) {
	tests := []struct {
		name           string
		backupPath     string
		bucket         string
		backupID       string
		overrideBucket string
		overridePath   string
		expected       string
	}{
		{
			name:     "default bucket, empty BackupPath",
			bucket:   "my-bucket",
			backupID: "backup-1",
			expected: "s3://my-bucket/backup-1",
		},
		{
			name:       "non-empty BackupPath, no override",
			bucket:     "my-bucket",
			backupPath: "tenant-id",
			backupID:   "backup-1",
			expected:   "s3://my-bucket/tenant-id/backup-1",
		},
		{
			name:           "override path only",
			bucket:         "my-bucket",
			backupID:       "backup-1",
			overrideBucket: "my-bucket",
			overridePath:   "override-path",
			expected:       "s3://my-bucket/override-path/backup-1",
		},
		{
			name:           "override both bucket and path",
			bucket:         "my-bucket",
			backupPath:     "default-path",
			backupID:       "backup-1",
			overrideBucket: "other-bucket",
			overridePath:   "tenant-path",
			expected:       "s3://other-bucket/tenant-path/backup-1",
		},
		{
			// Regression: when BackupPath == overridePath the path must not appear twice.
			// This was the original bug reported against WCS exports.
			name:           "override path equals BackupPath does not duplicate segment",
			bucket:         "my-bucket",
			backupPath:     "tenant-id",
			backupID:       "export-1",
			overrideBucket: "my-bucket",
			overridePath:   "tenant-id",
			expected:       "s3://my-bucket/tenant-id/export-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &s3Client{config: &clientConfig{
				Bucket:     tt.bucket,
				BackupPath: tt.backupPath,
			}}
			got := c.HomeDir(tt.backupID, tt.overrideBucket, tt.overridePath)
			assert.Equal(t, tt.expected, got)
		})
	}
}

const (
	listPage1 = `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix>backups/</Prefix><Delimiter>/</Delimiter><CommonPrefixes><Prefix>backups/b0/</Prefix></CommonPrefixes><IsTruncated>true</IsTruncated><NextContinuationToken>t1</NextContinuationToken></ListBucketResult>`
	listPage2 = `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><Prefix>backups/</Prefix><Delimiter>/</Delimiter><CommonPrefixes><Prefix>backups/b1/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>backups/b2/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>backups/b3/</Prefix></CommonPrefixes><IsTruncated>false</IsTruncated></ListBucketResult>`
)

// cancelOnSecondPageTransport fakes S3 ListObjectsV2: serving page 2 cancels
// the listing context, so minio yields the page-2 items with ctx already
// cancelled — the interleaving that leaks without the channel drain.
type cancelOnSecondPageTransport struct {
	mu     sync.Mutex
	page   int
	cancel context.CancelFunc
}

func (f *cancelOnSecondPageTransport) reset(cancel context.CancelFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.page = 0
	f.cancel = cancel
}

func (f *cancelOnSecondPageTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.page++
	body := listPage1
	if f.page > 1 {
		body = listPage2
		f.cancel()
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/xml"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    r,
	}, nil
}

// AllBackups must drain the ListObjects channel on early return, otherwise
// minio's producer goroutine blocks forever on an unguarded error-send.
// The leaking interleaving is racy (~25% per run), hence the repetition.
func TestAllBackupsNoGoroutineLeakOnCancel(t *testing.T) {
	transport := &cancelOnSecondPageTransport{}
	minioClient, err := minio.New("s3.example.invalid", &minio.Options{
		Creds:     credentials.NewStaticV4("key", "secret", ""),
		Region:    "us-east-1",
		Transport: transport,
	})
	require.NoError(t, err)

	s3c := &s3Client{
		client: minioClient,
		config: &clientConfig{Bucket: "bucket", BackupPath: "backups"},
		logger: logrus.New(),
	}

	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		transport.reset(cancel)
		_, err := s3c.AllBackups(ctx)
		require.Error(t, err)
		cancel()
	}

	require.Eventually(t, func() bool {
		return !strings.Contains(fullStackDump(), "minio-go")
	}, 3*time.Second, 50*time.Millisecond, "minio ListObjects producer goroutine leaked")
}

// fullStackDump returns the full goroutine dump. runtime.Stack truncates
// silently when the buffer is too small (n == len(buf)), so grow until it fits.
func fullStackDump() string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}
