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
