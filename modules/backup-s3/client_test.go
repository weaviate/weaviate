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
	"testing"

	"github.com/stretchr/testify/assert"
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
