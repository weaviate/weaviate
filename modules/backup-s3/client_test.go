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

func TestHomeDir(t *testing.T) {
	tests := []struct {
		name           string
		bucket         string
		backupPath     string
		backupID       string
		overrideBucket string
		overridePath   string
		expected       string
	}{
		{
			name:     "standard s3 bucket",
			bucket:   "my-bucket",
			backupID: "backup-1",
			expected: "s3://my-bucket/backup-1",
		},
		{
			name:           "with override bucket",
			bucket:         "my-bucket",
			backupID:       "backup-1",
			overrideBucket: "other-bucket",
			expected:       "s3://other-bucket/backup-1",
		},
		{
			name:         "with override path",
			bucket:       "my-bucket",
			backupID:     "backup-1",
			overridePath: "custom/path",
			expected:     "s3://my-bucket/custom/path/backup-1",
		},
		{
			name:     "bucket with https scheme",
			bucket:   "https://nyc3.digitaloceanspaces.com/my-bucket",
			backupID: "backup-1",
			expected: "https://nyc3.digitaloceanspaces.com/my-bucket/backup-1",
		},
		{
			name:           "override bucket with https scheme",
			bucket:         "my-bucket",
			backupID:       "backup-1",
			overrideBucket: "https://nyc3.digitaloceanspaces.com/other-bucket",
			expected:       "https://nyc3.digitaloceanspaces.com/other-bucket/backup-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &s3Client{
				config: &clientConfig{
					Bucket:     tt.bucket,
					BackupPath: tt.backupPath,
				},
			}
			result := client.HomeDir(tt.backupID, tt.overrideBucket, tt.overridePath)
			assert.Equal(t, tt.expected, result)
		})
	}
}
