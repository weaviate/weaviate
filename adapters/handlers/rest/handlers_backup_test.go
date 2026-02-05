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

package rest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/namespace"
)

func TestCompressionBackupCfg(t *testing.T) {
	tcs := map[string]struct {
		cfg                 *models.BackupConfig
		expectedCompression ubak.CompressionLevel
		expectedCPU         int
		expectedBucket      string
		expectedPath        string
	}{
		"without config": {
			cfg:                 nil,
			expectedCompression: ubak.GzipDefaultCompression,
			expectedCPU:         ubak.DefaultCPUPercentage,
		},
		"with config": {
			cfg: &models.BackupConfig{
				CPUPercentage:    25,
				CompressionLevel: models.BackupConfigCompressionLevelBestSpeed,
			},
			expectedCompression: ubak.GzipBestSpeed,
			expectedCPU:         25,
		},
		"with partial config [CPU]": {
			cfg: &models.BackupConfig{
				CPUPercentage: 25,
			},
			expectedCompression: ubak.GzipDefaultCompression,
			expectedCPU:         25,
		},
		"with partial config [Compression]": {
			cfg: &models.BackupConfig{
				CompressionLevel: models.BackupConfigCompressionLevelBestSpeed,
			},
			expectedCompression: ubak.GzipBestSpeed,
			expectedCPU:         ubak.DefaultCPUPercentage,
		},
		"with partial config [Bucket]": {
			cfg: &models.BackupConfig{
				Bucket: "a bucket name",
			},
			expectedCompression: ubak.GzipDefaultCompression,
			expectedCPU:         ubak.DefaultCPUPercentage,
			expectedBucket:      "a bucket name",
		},
		"with partial config [Path]": {
			cfg: &models.BackupConfig{
				Path: "a path",
			},
			expectedCompression: ubak.GzipDefaultCompression,
			expectedCPU:         ubak.DefaultCPUPercentage,
			expectedPath:        "a path",
		},
	}

	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			ccfg := compressionFromBCfg(tc.cfg)
			assert.Equal(t, tc.expectedCompression, ccfg.Level)
			assert.Equal(t, tc.expectedCPU, ccfg.CPUPercentage)
		})
	}
}

func TestCompressionRestoreCfg(t *testing.T) {
	tcs := map[string]struct {
		cfg                 *models.RestoreConfig
		expectedCompression ubak.CompressionLevel
		expectedCPU         int
	}{
		"without config": {
			cfg:                 nil,
			expectedCompression: ubak.GzipDefaultCompression,
			expectedCPU:         ubak.DefaultCPUPercentage,
		},
		"with config": {
			cfg: &models.RestoreConfig{
				CPUPercentage: 25,
			},
			expectedCPU: 25,
		},
	}

	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			ccfg := compressionFromRCfg(tc.cfg)
			assert.Equal(t, tc.expectedCPU, ccfg.CPUPercentage)
		})
	}
}

func TestPrefixBackupClassNames(t *testing.T) {
	tests := []struct {
		name     string
		classes  []string
		ns       string
		expected []string
	}{
		{
			name:     "prefix single class",
			classes:  []string{"Articles"},
			ns:       "tenanta",
			expected: []string{"tenanta__Articles"},
		},
		{
			name:     "prefix multiple classes",
			classes:  []string{"Articles", "BlogPosts"},
			ns:       "tenanta",
			expected: []string{"tenanta__Articles", "tenanta__BlogPosts"},
		},
		{
			name:     "already prefixed class left unchanged",
			classes:  []string{"tenanta__Articles"},
			ns:       "tenanta",
			expected: []string{"tenanta__Articles"},
		},
		{
			name:     "default namespace returns unchanged",
			classes:  []string{"Articles"},
			ns:       namespace.DefaultNamespace,
			expected: []string{"Articles"},
		},
		{
			name:     "empty list returns empty",
			classes:  []string{},
			ns:       "tenanta",
			expected: []string{},
		},
		{
			name:     "nil list returns nil",
			classes:  nil,
			ns:       "tenanta",
			expected: nil,
		},
		{
			name:     "mixed prefixed and unprefixed",
			classes:  []string{"Articles", "tenanta__BlogPosts"},
			ns:       "tenanta",
			expected: []string{"tenanta__Articles", "tenanta__BlogPosts"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := prefixBackupClassNames(tt.classes, tt.ns)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStripNamespacePrefixFromClassList(t *testing.T) {
	tests := []struct {
		name     string
		classes  []string
		ns       string
		expected []string
	}{
		{
			name:     "strip single class",
			classes:  []string{"tenanta__Articles"},
			ns:       "tenanta",
			expected: []string{"Articles"},
		},
		{
			name:     "strip multiple classes",
			classes:  []string{"tenanta__Articles", "tenanta__BlogPosts"},
			ns:       "tenanta",
			expected: []string{"Articles", "BlogPosts"},
		},
		{
			name:     "class from different namespace left unchanged",
			classes:  []string{"tenantb__Articles"},
			ns:       "tenanta",
			expected: []string{"tenantb__Articles"},
		},
		{
			name:     "default namespace returns unchanged",
			classes:  []string{"Articles"},
			ns:       namespace.DefaultNamespace,
			expected: []string{"Articles"},
		},
		{
			name:     "empty list returns empty",
			classes:  []string{},
			ns:       "tenanta",
			expected: []string{},
		},
		{
			name:     "nil list returns nil",
			classes:  nil,
			ns:       "tenanta",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripNamespacePrefixFromClassList(tt.classes, tt.ns)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStripBackupResponseClasses(t *testing.T) {
	t.Run("strips prefix from create response", func(t *testing.T) {
		resp := &models.BackupCreateResponse{
			Classes: []string{"tenanta__Articles", "tenanta__BlogPosts"},
		}
		stripBackupResponseClasses(resp, "tenanta")
		assert.Equal(t, []string{"Articles", "BlogPosts"}, resp.Classes)
	})

	t.Run("nil response does not panic", func(t *testing.T) {
		stripBackupResponseClasses(nil, "tenanta")
	})

	t.Run("default namespace leaves unchanged", func(t *testing.T) {
		resp := &models.BackupCreateResponse{
			Classes: []string{"Articles"},
		}
		stripBackupResponseClasses(resp, namespace.DefaultNamespace)
		assert.Equal(t, []string{"Articles"}, resp.Classes)
	})
}

func TestStripBackupRestoreResponseClasses(t *testing.T) {
	t.Run("strips prefix from restore response", func(t *testing.T) {
		resp := &models.BackupRestoreResponse{
			Classes: []string{"tenanta__Articles", "tenanta__BlogPosts"},
		}
		stripBackupRestoreResponseClasses(resp, "tenanta")
		assert.Equal(t, []string{"Articles", "BlogPosts"}, resp.Classes)
	})

	t.Run("nil response does not panic", func(t *testing.T) {
		stripBackupRestoreResponseClasses(nil, "tenanta")
	})
}
