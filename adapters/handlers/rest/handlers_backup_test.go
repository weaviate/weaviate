//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	ubak "github.com/weaviate/weaviate/usecases/backup"
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
			expectedCompression: ubak.DefaultCompression,
			expectedCPU:         ubak.DefaultCPUPercentage,
		},
		"with config": {
			cfg: &models.BackupConfig{
				CPUPercentage:    25,
				ChunkSize:        512,
				CompressionLevel: models.BackupConfigCompressionLevelBestSpeed,
			},
			expectedCompression: ubak.BestSpeed,
			expectedCPU:         25,
		},
		"with partial config [CPU]": {
			cfg: &models.BackupConfig{
				CPUPercentage: 25,
			},
			expectedCompression: ubak.DefaultCompression,
			expectedCPU:         25,
		},
		"with partial config [Compression]": {
			cfg: &models.BackupConfig{
				CompressionLevel: models.BackupConfigCompressionLevelBestSpeed,
			},
			expectedCompression: ubak.BestSpeed,
			expectedCPU:         ubak.DefaultCPUPercentage,
		},
		"with partial config [Bucket]": {
			cfg: &models.BackupConfig{
				Bucket: "a bucket name",
			},
			expectedCompression: ubak.DefaultCompression,
			expectedCPU:         ubak.DefaultCPUPercentage,
			expectedBucket:      "a bucket name",
		},
		"with partial config [Path]": {
			cfg: &models.BackupConfig{
				Path: "a path",
			},
			expectedCompression: ubak.DefaultCompression,
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
			expectedCompression: ubak.DefaultCompression,
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
