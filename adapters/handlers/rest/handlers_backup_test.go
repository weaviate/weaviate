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

package rest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	ubak "github.com/weaviate/weaviate/usecases/backup"
)

func TestCompressionCfg(t *testing.T) {
	l := "BestSpeed"
	tcs := map[string]struct {
		cfg                 *models.BackupConfig
		expectedCompression ubak.CompressionLevel
		expectedCPU         int
		expectedChunkSize   int
	}{
		"without config": {
			cfg:                 nil,
			expectedCompression: ubak.DefaultCompression,
			expectedCPU:         ubak.DefaultCPUPercentage,
			expectedChunkSize:   ubak.DefaultChunkSize,
		},
		"with config": {
			cfg: &models.BackupConfig{
				CPUPercentage:    25,
				ChunkSize:        512,
				CompressionLevel: &l,
			},
			expectedCompression: ubak.BestSpeed,
			expectedCPU:         25,
			expectedChunkSize:   512,
		},
	}

	for n, tc := range tcs {
		t.Run(n, func(t *testing.T) {
			ccfg := compressionFromCfg(tc.cfg)
			assert.Equal(t, tc.expectedCompression, ccfg.Level)
			assert.Equal(t, tc.expectedCPU, ccfg.CPUPercentage)
			assert.Equal(t, tc.expectedChunkSize, ccfg.ChunkSize)
		})
	}
}
