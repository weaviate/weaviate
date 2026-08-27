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

package db

import (
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	entschema "github.com/weaviate/weaviate/entities/schema"
)

func TestSplitObjectsBucketSize(t *testing.T) {
	className := "SplitObjects"
	shardName := "shard1"

	tests := []struct {
		name                   string
		objectsBucketSize      uint64
		uncompressedVectorSize uint64
		wantWithoutVectors     uint64
		wantVectors            uint64
		wantWarning            bool
	}{
		{
			name:                   "vectors below bucket size",
			objectsBucketSize:      1000,
			uncompressedVectorSize: 400,
			wantWithoutVectors:     600,
			wantVectors:            400,
		},
		{
			name:                   "no vectors modelled",
			objectsBucketSize:      1000,
			uncompressedVectorSize: 0,
			wantWithoutVectors:     1000,
			wantVectors:            0,
		},
		{
			name:                   "vectors exactly bucket size",
			objectsBucketSize:      1000,
			uncompressedVectorSize: 1000,
			wantWithoutVectors:     0,
			wantVectors:            1000,
		},
		{
			name:                   "vectors one byte above bucket size",
			objectsBucketSize:      1000,
			uncompressedVectorSize: 1001,
			wantWithoutVectors:     0,
			wantVectors:            1000,
			wantWarning:            true,
		},
		{
			name:                   "vectors far above bucket size",
			objectsBucketSize:      1000,
			uncompressedVectorSize: 1 << 40,
			wantWithoutVectors:     0,
			wantVectors:            1000,
			wantWarning:            true,
		},
		{
			name:                   "empty bucket, no vectors modelled",
			objectsBucketSize:      0,
			uncompressedVectorSize: 0,
			wantWithoutVectors:     0,
			wantVectors:            0,
		},
		{
			name:                   "vectors modelled for an empty bucket",
			objectsBucketSize:      0,
			uncompressedVectorSize: 512,
			wantWithoutVectors:     0,
			wantVectors:            0,
			wantWarning:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			idx := &Index{
				logger: logger,
				Config: IndexConfig{ClassName: entschema.ClassName(className)},
			}

			withoutVectors, vectors := idx.splitObjectsBucketSize(shardName,
				tt.objectsBucketSize, tt.uncompressedVectorSize)

			assert.Equal(t, tt.wantWithoutVectors, withoutVectors)
			assert.Equal(t, tt.wantVectors, vectors)

			// the bound is what catches a wrapped subtraction; the sum alone wraps back to the
			// bucket size and would pass either way
			assert.LessOrEqual(t, withoutVectors, tt.objectsBucketSize)
			assert.LessOrEqual(t, vectors, tt.objectsBucketSize)
			assert.Equal(t, tt.objectsBucketSize, withoutVectors+vectors)

			if !tt.wantWarning {
				assert.Empty(t, hook.AllEntries())
				return
			}
			require.Len(t, hook.AllEntries(), 1)
			entry := hook.LastEntry()
			assert.Equal(t, logrus.WarnLevel, entry.Level)
			assert.Equal(t, className, entry.Data["class"])
			assert.Equal(t, shardName, entry.Data["shard"])
		})
	}
}
