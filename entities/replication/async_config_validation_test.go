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

package replication

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestValidateAsyncConfig(t *testing.T) {
	i64 := func(v int64) *int64 { return &v }

	tests := []struct {
		name    string
		cfg     *models.ReplicationAsyncConfig
		wantErr string
	}{
		{name: "nil config", cfg: nil},
		{name: "empty config", cfg: &models.ReplicationAsyncConfig{}},
		{name: "all fields valid", cfg: &models.ReplicationAsyncConfig{
			HashtreeHeight:            i64(16),
			Frequency:                 i64(30_000),
			FrequencyWhilePropagating: i64(10_000),
			LoggingFrequency:          i64(5),
			DiffBatchSize:             i64(1_000),
			DiffPerNodeTimeout:        i64(10),
			PrePropagationTimeout:     i64(60),
			PropagationTimeout:        i64(30),
			PropagationLimit:          i64(10_000),
			PropagationConcurrency:    i64(5),
			PropagationBatchSize:      i64(100),
			PropagationDelay:          i64(30_000),
		}},
		{name: "below-min frequency accepted (clamped later)", cfg: &models.ReplicationAsyncConfig{Frequency: i64(100)}},
		{name: "below-min frequencyWhilePropagating accepted (clamped later)", cfg: &models.ReplicationAsyncConfig{FrequencyWhilePropagating: i64(500)}},
		{name: "zero frequency accepted", cfg: &models.ReplicationAsyncConfig{Frequency: i64(0)}},
		{name: "zero propagationDelay accepted", cfg: &models.ReplicationAsyncConfig{PropagationDelay: i64(0)}},

		{name: "hashtreeHeight negative", cfg: &models.ReplicationAsyncConfig{HashtreeHeight: i64(-1)}, wantErr: "hashtreeHeight: value -1 out of range: min 0, max 20"},
		{name: "hashtreeHeight above max", cfg: &models.ReplicationAsyncConfig{HashtreeHeight: i64(21)}, wantErr: "hashtreeHeight: value 21 out of range: min 0, max 20"},
		{name: "frequency negative", cfg: &models.ReplicationAsyncConfig{Frequency: i64(-1)}, wantErr: "frequency must be >= 0"},
		{name: "frequency overflow", cfg: &models.ReplicationAsyncConfig{Frequency: i64(MaxDurationMillis + 1)}, wantErr: "frequency too large"},
		{name: "frequencyWhilePropagating negative", cfg: &models.ReplicationAsyncConfig{FrequencyWhilePropagating: i64(-1)}, wantErr: "frequencyWhilePropagating must be >= 0"},
		{name: "frequencyWhilePropagating overflow", cfg: &models.ReplicationAsyncConfig{FrequencyWhilePropagating: i64(MaxDurationMillis + 1)}, wantErr: "frequencyWhilePropagating too large"},
		{name: "loggingFrequency zero", cfg: &models.ReplicationAsyncConfig{LoggingFrequency: i64(0)}, wantErr: "loggingFrequency must be > 0"},
		{name: "loggingFrequency negative", cfg: &models.ReplicationAsyncConfig{LoggingFrequency: i64(-5)}, wantErr: "loggingFrequency must be > 0"},
		{name: "loggingFrequency duration overflow", cfg: &models.ReplicationAsyncConfig{LoggingFrequency: i64(math.MaxInt64/int64(time.Second) + 1)}, wantErr: "loggingFrequency must be > 0"},
		{name: "diffBatchSize zero", cfg: &models.ReplicationAsyncConfig{DiffBatchSize: i64(0)}, wantErr: "diffBatchSize: value 0 out of range: min 1, max 10000"},
		{name: "diffBatchSize above max", cfg: &models.ReplicationAsyncConfig{DiffBatchSize: i64(10_001)}, wantErr: "diffBatchSize: value 10001 out of range: min 1, max 10000"},
		{name: "diffPerNodeTimeout zero", cfg: &models.ReplicationAsyncConfig{DiffPerNodeTimeout: i64(0)}, wantErr: "diffPerNodeTimeout must be > 0"},
		{name: "diffPerNodeTimeout negative", cfg: &models.ReplicationAsyncConfig{DiffPerNodeTimeout: i64(-1)}, wantErr: "diffPerNodeTimeout must be > 0"},
		{name: "prePropagationTimeout zero", cfg: &models.ReplicationAsyncConfig{PrePropagationTimeout: i64(0)}, wantErr: "prePropagationTimeout must be > 0"},
		{name: "propagationTimeout zero", cfg: &models.ReplicationAsyncConfig{PropagationTimeout: i64(0)}, wantErr: "propagationTimeout must be > 0"},
		{name: "propagationLimit zero", cfg: &models.ReplicationAsyncConfig{PropagationLimit: i64(0)}, wantErr: "propagationLimit: value 0 out of range: min 1, max 100000"},
		{name: "propagationLimit above max", cfg: &models.ReplicationAsyncConfig{PropagationLimit: i64(100_001)}, wantErr: "propagationLimit: value 100001 out of range: min 1, max 100000"},
		{name: "propagationConcurrency zero", cfg: &models.ReplicationAsyncConfig{PropagationConcurrency: i64(0)}, wantErr: "propagationConcurrency: value 0 out of range: min 1, max 20"},
		{name: "propagationConcurrency above max", cfg: &models.ReplicationAsyncConfig{PropagationConcurrency: i64(21)}, wantErr: "propagationConcurrency: value 21 out of range: min 1, max 20"},
		{name: "propagationBatchSize zero", cfg: &models.ReplicationAsyncConfig{PropagationBatchSize: i64(0)}, wantErr: "propagationBatchSize: value 0 out of range: min 1, max 1000"},
		{name: "propagationBatchSize above max", cfg: &models.ReplicationAsyncConfig{PropagationBatchSize: i64(1_001)}, wantErr: "propagationBatchSize: value 1001 out of range: min 1, max 1000"},
		{name: "propagationDelay negative", cfg: &models.ReplicationAsyncConfig{PropagationDelay: i64(-1)}, wantErr: "propagationDelay must be >= 0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAsyncConfig(tt.cfg)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestSanitizeAsyncConfig(t *testing.T) {
	i64 := func(v int64) *int64 { return &v }

	tests := []struct {
		name        string
		cfg         *models.ReplicationAsyncConfig
		want        *models.ReplicationAsyncConfig
		wantDropped []string
	}{
		{name: "nil config"},
		{name: "empty config", cfg: &models.ReplicationAsyncConfig{}, want: &models.ReplicationAsyncConfig{}},
		{
			name: "all valid untouched",
			cfg:  &models.ReplicationAsyncConfig{HashtreeHeight: i64(12), Frequency: i64(30_000), PropagationConcurrency: i64(5)},
			want: &models.ReplicationAsyncConfig{HashtreeHeight: i64(12), Frequency: i64(30_000), PropagationConcurrency: i64(5)},
		},
		{
			name:        "invalid frequency dropped, valid height kept",
			cfg:         &models.ReplicationAsyncConfig{HashtreeHeight: i64(12), Frequency: i64(-1)},
			want:        &models.ReplicationAsyncConfig{HashtreeHeight: i64(12)},
			wantDropped: []string{"frequency must be >= 0"},
		},
		{
			name:        "multiple invalid dropped in field order, valid kept",
			cfg:         &models.ReplicationAsyncConfig{HashtreeHeight: i64(99), Frequency: i64(-1), LoggingFrequency: i64(0), PropagationConcurrency: i64(5)},
			want:        &models.ReplicationAsyncConfig{PropagationConcurrency: i64(5)},
			wantDropped: []string{"hashtreeHeight: value 99 out of range: min 0, max 20", "frequency must be >= 0", "loggingFrequency must be > 0"},
		},
		{
			name:        "frequency overflow dropped",
			cfg:         &models.ReplicationAsyncConfig{Frequency: i64(MaxDurationMillis + 1)},
			want:        &models.ReplicationAsyncConfig{},
			wantDropped: []string{"frequency too large"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sanitized, dropped := SanitizeAsyncConfig(tt.cfg)
			assert.Equal(t, tt.want, sanitized)
			require.Len(t, dropped, len(tt.wantDropped))
			for i, wantErr := range tt.wantDropped {
				assert.ErrorContains(t, dropped[i], wantErr)
			}
			assert.NoError(t, ValidateAsyncConfig(sanitized))
		})
	}
}

func TestSanitizeAsyncConfigDoesNotMutateInput(t *testing.T) {
	freq := int64(-1)
	cfg := &models.ReplicationAsyncConfig{Frequency: &freq}
	_, dropped := SanitizeAsyncConfig(cfg)
	require.Len(t, dropped, 1)
	require.NotNil(t, cfg.Frequency)
	require.Equal(t, int64(-1), *cfg.Frequency)
}
