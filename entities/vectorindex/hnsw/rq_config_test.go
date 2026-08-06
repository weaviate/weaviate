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

package hnsw

import (
	"testing"

	"github.com/weaviate/weaviate/entities/schema/config"
)

func TestValidateRQConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  RQConfig
		wantErr bool
	}{
		{
			name:    "disabled config skips bits validation",
			config:  RQConfig{Enabled: false, Bits: 3},
			wantErr: false,
		},
		{
			name:    "enabled with bits=1",
			config:  RQConfig{Enabled: true, Bits: 1},
			wantErr: false,
		},
		{
			name:    "enabled with bits=4",
			config:  RQConfig{Enabled: true, Bits: 4},
			wantErr: false,
		},
		{
			name:    "enabled with bits=8",
			config:  RQConfig{Enabled: true, Bits: 8},
			wantErr: false,
		},
		{
			name:    "enabled with bits=0",
			config:  RQConfig{Enabled: true, Bits: 0},
			wantErr: true,
		},
		{
			name:    "enabled with bits=2",
			config:  RQConfig{Enabled: true, Bits: 2},
			wantErr: true,
		},
		{
			name:    "enabled with bits=16",
			config:  RQConfig{Enabled: true, Bits: 16},
			wantErr: true,
		},
		{
			name:    "centering with bits=4",
			config:  RQConfig{Enabled: true, Bits: 4, Centering: true, TrainingLimit: DefaultRQTrainingLimit},
			wantErr: false,
		},
		{
			name:    "centering with bits=8 rejected",
			config:  RQConfig{Enabled: true, Bits: 8, Centering: true, TrainingLimit: DefaultRQTrainingLimit},
			wantErr: true,
		},
		{
			name:    "centering with bits=1 rejected",
			config:  RQConfig{Enabled: true, Bits: 1, Centering: true, TrainingLimit: DefaultRQTrainingLimit},
			wantErr: true,
		},
		{
			name:    "centering with non-positive trainingLimit rejected",
			config:  RQConfig{Enabled: true, Bits: 4, Centering: true, TrainingLimit: 0},
			wantErr: true,
		},
		{
			name:    "disabled config skips centering validation",
			config:  RQConfig{Enabled: false, Bits: 8, Centering: true},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRQConfig(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateRQConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseRQMapCentering(t *testing.T) {
	uc := UserConfig{}
	uc.SetDefaults()
	if uc.RQ.TrainingLimit != DefaultRQTrainingLimit {
		t.Fatalf("default trainingLimit = %d, want %d", uc.RQ.TrainingLimit, DefaultRQTrainingLimit)
	}
	if uc.RQ.Centering {
		t.Fatal("centering must default to false")
	}

	in := map[string]interface{}{
		"rq": map[string]interface{}{
			"enabled":       true,
			"bits":          float64(4),
			"centering":     true,
			"trainingLimit": float64(5000),
		},
	}
	if err := parseRQMap(in, &uc.RQ); err != nil {
		t.Fatalf("parseRQMap: %v", err)
	}
	if !uc.RQ.Centering || uc.RQ.TrainingLimit != 5000 || uc.RQ.Bits != 4 {
		t.Fatalf("parsed config mismatch: %+v", uc.RQ)
	}
	if err := ValidateRQConfig(uc.RQ); err != nil {
		t.Fatalf("parsed centered config should validate: %v", err)
	}
}

func TestGetRQBits(t *testing.T) {
	tests := []struct {
		name     string
		config   config.VectorIndexConfig
		expected int16
	}{
		{
			name: "RQ disabled should return 0",
			config: UserConfig{
				RQ: RQConfig{
					Enabled: false,
					Bits:    8,
				},
			},
			expected: 0,
		},
		{
			name: "RQ enabled with bits=1 should return 1",
			config: UserConfig{
				RQ: RQConfig{
					Enabled: true,
					Bits:    1,
				},
			},
			expected: 1,
		},
		{
			name: "RQ enabled with bits=4 should return 4",
			config: UserConfig{
				RQ: RQConfig{
					Enabled: true,
					Bits:    4,
				},
			},
			expected: 4,
		},
		{
			name: "RQ enabled with bits=8 should return 8",
			config: UserConfig{
				RQ: RQConfig{
					Enabled: true,
					Bits:    8,
				},
			},
			expected: 8,
		},
		{
			name: "non-RQ config should return 0",
			config: UserConfig{
				BQ: BQConfig{
					Enabled: true,
				},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetRQBits(tt.config)
			if result != tt.expected {
				t.Errorf("GetRQBits() = %v, want %v", result, tt.expected)
			}
		})
	}
}
