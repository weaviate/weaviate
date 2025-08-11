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

package hnsw

import (
	"testing"

	"github.com/weaviate/weaviate/entities/schema/config"
)

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
