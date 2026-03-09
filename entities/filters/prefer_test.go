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

package filters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidatePrefer(t *testing.T) {
	tests := []struct {
		name    string
		prefer  *Prefer
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil prefer returns no error",
			prefer:  nil,
			wantErr: false,
		},
		{
			name:    "empty conditions returns error",
			prefer:  &Prefer{Conditions: []PreferCondition{}},
			wantErr: true,
			errMsg:  "at least one condition is required",
		},
		{
			name: "negative strength returns error",
			prefer: &Prefer{
				Strength: -1.0,
				Conditions: []PreferCondition{
					{Filter: &LocalFilter{}},
				},
			},
			wantErr: true,
			errMsg:  "strength must be between 0 and 1",
		},
		{
			name: "condition with neither filter nor decay returns error",
			prefer: &Prefer{
				Conditions: []PreferCondition{
					{Weight: 1.0},
				},
			},
			wantErr: true,
			errMsg:  "exactly one of 'filter' or 'decay' must be set",
		},
		{
			name: "condition with both filter and decay returns error",
			prefer: &Prefer{
				Conditions: []PreferCondition{
					{
						Filter: &LocalFilter{},
						Decay:  &Decay{Path: &Path{Property: "age"}, Origin: "30", Scale: "10"},
					},
				},
			},
			wantErr: true,
			errMsg:  "both are set",
		},
		{
			name: "condition with negative weight returns error",
			prefer: &Prefer{
				Conditions: []PreferCondition{
					{Filter: &LocalFilter{}, Weight: -0.5},
				},
			},
			wantErr: true,
			errMsg:  "weight must be >= 0",
		},
		{
			name: "decay with missing path returns error",
			prefer: &Prefer{
				Conditions: []PreferCondition{
					{Decay: &Decay{Scale: "10"}},
				},
			},
			wantErr: true,
			errMsg:  "path is required",
		},
		{
			name: "decay with missing origin returns error",
			prefer: &Prefer{
				Conditions: []PreferCondition{
					{Decay: &Decay{Path: &Path{Property: "age"}, Scale: "10"}},
				},
			},
			wantErr: true,
			errMsg:  "origin is required",
		},
		{
			name: "decay with missing scale returns error",
			prefer: &Prefer{
				Conditions: []PreferCondition{
					{Decay: &Decay{Path: &Path{Property: "age"}, Origin: "30"}},
				},
			},
			wantErr: true,
			errMsg:  "scale is required",
		},
		{
			name: "decay with invalid curve returns error",
			prefer: &Prefer{
				Conditions: []PreferCondition{
					{Decay: &Decay{
						Path:   &Path{Property: "age"},
						Origin: "30",
						Scale:  "10",
						Curve:  "invalid",
					}},
				},
			},
			wantErr: true,
			errMsg:  "curve must be one of",
		},
		{
			name: "valid filter condition returns no error",
			prefer: &Prefer{
				Strength: 0.5,
				Conditions: []PreferCondition{
					{Filter: &LocalFilter{}, Weight: 1.0},
				},
			},
			wantErr: false,
		},
		{
			name: "valid decay condition returns no error",
			prefer: &Prefer{
				Strength: 0.5,
				Conditions: []PreferCondition{
					{Decay: &Decay{
						Path:   &Path{Property: "age"},
						Origin: "30",
						Scale:  "10",
						Curve:  "exp",
					}, Weight: 1.0},
				},
			},
			wantErr: false,
		},
		{
			name: "multiple valid conditions returns no error",
			prefer: &Prefer{
				Strength: 0.8,
				Conditions: []PreferCondition{
					{Filter: &LocalFilter{}, Weight: 1.0},
					{Decay: &Decay{
						Path:   &Path{Property: "timestamp"},
						Origin: "now",
						Scale:  "7d",
						Curve:  "gauss",
					}, Weight: 2.0},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePrefer(tt.prefer)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
