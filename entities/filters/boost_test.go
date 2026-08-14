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

func TestValidateBoost(t *testing.T) {
	tests := []struct {
		name    string
		boost   *Boost
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil boost returns no error",
			boost:   nil,
			wantErr: false,
		},
		{
			name:    "empty conditions returns error",
			boost:   &Boost{Conditions: []BoostCondition{}},
			wantErr: true,
			errMsg:  "at least one condition is required",
		},
		{
			name: "negative weight returns error",
			boost: &Boost{
				Weight: -1.0,
				Conditions: []BoostCondition{
					{Filter: &LocalFilter{}},
				},
			},
			wantErr: true,
			errMsg:  "weight must be between 0 and 1",
		},
		{
			name: "condition with none set returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{Weight: 1.0},
				},
			},
			wantErr: true,
			errMsg:  "exactly one of 'filter', 'decay', or 'property_value' must be set",
		},
		{
			name: "condition with both filter and decay returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{
						Filter: &LocalFilter{},
						Decay:  &Decay{Path: &Path{Property: "age"}, Origin: "30", Scale: "10"},
					},
				},
			},
			wantErr: true,
			errMsg:  "exactly one of 'filter', 'decay', or 'property_value' must be set",
		},
		{
			name: "condition with negative weight is valid",
			boost: &Boost{
				Conditions: []BoostCondition{
					{Filter: &LocalFilter{}, Weight: -0.5},
				},
			},
			wantErr: false,
		},
		{
			name: "decay with missing path returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{Decay: &Decay{Scale: "10"}},
				},
			},
			wantErr: true,
			errMsg:  "path is required",
		},
		{
			name: "decay with missing origin is valid (defaults to now for dates)",
			boost: &Boost{
				Conditions: []BoostCondition{
					{Decay: &Decay{Path: &Path{Property: "created_at"}, Scale: "7d"}},
				},
			},
			wantErr: false,
		},
		{
			name: "decay with missing scale returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{Decay: &Decay{Path: &Path{Property: "age"}, Origin: "30"}},
				},
			},
			wantErr: true,
			errMsg:  "scale is required",
		},
		{
			name: "decay with invalid curve returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
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
			name: "negative depth returns error",
			boost: &Boost{
				Depth: -1,
				Conditions: []BoostCondition{
					{Filter: &LocalFilter{}, Weight: 1.0},
				},
			},
			wantErr: true,
			errMsg:  "depth must be >= 0",
		},
		{
			name: "positive depth is valid",
			boost: &Boost{
				Depth: 500,
				Conditions: []BoostCondition{
					{Filter: &LocalFilter{}, Weight: 1.0},
				},
			},
			wantErr: false,
		},
		{
			name: "property_value with missing path returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{PropertyValue: &PropertyValue{Modifier: "log1p"}},
				},
			},
			wantErr: true,
			errMsg:  "path is required",
		},
		{
			name: "property_value with invalid modifier returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{PropertyValue: &PropertyValue{
						Path:     &Path{Property: "likes"},
						Modifier: "invalid",
					}},
				},
			},
			wantErr: true,
			errMsg:  "modifier must be one of",
		},
		{
			name: "valid property_value condition",
			boost: &Boost{
				Conditions: []BoostCondition{
					{PropertyValue: &PropertyValue{
						Path:     &Path{Property: "likes"},
						Modifier: "log1p",
					}},
				},
			},
			wantErr: false,
		},
		{
			name: "condition with filter and property_value returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{
						Filter:        &LocalFilter{},
						PropertyValue: &PropertyValue{Path: &Path{Property: "likes"}},
					},
				},
			},
			wantErr: true,
			errMsg:  "exactly one of",
		},
		{
			name: "filter with WithinGeoRange returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{Filter: &LocalFilter{Root: &Clause{Operator: OperatorWithinGeoRange}}},
				},
			},
			wantErr: true,
			errMsg:  "not supported in boost conditions",
		},
		{
			name: "filter with ContainsAny returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{Filter: &LocalFilter{Root: &Clause{Operator: ContainsAny}}},
				},
			},
			wantErr: true,
			errMsg:  "not supported in boost conditions",
		},
		{
			name: "filter with nested unsupported operator returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{Filter: &LocalFilter{Root: &Clause{
						Operator: OperatorAnd,
						Operands: []Clause{
							{Operator: OperatorEqual},
							{Operator: ContainsAll},
						},
					}}},
				},
			},
			wantErr: true,
			errMsg:  "not supported in boost conditions",
		},
		{
			name: "valid filter condition returns no error",
			boost: &Boost{
				Weight: 0.5,
				Conditions: []BoostCondition{
					{Filter: &LocalFilter{}, Weight: 1.0},
				},
			},
			wantErr: false,
		},
		{
			name: "valid decay condition returns no error",
			boost: &Boost{
				Weight: 0.5,
				Conditions: []BoostCondition{
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
			name: "negative decay_value returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{Decay: &Decay{
						Path:       &Path{Property: "age"},
						Origin:     "30",
						Scale:      "10",
						DecayValue: -0.5,
					}},
				},
			},
			wantErr: true,
			errMsg:  "decay_value must be between 0 and 1",
		},
		{
			name: "decay_value greater than 1 returns error",
			boost: &Boost{
				Conditions: []BoostCondition{
					{Decay: &Decay{
						Path:       &Path{Property: "age"},
						Origin:     "30",
						Scale:      "10",
						DecayValue: 1.5,
					}},
				},
			},
			wantErr: true,
			errMsg:  "decay_value must be between 0 and 1",
		},
		{
			name: "multiple valid conditions returns no error",
			boost: &Boost{
				Weight: 0.8,
				Conditions: []BoostCondition{
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
			err := ValidateBoost(tt.boost)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
