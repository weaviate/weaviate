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

package hfresh

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func muveraConfig(ksim, dprojections, repetitions int) UserConfig {
	uc := NewDefaultUserConfig()
	uc.Multivector.Enabled = true
	uc.Multivector.MuveraConfig.Enabled = true
	if ksim > 0 {
		uc.Multivector.MuveraConfig.KSim = ksim
	}
	if dprojections > 0 {
		uc.Multivector.MuveraConfig.DProjections = dprojections
	}
	if repetitions > 0 {
		uc.Multivector.MuveraConfig.Repetitions = repetitions
	}
	return uc
}

func TestValidateMuveraUpperBounds(t *testing.T) {
	tests := []struct {
		name    string
		uc      UserConfig
		wantErr string
	}{
		{name: "defaults", uc: muveraConfig(0, 0, 0)},
		{name: "ksim one million", uc: muveraConfig(1_000_000, 0, 0), wantErr: "ksim"},
		{name: "ksim INT_MAX", uc: muveraConfig(math.MaxInt32, 0, 0), wantErr: "ksim"},
		{name: "dprojections one hundred thousand", uc: muveraConfig(0, 100_000, 0), wantErr: "dprojections"},
		{name: "repetitions one million", uc: muveraConfig(0, 0, 1_000_000), wantErr: "repetitions"},
		{name: "ksim at the limit", uc: muveraConfig(MaximumAllowedMuveraKSim, 0, 0)},
		{name: "ksim above the limit", uc: muveraConfig(MaximumAllowedMuveraKSim+1, 0, 0), wantErr: "ksim"},
		{name: "dprojections at the limit", uc: muveraConfig(0, MaximumAllowedMuveraDProjections, 0)},
		{name: "dprojections above the limit", uc: muveraConfig(0, MaximumAllowedMuveraDProjections+1, 0), wantErr: "dprojections"},
		{name: "repetitions at the limit", uc: muveraConfig(0, 0, MaximumAllowedMuveraRepetitions)},
		{name: "repetitions above the limit", uc: muveraConfig(0, 0, MaximumAllowedMuveraRepetitions+1), wantErr: "repetitions"},
		{
			name:    "individually valid but combined FDE too large",
			uc:      muveraConfig(MaximumAllowedMuveraKSim, MaximumAllowedMuveraDProjections, MaximumAllowedMuveraRepetitions),
			wantErr: "encoded vector length",
		},
		{
			// 8 x 2^10 x 128 = 2^20, exactly at the combined limit
			name: "combined FDE exactly at the limit",
			uc:   muveraConfig(10, 128, 8),
		},
		{
			name: "muvera disabled skips all bounds",
			uc: func() UserConfig {
				uc := muveraConfig(math.MaxInt32, math.MaxInt32, math.MaxInt32)
				uc.Multivector.MuveraConfig.Enabled = false
				return uc
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMuveraUpperBounds(tt.uc)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestParseAcceptsOutOfBoundsMuvera pins the startup-safety contract: the
// config parser — which also runs when restoring persisted schemas or
// replaying the RAFT log at node startup — must NOT enforce the muvera upper
// bounds. A class persisted with out-of-range values has to load (the index
// logs a warning); only schema create/update rejects them, via
// ValidateMuveraUpperBounds in the schema handler.
func TestParseAcceptsOutOfBoundsMuvera(t *testing.T) {
	input := map[string]interface{}{
		"multivector": map[string]interface{}{
			"enabled": true,
			"muvera": map[string]interface{}{
				"enabled":      true,
				"ksim":         float64(1_000_000),
				"dprojections": float64(100_000),
				"repetitions":  float64(1_000_000),
			},
		},
	}

	cfg, err := ParseAndValidateConfig(input, true)
	require.NoError(t, err, "startup parsing must tolerate out-of-range muvera params")

	uc, ok := cfg.(UserConfig)
	require.True(t, ok)
	require.Equal(t, 1_000_000, uc.Multivector.MuveraConfig.KSim)
	require.Error(t, ValidateMuveraUpperBounds(uc), "the same config must fail the create/update bounds")
}
