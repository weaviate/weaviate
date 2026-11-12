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

// TestParseMultiVectorModuleEnablesMuvera pins how hfresh reacts to a
// multi-vector vectorizer module (isMultiVector == true). hfresh has exactly
// one multi-vector mode, MUVERA, so a multi-vector module implies muvera the
// same way it implies multivector.enabled on hnsw. Only an explicit
// muvera.enabled=false is a contradiction and rejected.
func TestParseMultiVectorModuleEnablesMuvera(t *testing.T) {
	tests := []struct {
		name          string
		input         interface{}
		isMultiVector bool
		wantEnabled   bool
		wantMuvera    bool
		wantErr       string
	}{
		{
			name:          "multi-vector module, no config at all",
			input:         nil,
			isMultiVector: true,
			wantEnabled:   true,
			wantMuvera:    true,
		},
		{
			name:          "multi-vector module, empty config map",
			input:         map[string]interface{}{},
			isMultiVector: true,
			wantEnabled:   true,
			wantMuvera:    true,
		},
		{
			name: "multi-vector module, multivector enabled without muvera block",
			input: map[string]interface{}{
				"multivector": map[string]interface{}{"enabled": true},
			},
			isMultiVector: true,
			wantEnabled:   true,
			wantMuvera:    true,
		},
		{
			name: "multi-vector module, multivector explicitly disabled is overridden like hnsw",
			input: map[string]interface{}{
				"multivector": map[string]interface{}{"enabled": false},
			},
			isMultiVector: true,
			wantEnabled:   true,
			wantMuvera:    true,
		},
		{
			name: "multi-vector module, muvera explicitly enabled with params",
			input: map[string]interface{}{
				"multivector": map[string]interface{}{
					"enabled": true,
					"muvera":  map[string]interface{}{"enabled": true, "ksim": float64(3)},
				},
			},
			isMultiVector: true,
			wantEnabled:   true,
			wantMuvera:    true,
		},
		{
			name: "multi-vector module, muvera explicitly disabled is a contradiction",
			input: map[string]interface{}{
				"multivector": map[string]interface{}{
					"enabled": true,
					"muvera":  map[string]interface{}{"enabled": false},
				},
			},
			isMultiVector: true,
			wantErr:       "muvera",
		},
		{
			// startup-safety: without a multi-vector module the parser must
			// keep accepting this shape, a class persisted this way must load.
			// Create/update rejects it via ValidateMultiVectorRequiresMuvera.
			name: "no module, multivector enabled without muvera still parses",
			input: map[string]interface{}{
				"multivector": map[string]interface{}{"enabled": true},
			},
			isMultiVector: false,
			wantEnabled:   true,
			wantMuvera:    false,
		},
		{
			name:          "no module, no config",
			input:         nil,
			isMultiVector: false,
			wantEnabled:   false,
			wantMuvera:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseAndValidateConfig(tt.input, tt.isMultiVector)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			uc, ok := cfg.(UserConfig)
			require.True(t, ok)
			require.Equal(t, tt.wantEnabled, uc.Multivector.Enabled, "multivector.enabled")
			require.Equal(t, tt.wantMuvera, uc.Multivector.MuveraConfig.Enabled, "muvera.enabled")
			require.Equal(t, tt.wantMuvera, uc.IsMultiVector(), "IsMultiVector follows muvera")
			if tt.wantMuvera {
				require.Greater(t, uc.Multivector.MuveraConfig.KSim, 0, "muvera defaults must be populated")
				require.Greater(t, uc.Multivector.MuveraConfig.DProjections, 0)
				require.Greater(t, uc.Multivector.MuveraConfig.Repetitions, 0)
			}
		})
	}
}

// TestValidateMultiVectorRequiresMuvera pins the create/update-only rule:
// hfresh only takes multi-vectors MUVERA-encoded, so multivector.enabled
// without muvera.enabled is a config that accepts no insert and must be
// refused up front rather than stored.
func TestValidateMultiVectorRequiresMuvera(t *testing.T) {
	tests := []struct {
		name    string
		uc      UserConfig
		wantErr string
	}{
		{name: "defaults, single vector", uc: NewDefaultUserConfig()},
		{name: "multivector with muvera", uc: muveraConfig(0, 0, 0)},
		{
			name: "multivector without muvera",
			uc: func() UserConfig {
				uc := NewDefaultUserConfig()
				uc.Multivector.Enabled = true
				return uc
			}(),
			wantErr: "muvera",
		},
		{
			// muvera on its own implies multivector; nothing to reject
			name: "muvera without the multivector flag",
			uc: func() UserConfig {
				uc := NewDefaultUserConfig()
				uc.Multivector.MuveraConfig.Enabled = true
				return uc
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMultiVectorRequiresMuvera(tt.uc)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
