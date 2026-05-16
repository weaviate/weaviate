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

package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestValidateRestrictions(t *testing.T) {
	tests := []struct {
		name             string
		allowVector      []string
		allowCompression []string
		defaultVector    string
		defaultQuant     string
		wantErrContains  string
		// Optional asserts on side effects after a successful call.
		wantDefaultVector string
		wantDefaultQuant  string
	}{
		// --- unrestricted baseline ---
		{
			name: "no restrictions, no defaults — unrestricted",
		},
		{
			name:          "no restrictions, with defaults set — left alone",
			defaultVector: "hnsw",
			defaultQuant:  "rq-8",
			// no allow lists → no enforcement, no mutation
			wantDefaultVector: "hnsw",
			wantDefaultQuant:  "rq-8",
		},

		// --- vector index allow-list rules ---
		{
			name:              "single vector allow + unset default seeds default",
			allowVector:       []string{"hfresh"},
			wantDefaultVector: "hfresh",
		},
		{
			name:              "single vector allow + matching default — OK",
			allowVector:       []string{"hfresh"},
			defaultVector:     "hfresh",
			wantDefaultVector: "hfresh",
		},
		{
			name:            "single vector allow + mismatched default — error",
			allowVector:     []string{"hfresh"},
			defaultVector:   "hnsw",
			wantErrContains: "DEFAULT_VECTOR_INDEX",
		},
		{
			name:            "multi vector allow without default — error",
			allowVector:     []string{"hfresh", "hnsw"},
			wantErrContains: "DEFAULT_VECTOR_INDEX must also be set",
		},
		{
			name:              "multi vector allow with default in list — OK",
			allowVector:       []string{"hfresh", "hnsw"},
			defaultVector:     "hfresh",
			wantDefaultVector: "hfresh",
		},
		{
			name:            "multi vector allow with default not in list — error",
			allowVector:     []string{"hfresh", "hnsw"},
			defaultVector:   "flat",
			wantErrContains: "is not in ALLOWED_VECTOR_INDEX_TYPES",
		},
		{
			name:            "invalid vector value — error",
			allowVector:     []string{"hfresh", "bogus"},
			wantErrContains: "invalid entry",
		},

		// --- compression allow-list rules (mirror vector rules) ---
		{
			name:             "single compression allow + unset default seeds default",
			allowVector:      []string{"hnsw"}, // need a non-hfresh-only vector list
			allowCompression: []string{"rq-8"},
			wantDefaultQuant: "rq-8",
		},
		{
			name:             "compression rq-1 valid",
			allowVector:      []string{"hnsw"},
			allowCompression: []string{"rq-1"},
			wantDefaultQuant: "rq-1",
		},
		{
			name:             "compression none valid",
			allowVector:      []string{"hnsw"},
			allowCompression: []string{"none"},
			wantDefaultQuant: "none",
		},
		{
			name:             "single compression + matching default",
			allowVector:      []string{"hnsw"},
			allowCompression: []string{"rq-8"},
			defaultQuant:     "rq-8",
			wantDefaultQuant: "rq-8",
		},
		{
			name:             "single compression + mismatched default — error",
			allowVector:      []string{"hnsw"},
			allowCompression: []string{"rq-8"},
			defaultQuant:     "pq",
			wantErrContains:  "DEFAULT_QUANTIZATION",
		},
		{
			name:             "multi compression without default — error",
			allowVector:      []string{"hnsw"},
			allowCompression: []string{"rq-8", "bq"},
			wantErrContains:  "DEFAULT_QUANTIZATION must also be set",
		},
		{
			name:             "multi compression with default in list — OK",
			allowVector:      []string{"hnsw"},
			allowCompression: []string{"rq-8", "bq"},
			defaultQuant:     "bq",
			wantDefaultQuant: "bq",
		},
		{
			name:             "multi compression with default not in list — error",
			allowVector:      []string{"hnsw"},
			allowCompression: []string{"rq-8", "bq"},
			defaultQuant:     "pq",
			wantErrContains:  "is not in ALLOWED_COMPRESSION_TYPES",
		},
		{
			name:             "invalid compression value — error",
			allowVector:      []string{"hnsw"},
			allowCompression: []string{"rq8"}, // misspelling of rq-8
			wantErrContains:  "invalid entry",
		},

		// --- hfresh + compression interaction ---
		{
			name:             "hfresh-only + compression set — error",
			allowVector:      []string{"hfresh"},
			allowCompression: []string{"rq-8"},
			wantErrContains:  "hfresh has no compression",
		},
		{
			name:             "hfresh+hnsw + compression set — OK (compression applies to hnsw)",
			allowVector:      []string{"hfresh", "hnsw"},
			defaultVector:    "hfresh",
			allowCompression: []string{"rq-8"},
			wantDefaultQuant: "rq-8",
		},

		// --- normalization ---
		{
			name:              "case + whitespace normalized",
			allowVector:       []string{" HNSW ", "Flat"},
			defaultVector:     "HNSW",
			wantDefaultVector: "hnsw",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				DefaultVectorIndexType: runtime.NewDynamicValue(tt.defaultVector),
				DefaultQuantization:    runtime.NewDynamicValue(tt.defaultQuant),
			}
			if tt.allowVector != nil {
				c.Restrictions.AllowedVectorIndexTypes = runtime.NewDynamicValue(tt.allowVector)
			}
			if tt.allowCompression != nil {
				c.Restrictions.AllowedCompressionTypes = runtime.NewDynamicValue(tt.allowCompression)
			}

			err := c.validateRestrictions()
			if tt.wantErrContains != "" {
				require.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.wantErrContains),
					"err = %q, want substring %q", err.Error(), tt.wantErrContains)
				return
			}
			require.NoError(t, err)
			if tt.wantDefaultVector != "" {
				assert.Equal(t, tt.wantDefaultVector, c.DefaultVectorIndexType.Get())
			}
			if tt.wantDefaultQuant != "" {
				assert.Equal(t, tt.wantDefaultQuant, c.DefaultQuantization.Get())
			}
		})
	}
}

func TestValidateRestrictions_EmptyAllowList_NoRestriction(t *testing.T) {
	c := &Config{
		DefaultVectorIndexType: runtime.NewDynamicValue(""),
		DefaultQuantization:    runtime.NewDynamicValue(""),
		Restrictions: RestrictionsConfig{
			AllowedVectorIndexTypes: runtime.NewDynamicValue[[]string](nil),
			AllowedCompressionTypes: runtime.NewDynamicValue[[]string](nil),
		},
	}
	require.NoError(t, c.validateRestrictions())
	// Defaults are left untouched (no seeding when allow-list is empty).
	assert.Equal(t, "", c.DefaultVectorIndexType.Get())
	assert.Equal(t, "", c.DefaultQuantization.Get())
}

func TestValidateRestrictions_DedupsAndNormalizesPersistedList(t *testing.T) {
	c := &Config{
		DefaultVectorIndexType: runtime.NewDynamicValue("hnsw"),
		DefaultQuantization:    runtime.NewDynamicValue("rq-8"),
		Restrictions: RestrictionsConfig{
			AllowedVectorIndexTypes: runtime.NewDynamicValue([]string{"HNSW", " hnsw ", "Flat"}),
			AllowedCompressionTypes: runtime.NewDynamicValue([]string{"RQ-8", "rq-8"}),
		},
	}
	require.NoError(t, c.validateRestrictions())
	// Order of remaining entries is preserved from first occurrence after dedupe.
	assert.Equal(t, []string{"hnsw", "flat"}, c.Restrictions.AllowedVectorIndexTypes.Get())
	assert.Equal(t, []string{"rq-8"}, c.Restrictions.AllowedCompressionTypes.Get())
}
