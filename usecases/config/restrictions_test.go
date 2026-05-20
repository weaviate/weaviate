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

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
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

// TestRestrictionListValidator covers the per-DynamicValue validator
// wired in environment.go: a YAML runtime override pushing an entry
// outside the canonical set must be rejected at SetValue time so it
// never lands in the live config. Cross-field rules (multi-entry needs
// default, hfresh+compression) intentionally remain a startup-only
// check; the read-side accessors normalize defensively for those.
func TestRestrictionListValidator(t *testing.T) {
	vectorValidator := NewRestrictionVectorIndexTypeListValidator()
	compressionValidator := NewRestrictionCompressionTypeListValidator()

	t.Run("vector valid passes", func(t *testing.T) {
		require.NoError(t, vectorValidator([]string{"hnsw", "flat"}))
	})
	t.Run("vector mixed case passes (normalized before check)", func(t *testing.T) {
		require.NoError(t, vectorValidator([]string{" HNSW ", "Flat"}))
	})
	t.Run("vector empty entries tolerated", func(t *testing.T) {
		require.NoError(t, vectorValidator([]string{"", " ", "hnsw"}))
	})
	t.Run("vector unknown rejected", func(t *testing.T) {
		err := vectorValidator([]string{"hnsw", "bogus"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bogus")
	})
	t.Run("compression valid passes", func(t *testing.T) {
		require.NoError(t, compressionValidator([]string{"rq-8", "bq", "none"}))
	})
	t.Run("compression unknown rejected", func(t *testing.T) {
		err := compressionValidator([]string{"rq-8", "rq8"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rq8")
	})
	t.Run("nil tolerated", func(t *testing.T) {
		require.NoError(t, vectorValidator(nil))
		require.NoError(t, compressionValidator(nil))
	})
}

func TestValidateRestrictions_WhitespaceOnly_PersistsEmpty(t *testing.T) {
	// Regression: when the env var contains only blank/whitespace entries
	// (e.g. `ALLOWED_VECTOR_INDEX_TYPES=,` or `, , `), normalization
	// collapses to an empty slice. The validator must persist that empty
	// slice back so downstream readers don't see the original non-empty
	// raw input. Without this, allowedVectorIndexTypes() would treat the
	// raw `["", ""]` as a real allow-list and reject every class.
	c := &Config{
		DefaultVectorIndexType: runtime.NewDynamicValue(""),
		DefaultQuantization:    runtime.NewDynamicValue(""),
		Restrictions: RestrictionsConfig{
			// Mirrors what `strings.Split(",", ",")` produces for env
			// vars like `ALLOWED_VECTOR_INDEX_TYPES=,` — a slice of
			// blank strings that should normalize to empty.
			AllowedVectorIndexTypes: runtime.NewDynamicValue([]string{"", " ", "\t"}),
			AllowedCompressionTypes: runtime.NewDynamicValue([]string{"", " "}),
		},
	}
	require.NoError(t, c.validateRestrictions())
	assert.Empty(t, c.Restrictions.AllowedVectorIndexTypes.Get(),
		"whitespace-only input should normalize to empty, not be passed through")
	assert.Empty(t, c.Restrictions.AllowedCompressionTypes.Get(),
		"whitespace-only input should normalize to empty, not be passed through")
}

// TestValidateRestrictionsRuntime covers the hook that re-runs
// cross-field invariants after a runtime YAML override changes any of
// the Allowed* / Default* fields. Per-value validation runs at
// SetValue time (covered by TestRestrictionListValidator above); this
// is the cross-field layer.
//
// Failure semantics: when an invariant is violated, the hook logs and
// resets BOTH allow-lists to empty so the schema layer falls back to
// "no restriction" rather than running with a broken config. Operators
// see the loud log, fix their YAML, restriction comes back on the next
// reload.
func TestValidateRestrictionsRuntime(t *testing.T) {
	t.Run("valid config passes silently", func(t *testing.T) {
		log, hook := logrustest.NewNullLogger()
		c := &Config{
			DefaultVectorIndexType: runtime.NewDynamicValue("hfresh"),
			DefaultQuantization:    runtime.NewDynamicValue("rq-8"),
			Restrictions: RestrictionsConfig{
				AllowedVectorIndexTypes: runtime.NewDynamicValue([]string{"hfresh", "hnsw"}),
				AllowedCompressionTypes: runtime.NewDynamicValue([]string{"rq-8"}),
			},
		}
		require.NoError(t, c.ValidateRestrictionsRuntime(log))
		for _, e := range hook.AllEntries() {
			assert.NotEqual(t, logrus.ErrorLevel, e.Level,
				"valid config should not emit an error: %s", e.Message)
		}
		// Allow-lists untouched.
		assert.Equal(t, []string{"hfresh", "hnsw"}, c.Restrictions.AllowedVectorIndexTypes.Get())
		assert.Equal(t, []string{"rq-8"}, c.Restrictions.AllowedCompressionTypes.Get())
	})

	t.Run("hfresh-only + compression set resets to empty", func(t *testing.T) {
		log, hook := logrustest.NewNullLogger()
		c := &Config{
			DefaultVectorIndexType: runtime.NewDynamicValue("hfresh"),
			DefaultQuantization:    runtime.NewDynamicValue(""),
			Restrictions: RestrictionsConfig{
				AllowedVectorIndexTypes: runtime.NewDynamicValue([]string{"hfresh"}),
				AllowedCompressionTypes: runtime.NewDynamicValue([]string{"rq-8"}),
			},
		}
		require.NoError(t, c.ValidateRestrictionsRuntime(log))
		// Both lists reset to empty (no restriction).
		assert.Empty(t, c.Restrictions.AllowedVectorIndexTypes.Get())
		assert.Empty(t, c.Restrictions.AllowedCompressionTypes.Get())
		// An error log was emitted naming the violated invariant.
		require.NotEmpty(t, hook.AllEntries())
		found := false
		for _, e := range hook.AllEntries() {
			if e.Level == logrus.ErrorLevel && strings.Contains(e.Message, "hfresh") {
				found = true
				break
			}
		}
		assert.True(t, found, "expected an error log mentioning hfresh")
	})

	t.Run("multi-entry without matching default resets to empty", func(t *testing.T) {
		log, hook := logrustest.NewNullLogger()
		c := &Config{
			DefaultVectorIndexType: runtime.NewDynamicValue(""),
			DefaultQuantization:    runtime.NewDynamicValue(""),
			Restrictions: RestrictionsConfig{
				AllowedVectorIndexTypes: runtime.NewDynamicValue([]string{"hfresh", "hnsw"}),
				AllowedCompressionTypes: runtime.NewDynamicValue[[]string](nil),
			},
		}
		require.NoError(t, c.ValidateRestrictionsRuntime(log))
		assert.Empty(t, c.Restrictions.AllowedVectorIndexTypes.Get())
		require.NotEmpty(t, hook.AllEntries())
	})

	t.Run("default not in allow-list resets to empty", func(t *testing.T) {
		log, hook := logrustest.NewNullLogger()
		c := &Config{
			DefaultVectorIndexType: runtime.NewDynamicValue("flat"),
			DefaultQuantization:    runtime.NewDynamicValue(""),
			Restrictions: RestrictionsConfig{
				AllowedVectorIndexTypes: runtime.NewDynamicValue([]string{"hfresh", "hnsw"}),
				AllowedCompressionTypes: runtime.NewDynamicValue[[]string](nil),
			},
		}
		require.NoError(t, c.ValidateRestrictionsRuntime(log))
		assert.Empty(t, c.Restrictions.AllowedVectorIndexTypes.Get())
		require.NotEmpty(t, hook.AllEntries())
	})

	t.Run("invalid enum entry resets to empty (defensive)", func(t *testing.T) {
		// Per-value validators should catch this at SetValue time, but
		// the hook is the second line of defense.
		log, _ := logrustest.NewNullLogger()
		c := &Config{
			DefaultVectorIndexType: runtime.NewDynamicValue(""),
			DefaultQuantization:    runtime.NewDynamicValue(""),
			Restrictions: RestrictionsConfig{
				AllowedVectorIndexTypes: runtime.NewDynamicValue([]string{"bogus"}),
				AllowedCompressionTypes: runtime.NewDynamicValue[[]string](nil),
			},
		}
		require.NoError(t, c.ValidateRestrictionsRuntime(log))
		assert.Empty(t, c.Restrictions.AllowedVectorIndexTypes.Get())
	})

	// Single-entry allow-list with an EXPLICIT default that doesn't
	// match: boot-time validator's reconcileAllowListWithDefault
	// rejects this, the runtime path used to silently let it through.
	// The fail-safe is the same as the other invariant violations —
	// reset both allow-lists, log the problem.
	t.Run("single-entry vector list with mismatched default resets to empty", func(t *testing.T) {
		log, hook := logrustest.NewNullLogger()
		c := &Config{
			DefaultVectorIndexType: runtime.NewDynamicValue("hnsw"),
			DefaultQuantization:    runtime.NewDynamicValue(""),
			Restrictions: RestrictionsConfig{
				AllowedVectorIndexTypes: runtime.NewDynamicValue([]string{"hfresh"}),
				AllowedCompressionTypes: runtime.NewDynamicValue[[]string](nil),
			},
		}
		require.NoError(t, c.ValidateRestrictionsRuntime(log))
		assert.Empty(t, c.Restrictions.AllowedVectorIndexTypes.Get(),
			"single-entry mismatched default should fail-safe to empty")
		require.NotEmpty(t, hook.AllEntries())
	})

	t.Run("single-entry compression list with mismatched default resets to empty", func(t *testing.T) {
		log, hook := logrustest.NewNullLogger()
		c := &Config{
			DefaultVectorIndexType: runtime.NewDynamicValue("hnsw"),
			DefaultQuantization:    runtime.NewDynamicValue("pq"),
			Restrictions: RestrictionsConfig{
				AllowedVectorIndexTypes: runtime.NewDynamicValue[[]string](nil),
				AllowedCompressionTypes: runtime.NewDynamicValue([]string{"rq-8"}),
			},
		}
		require.NoError(t, c.ValidateRestrictionsRuntime(log))
		assert.Empty(t, c.Restrictions.AllowedCompressionTypes.Get())
		require.NotEmpty(t, hook.AllEntries())
	})

	// Single-entry list with an UNSET default is fine at runtime: boot
	// seeds the default in that case, but the runtime path intentionally
	// does NOT mutate the default (boot-only seeding contract). The
	// allow-list stays in effect; downstream readers see the empty
	// default and fall through whatever the prior default behaviour was.
	t.Run("single-entry list with unset default is accepted (no seed at runtime)", func(t *testing.T) {
		log, hook := logrustest.NewNullLogger()
		c := &Config{
			DefaultVectorIndexType: runtime.NewDynamicValue(""),
			DefaultQuantization:    runtime.NewDynamicValue(""),
			Restrictions: RestrictionsConfig{
				AllowedVectorIndexTypes: runtime.NewDynamicValue([]string{"hfresh"}),
				AllowedCompressionTypes: runtime.NewDynamicValue[[]string](nil),
			},
		}
		require.NoError(t, c.ValidateRestrictionsRuntime(log))
		assert.Equal(t, []string{"hfresh"}, c.Restrictions.AllowedVectorIndexTypes.Get())
		assert.Equal(t, "", c.DefaultVectorIndexType.Get(),
			"runtime hook must not seed the default — that's boot-time-only behaviour")
		for _, e := range hook.AllEntries() {
			assert.NotEqual(t, logrus.ErrorLevel, e.Level,
				"unset default with single-entry list is valid at runtime: %s", e.Message)
		}
	})
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
