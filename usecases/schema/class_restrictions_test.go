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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/restrictions"
)

// TestValidateVectorIndexType_AllowList covers the allow-list gate on
// validateVectorIndexType. The function's pre-existing per-type
// validation (async-indexing required for dynamic, experimental flag
// for hfresh) is left in place; the allow-list runs after those checks.
func TestValidateVectorIndexType_AllowList(t *testing.T) {
	tests := []struct {
		name        string
		allow       []string
		indexType   string
		wantErr     bool
		wantViolate bool
		wantValue   string
	}{
		{
			name:      "no allow-list — hnsw OK",
			indexType: vectorindex.VectorIndexTypeHNSW,
		},
		{
			name:      "hfresh allowed",
			allow:     []string{"hfresh"},
			indexType: vectorindex.VectorIndexTypeHFresh,
		},
		{
			name:        "hfresh-only allow-list rejects hnsw",
			allow:       []string{"hfresh"},
			indexType:   vectorindex.VectorIndexTypeHNSW,
			wantErr:     true,
			wantViolate: true,
			wantValue:   "hnsw",
		},
		{
			name:      "multi-allow accepts each member",
			allow:     []string{"hfresh", "hnsw"},
			indexType: vectorindex.VectorIndexTypeHNSW,
		},
		{
			name:        "multi-allow rejects non-member",
			allow:       []string{"hfresh", "hnsw"},
			indexType:   vectorindex.VectorIndexTypeFLAT,
			wantErr:     true,
			wantViolate: true,
			wantValue:   "flat",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, _ := newTestHandler(t, &fakeDB{})
			handler.config.HFreshEnabled = true
			handler.asyncIndexingEnabled = true
			if tt.allow != nil {
				handler.config.Restrictions.AllowedVectorIndexTypes = runtime.NewDynamicValue(tt.allow)
			}

			err := handler.validateVectorIndexType(tt.indexType)
			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			if tt.wantViolate {
				v, ok := restrictions.AsViolation(err)
				require.True(t, ok, "expected *restrictions.ViolationError, got %T: %v", err, err)
				assert.Equal(t, restrictions.RestrictionVectorIndexType, v.Restriction)
				assert.Equal(t, tt.wantValue, v.Value)
				assert.Equal(t, tt.allow, v.Allowed)
			}
		})
	}
}

// TestValidateVectorIndexType_RuntimeOverrideNormalization is the
// regression for the Copilot review on runtime overrides: a YAML push
// can bypass startup validation, so the accessor must normalize at
// read time. Mixed-case entries that the validator at SetValue time
// somehow let through (test injection here) must still match the
// canonical lowercase form the schema layer compares against.
func TestValidateVectorIndexType_RuntimeOverrideNormalization(t *testing.T) {
	handler, _ := newTestHandler(t, &fakeDB{})
	handler.config.HFreshEnabled = true
	// Bypass the env-var validator by writing directly to the
	// DynamicValue — simulating a runtime push that contains
	// mixed-case + an unknown entry.
	dv := runtime.NewDynamicValue([]string{"HNSW", " FLAT ", "Bogus", "hnsw"})
	handler.config.Restrictions.AllowedVectorIndexTypes = dv

	// "hnsw" must be accepted: the accessor lowercases the entries.
	require.NoError(t, handler.validateVectorIndexType(vectorindex.VectorIndexTypeHNSW))
	require.NoError(t, handler.validateVectorIndexType(vectorindex.VectorIndexTypeFLAT))

	// "hfresh" not in the (normalized) list — rejected.
	err := handler.validateVectorIndexType(vectorindex.VectorIndexTypeHFresh)
	require.Error(t, err)
	v, ok := restrictions.AsViolation(err)
	require.True(t, ok)
	// The Allowed slice in the violation reflects what the accessor
	// surfaces: canonical lowercase, dedup'd, with the unknown
	// "bogus" entry filtered out.
	assert.Equal(t, []string{"hnsw", "flat"}, v.Allowed)
}

// TestValidateVectorSettingsAgainst_GrandfatherUnchanged is the
// regression for QA-#2: after an operator tightens the allow-list at
// runtime, a PUT on a pre-existing class whose body is unchanged must
// NOT be rejected with 422 — the restriction is meant to gate *new*
// configurations, not police pre-existing ones. Inserts and add-property
// already work on the existing data; this brings PUT-update in line.
//
// Validation matrix:
//  1. Add (initial=nil) with disallowed type → REJECTED (the new
//     restriction still applies to genuinely-new classes).
//  2. Update (initial != nil) with unchanged disallowed type → PASS
//     (grandfather: same value as stored, no policy violation introduced).
//  3. Update with type *changing* to a still-disallowed value → REJECTED
//     (the operator is actively introducing a violation, not preserving
//     pre-existing state).
//  4. Update with unchanged disallowed compression → PASS.
//  5. Update changing compression to a disallowed value → REJECTED.
func TestValidateVectorSettingsAgainst_GrandfatherUnchanged(t *testing.T) {
	t.Run("add with disallowed type — rejected", func(t *testing.T) {
		handler := newHandlerWithAllowList(t, []string{"hfresh", "hnsw"}, nil)
		err := handler.validateVectorSettingsAgainst(classWithType("flat"), nil)
		require.Error(t, err)
		_, ok := restrictions.AsViolation(err)
		assert.True(t, ok)
	})

	t.Run("update with unchanged disallowed type — passes (grandfathered)", func(t *testing.T) {
		handler := newHandlerWithAllowList(t, []string{"hfresh", "hnsw"}, nil)
		initial := classWithType("flat")
		updated := classWithType("flat")
		require.NoError(t, handler.validateVectorSettingsAgainst(updated, initial))
	})

	t.Run("update changing type to still-disallowed value — rejected", func(t *testing.T) {
		handler := newHandlerWithAllowList(t, []string{"hfresh", "hnsw"}, nil)
		initial := classWithType("flat")
		updated := classWithType("dynamic") // still not in [hfresh, hnsw]
		err := handler.validateVectorSettingsAgainst(updated, initial)
		require.Error(t, err)
		v, ok := restrictions.AsViolation(err)
		require.True(t, ok)
		assert.Equal(t, "dynamic", v.Value)
	})

	t.Run("update with unchanged disallowed compression — passes", func(t *testing.T) {
		handler := newHandlerWithAllowList(t, nil, []string{"rq-8"})
		initial := classWithHnswCompression(t, "pq")
		updated := classWithHnswCompression(t, "pq")
		require.NoError(t, handler.validateVectorSettingsAgainst(updated, initial))
	})

	t.Run("update changing to disallowed compression — rejected", func(t *testing.T) {
		handler := newHandlerWithAllowList(t, nil, []string{"rq-8"})
		initial := classWithHnswCompression(t, "rq-8")
		updated := classWithHnswCompression(t, "pq")
		err := handler.validateVectorSettingsAgainst(updated, initial)
		require.Error(t, err)
		v, ok := restrictions.AsViolation(err)
		require.True(t, ok)
		assert.Equal(t, "pq", v.Value)
	})
}

// --- helpers for the grandfather tests ---

func newHandlerWithAllowList(t *testing.T, vectorAllow, compressionAllow []string) *Handler {
	t.Helper()
	handler, _ := newTestHandler(t, &fakeDB{})
	handler.config.HFreshEnabled = true
	handler.asyncIndexingEnabled = true
	if vectorAllow != nil {
		handler.config.Restrictions.AllowedVectorIndexTypes = runtime.NewDynamicValue(vectorAllow)
	}
	if compressionAllow != nil {
		handler.config.Restrictions.AllowedCompressionTypes = runtime.NewDynamicValue(compressionAllow)
	}
	return handler
}

func classWithType(t string) *models.Class {
	return &models.Class{Class: "Demo", VectorIndexType: t, Vectorizer: "none"}
}

func classWithHnswCompression(t *testing.T, compression string) *models.Class {
	t.Helper()
	cfg := hnsw.UserConfig{}
	switch compression {
	case "pq":
		cfg.PQ.Enabled = true
	case "rq-8":
		cfg.RQ.Enabled = true
		cfg.RQ.Bits = 8
	default:
		t.Fatalf("unsupported test compression %q", compression)
	}
	return &models.Class{
		Class:             "Demo",
		VectorIndexType:   "hnsw",
		Vectorizer:        "none",
		VectorIndexConfig: cfg,
	}
}

func TestValidateVectorIndexType_AllowListUsesOperatorTemplate(t *testing.T) {
	handler, _ := newTestHandler(t, &fakeDB{})
	handler.config.HFreshEnabled = true
	handler.config.Restrictions.AllowedVectorIndexTypes = runtime.NewDynamicValue([]string{"hfresh"})
	handler.config.Restrictions.ErrorMessage = runtime.NewDynamicValue(
		"Invalid config: {value} is not allowed for {restriction} (allowed: {allowed})",
	)

	err := handler.validateVectorIndexType(vectorindex.VectorIndexTypeHNSW)
	require.Error(t, err)
	v, ok := restrictions.AsViolation(err)
	require.True(t, ok)
	assert.Contains(t, v.RenderedMessage, "Invalid config: hnsw is not allowed for vector_index_type")
	assert.Contains(t, v.RenderedMessage, "allowed: hfresh")
}

// --- compressionsFromIndexConfig ---

func TestCompressionsFromIndexConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  schemaConfig.VectorIndexConfig
		want []string
	}{
		{
			name: "hnsw no compression",
			cfg:  hnsw.UserConfig{},
			want: nil,
		},
		{
			name: "hnsw pq enabled",
			cfg:  hnsw.UserConfig{PQ: hnsw.PQConfig{Enabled: true}},
			want: []string{"pq"},
		},
		{
			name: "hnsw sq enabled",
			cfg:  hnsw.UserConfig{SQ: hnsw.SQConfig{Enabled: true}},
			want: []string{"sq"},
		},
		{
			name: "hnsw rq-8 enabled",
			cfg:  hnsw.UserConfig{RQ: hnsw.RQConfig{Enabled: true, Bits: 8}},
			want: []string{"rq-8"},
		},
		{
			name: "hnsw rq-1 enabled",
			cfg:  hnsw.UserConfig{RQ: hnsw.RQConfig{Enabled: true, Bits: 1}},
			want: []string{"rq-1"},
		},
		{
			name: "hnsw bq enabled",
			cfg:  hnsw.UserConfig{BQ: hnsw.BQConfig{Enabled: true}},
			want: []string{"bq"},
		},
		{
			name: "hnsw skipDefaultQuantization — none",
			cfg:  hnsw.UserConfig{SkipDefaultQuantization: true},
			want: []string{"none"},
		},
		{
			name: "flat no compression",
			cfg:  flat.UserConfig{},
			want: nil,
		},
		{
			name: "flat rq-8",
			cfg:  flat.UserConfig{RQ: flat.RQUserConfig{Enabled: true, Bits: 8}},
			want: []string{"rq-8"},
		},
		{
			name: "flat bq",
			cfg:  flat.UserConfig{BQ: flat.CompressionUserConfig{Enabled: true}},
			want: []string{"bq"},
		},
		{
			name: "dynamic with hnsw branch set",
			cfg: dynamic.UserConfig{
				HnswUC: hnsw.UserConfig{PQ: hnsw.PQConfig{Enabled: true}},
			},
			want: []string{"pq"},
		},
		{
			name: "dynamic with flat branch set",
			cfg: dynamic.UserConfig{
				FlatUC: flat.UserConfig{BQ: flat.CompressionUserConfig{Enabled: true}},
			},
			want: []string{"bq"},
		},
		{
			name: "dynamic with BOTH branches set — both surfaced",
			cfg: dynamic.UserConfig{
				HnswUC: hnsw.UserConfig{PQ: hnsw.PQConfig{Enabled: true}},
				FlatUC: flat.UserConfig{BQ: flat.CompressionUserConfig{Enabled: true}},
			},
			want: []string{"pq", "bq"},
		},
		{
			name: "dynamic with hnsw skipDefault + flat compression — both surfaced",
			cfg: dynamic.UserConfig{
				HnswUC: hnsw.UserConfig{SkipDefaultQuantization: true},
				FlatUC: flat.UserConfig{RQ: flat.RQUserConfig{Enabled: true, Bits: 8}},
			},
			want: []string{"none", "rq-8"},
		},
		{
			name: "dynamic with nothing — empty",
			cfg:  dynamic.UserConfig{},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compressionsFromIndexConfig(tt.cfg)
			assert.Equal(t, tt.want, got)
		})
	}
}

// --- validateAllowedCompression ---

func TestValidateAllowedCompression(t *testing.T) {
	tests := []struct {
		name      string
		allow     []string
		indexType string
		cfg       schemaConfig.VectorIndexConfig
		wantErr   bool
		wantValue string
	}{
		{
			name: "no allow-list — anything OK",
			cfg:  hnsw.UserConfig{PQ: hnsw.PQConfig{Enabled: true}},
		},
		{
			name:      "hfresh skipped even with allow-list",
			allow:     []string{"rq-8"},
			indexType: vectorindex.VectorIndexTypeHFresh,
			cfg:       hnsw.UserConfig{}, // shape doesn't matter; skip on type
		},
		{
			name:      "user pq in allow-list rq-8 — rejected",
			allow:     []string{"rq-8"},
			indexType: vectorindex.VectorIndexTypeHNSW,
			cfg:       hnsw.UserConfig{PQ: hnsw.PQConfig{Enabled: true}},
			wantErr:   true,
			wantValue: "pq",
		},
		{
			name:      "user rq-8 in allow-list rq-8 — OK",
			allow:     []string{"rq-8"},
			indexType: vectorindex.VectorIndexTypeHNSW,
			cfg:       hnsw.UserConfig{RQ: hnsw.RQConfig{Enabled: true, Bits: 8}},
		},
		{
			name:      "user no compression — defer (no error)",
			allow:     []string{"rq-8"},
			indexType: vectorindex.VectorIndexTypeHNSW,
			cfg:       hnsw.UserConfig{},
		},
		{
			name:      "user skipDefaultQuantization → none not in allow-list — rejected",
			allow:     []string{"rq-8"},
			indexType: vectorindex.VectorIndexTypeHNSW,
			cfg:       hnsw.UserConfig{SkipDefaultQuantization: true},
			wantErr:   true,
			wantValue: "none",
		},
		{
			name:      "user skipDefaultQuantization, none allowed — OK",
			allow:     []string{"rq-8", "none"},
			indexType: vectorindex.VectorIndexTypeHNSW,
			cfg:       hnsw.UserConfig{SkipDefaultQuantization: true},
		},
		{
			// Regression test for the Copilot review on the original
			// "first hit wins" implementation: with a dynamic config
			// that has an *allowed* compression on one branch and a
			// *disallowed* compression on the other branch, the
			// disallowed one must still cause rejection.
			name:      "dynamic with one allowed + one disallowed branch — rejected on disallowed",
			allow:     []string{"pq"},
			indexType: vectorindex.VectorIndexTypeDYNAMIC,
			cfg: dynamic.UserConfig{
				HnswUC: hnsw.UserConfig{PQ: hnsw.PQConfig{Enabled: true}},
				FlatUC: flat.UserConfig{BQ: flat.CompressionUserConfig{Enabled: true}},
			},
			wantErr:   true,
			wantValue: "bq",
		},
		{
			name:      "dynamic with both branches allowed — OK",
			allow:     []string{"pq", "bq"},
			indexType: vectorindex.VectorIndexTypeDYNAMIC,
			cfg: dynamic.UserConfig{
				HnswUC: hnsw.UserConfig{PQ: hnsw.PQConfig{Enabled: true}},
				FlatUC: flat.UserConfig{BQ: flat.CompressionUserConfig{Enabled: true}},
			},
		},
		{
			name:      "dynamic with skipDefault on one branch — checks 'none'",
			allow:     []string{"rq-8"},
			indexType: vectorindex.VectorIndexTypeDYNAMIC,
			cfg: dynamic.UserConfig{
				HnswUC: hnsw.UserConfig{SkipDefaultQuantization: true},
				FlatUC: flat.UserConfig{RQ: flat.RQUserConfig{Enabled: true, Bits: 8}},
			},
			wantErr:   true,
			wantValue: "none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, _ := newTestHandler(t, &fakeDB{})
			if tt.allow != nil {
				handler.config.Restrictions.AllowedCompressionTypes = runtime.NewDynamicValue(tt.allow)
			}
			err := handler.validateAllowedCompression(tt.indexType, tt.cfg)
			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			v, ok := restrictions.AsViolation(err)
			require.True(t, ok)
			assert.Equal(t, restrictions.RestrictionCompression, v.Restriction)
			assert.Equal(t, tt.wantValue, v.Value)
		})
	}
}
