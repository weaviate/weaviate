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

// TestValidateVectorIndexType_AllowList composes the basic per-type
// check with the allow-list gate, matching production ordering.
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

			err := handler.validateVectorIndexTypeBasic(tt.indexType)
			if err == nil {
				err = handler.validateVectorIndexTypeAllowList(tt.indexType)
			}
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

// TestValidateVectorIndexType_RuntimeOverrideNormalization pins that
// the accessor normalizes at read time — a runtime YAML push of mixed-
// case or unknown entries (test injected here, bypassing the validator)
// must still match the canonical lowercase comparison.
func TestValidateVectorIndexType_RuntimeOverrideNormalization(t *testing.T) {
	handler, _ := newTestHandler(t, &fakeDB{})
	handler.config.HFreshEnabled = true
	dv := runtime.NewDynamicValue([]string{"HNSW", " FLAT ", "Bogus", "hnsw"})
	handler.config.Restrictions.AllowedVectorIndexTypes = dv

	require.NoError(t, handler.validateVectorIndexTypeBasic(vectorindex.VectorIndexTypeHNSW))
	require.NoError(t, handler.validateVectorIndexTypeAllowList(vectorindex.VectorIndexTypeHNSW))
	require.NoError(t, handler.validateVectorIndexTypeBasic(vectorindex.VectorIndexTypeFLAT))
	require.NoError(t, handler.validateVectorIndexTypeAllowList(vectorindex.VectorIndexTypeFLAT))

	require.NoError(t, handler.validateVectorIndexTypeBasic(vectorindex.VectorIndexTypeHFresh))
	err := handler.validateVectorIndexTypeAllowList(vectorindex.VectorIndexTypeHFresh)
	require.Error(t, err)
	v, ok := restrictions.AsViolation(err)
	require.True(t, ok)
	// Violation reflects what the accessor surfaces: lowercase, deduped,
	// unknown "bogus" filtered out.
	assert.Equal(t, []string{"hnsw", "flat"}, v.Allowed)
}

// TestValidateVectorSettingsAgainst_GrandfatherUnchanged pins that a
// PUT with unchanged fields passes after an allow-list tighten, while
// Add and field-changing PUT remain gated. See QA-#2 thread.
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

// TestValidateVectorSettingsAgainst_GrandfatherUnchanged_NamedVector
// mirrors the legacy grandfather matrix for entries inside
// class.VectorConfig — the named-vector loop in
// validateVectorSettingsAgainst runs through the same allow-list +
// grandfather logic on a per-entry basis. Includes the
// add-a-new-named-vector-on-PUT cases that the RAFT-side immutable
// check does NOT cover (it only iterates initial.VectorConfig), so the
// allow-list is the sole gate.
func TestValidateVectorSettingsAgainst_GrandfatherUnchanged_NamedVector(t *testing.T) {
	t.Run("update with unchanged disallowed named-vector type — passes (grandfathered)", func(t *testing.T) {
		handler := newHandlerWithAllowList(t, []string{"hfresh", "hnsw"}, nil)
		initial := classWithNamedVector(t, "v1", "flat", "")
		updated := classWithNamedVector(t, "v1", "flat", "")
		require.NoError(t, handler.validateVectorSettingsAgainst(updated, initial))
	})

	t.Run("update with unchanged disallowed named-vector compression — passes", func(t *testing.T) {
		handler := newHandlerWithAllowList(t, nil, []string{"rq-8"})
		initial := classWithNamedVector(t, "v1", "hnsw", "pq")
		updated := classWithNamedVector(t, "v1", "hnsw", "pq")
		require.NoError(t, handler.validateVectorSettingsAgainst(updated, initial))
	})

	t.Run("update changing named-vector compression to disallowed — rejected", func(t *testing.T) {
		handler := newHandlerWithAllowList(t, nil, []string{"rq-8"})
		initial := classWithNamedVector(t, "v1", "hnsw", "rq-8")
		updated := classWithNamedVector(t, "v1", "hnsw", "pq")
		err := handler.validateVectorSettingsAgainst(updated, initial)
		require.Error(t, err)
		v, ok := restrictions.AsViolation(err)
		require.True(t, ok)
		assert.Equal(t, "pq", v.Value)
	})

	t.Run("update adding new named-vector with disallowed type — rejected", func(t *testing.T) {
		// Allow-list is the sole gate here: the RAFT-side immutable check
		// (validateNamedVectorConfigsParityAndImmutables) iterates only
		// initial.VectorConfig, so brand-new entries on the updated side
		// pass that gate and reach allow-list unfiltered.
		handler := newHandlerWithAllowList(t, []string{"hfresh", "hnsw"}, nil)
		initial := classWithNamedVector(t, "v1", "hnsw", "rq-8")
		updated := classWithNamedVector(t, "v1", "hnsw", "rq-8")
		updated.VectorConfig["v2"] = models.VectorConfig{
			Vectorizer:      map[string]interface{}{"none": map[string]interface{}{}},
			VectorIndexType: "flat",
		}
		err := handler.validateVectorSettingsAgainst(updated, initial)
		require.Error(t, err)
		v, ok := restrictions.AsViolation(err)
		require.True(t, ok)
		assert.Equal(t, "flat", v.Value)
	})

	t.Run("update adding new named-vector with disallowed compression — rejected", func(t *testing.T) {
		handler := newHandlerWithAllowList(t, nil, []string{"rq-8"})
		initial := classWithNamedVector(t, "v1", "hnsw", "rq-8")
		updated := classWithNamedVector(t, "v1", "hnsw", "rq-8")
		updated.VectorConfig["v2"] = models.VectorConfig{
			Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
			VectorIndexType:   "hnsw",
			VectorIndexConfig: hnsw.UserConfig{PQ: hnsw.PQConfig{Enabled: true}},
		}
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

// classWithNamedVector builds a class with a single named-vector entry.
// compression == "" leaves VectorIndexConfig unset on the entry (no
// compression configured); otherwise it embeds an hnsw.UserConfig with
// the named compression enabled. Used by the named-vector grandfather
// matrix.
func classWithNamedVector(t *testing.T, name, indexType, compression string) *models.Class {
	t.Helper()
	var cfg interface{}
	if compression != "" {
		c := hnsw.UserConfig{}
		switch compression {
		case "pq":
			c.PQ.Enabled = true
		case "rq-8":
			c.RQ.Enabled = true
			c.RQ.Bits = 8
		default:
			t.Fatalf("unsupported test compression %q", compression)
		}
		cfg = c
	}
	return &models.Class{
		Class: "Demo",
		VectorConfig: map[string]models.VectorConfig{
			name: {
				Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
				VectorIndexType:   indexType,
				VectorIndexConfig: cfg,
			},
		},
	}
}

func TestValidateVectorIndexType_AllowListUsesOperatorTemplate(t *testing.T) {
	handler, _ := newTestHandler(t, &fakeDB{})
	handler.config.HFreshEnabled = true
	handler.config.Restrictions.AllowedVectorIndexTypes = runtime.NewDynamicValue([]string{"hfresh"})
	handler.config.Restrictions.ErrorMessage = runtime.NewDynamicValue(
		"Invalid config: {value} is not allowed for {restriction} (allowed: {allowed})",
	)

	require.NoError(t, handler.validateVectorIndexTypeBasic(vectorindex.VectorIndexTypeHNSW))
	err := handler.validateVectorIndexTypeAllowList(vectorindex.VectorIndexTypeHNSW)
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
			// Regression vs. "first hit wins": both Dynamic branches
			// must be checked, not just the first.
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
