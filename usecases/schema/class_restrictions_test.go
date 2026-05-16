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

// --- compressionFromIndexConfig ---

func TestCompressionFromIndexConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  schemaConfig.VectorIndexConfig
		want string
	}{
		{
			name: "hnsw no compression",
			cfg:  hnsw.UserConfig{},
			want: "",
		},
		{
			name: "hnsw pq enabled",
			cfg:  hnsw.UserConfig{PQ: hnsw.PQConfig{Enabled: true}},
			want: "pq",
		},
		{
			name: "hnsw sq enabled",
			cfg:  hnsw.UserConfig{SQ: hnsw.SQConfig{Enabled: true}},
			want: "sq",
		},
		{
			name: "hnsw rq-8 enabled",
			cfg:  hnsw.UserConfig{RQ: hnsw.RQConfig{Enabled: true, Bits: 8}},
			want: "rq-8",
		},
		{
			name: "hnsw rq-1 enabled",
			cfg:  hnsw.UserConfig{RQ: hnsw.RQConfig{Enabled: true, Bits: 1}},
			want: "rq-1",
		},
		{
			name: "hnsw bq enabled",
			cfg:  hnsw.UserConfig{BQ: hnsw.BQConfig{Enabled: true}},
			want: "bq",
		},
		{
			name: "hnsw skipDefaultQuantization — none",
			cfg:  hnsw.UserConfig{SkipDefaultQuantization: true},
			want: "none",
		},
		{
			name: "flat no compression",
			cfg:  flat.UserConfig{},
			want: "",
		},
		{
			name: "flat rq-8",
			cfg:  flat.UserConfig{RQ: flat.RQUserConfig{Enabled: true, Bits: 8}},
			want: "rq-8",
		},
		{
			name: "flat bq",
			cfg:  flat.UserConfig{BQ: flat.CompressionUserConfig{Enabled: true}},
			want: "bq",
		},
		{
			name: "dynamic delegates to hnsw sub-config",
			cfg: dynamic.UserConfig{
				HnswUC: hnsw.UserConfig{PQ: hnsw.PQConfig{Enabled: true}},
			},
			want: "pq",
		},
		{
			name: "dynamic delegates to flat when hnsw has nothing",
			cfg: dynamic.UserConfig{
				FlatUC: flat.UserConfig{BQ: flat.CompressionUserConfig{Enabled: true}},
			},
			want: "bq",
		},
		{
			name: "dynamic with nothing — empty",
			cfg:  dynamic.UserConfig{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compressionFromIndexConfig(tt.cfg)
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
