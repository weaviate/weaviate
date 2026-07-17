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

package rest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/models"
)

// boolPtr is defined in handlers_indexes_gaps_test.go (same package).

// TestNormalizeIndexTypeParam pins the `rangeable` → `rangeFilters` alias
// mapping and out-of-enum rejection.
func TestNormalizeIndexTypeParam(t *testing.T) {
	cases := []struct {
		in      string
		wantTok string
		wantOK  bool
	}{
		{"filterable", "filterable", true},
		{"searchable", "searchable", true},
		{"rangeFilters", "rangeable", true},
		{"rangeable", "rangeable", true},
		{"bogus", "", false},
		{"", "", false},
		{"RangeFilters", "", false}, // case-sensitive; swagger enum is the gate
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			tok, ok := normalizeIndexTypeParam(tc.in)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantTok, tok)
		})
	}
}

// TestCanonicalIndexType pins that "rangeable" always renders as
// "rangeFilters" in responses.
func TestCanonicalIndexType(t *testing.T) {
	assert.Equal(t, "rangeFilters", canonicalIndexType("rangeable"))
	assert.Equal(t, "filterable", canonicalIndexType("filterable"))
	assert.Equal(t, "searchable", canonicalIndexType("searchable"))
}

// textProp / numProp / build a class for the resolver tests.
func textProp(name, tok string, searchable, filterable *bool) *models.Property {
	return &models.Property{
		Name: name, DataType: []string{"text"}, Tokenization: tok,
		IndexSearchable: searchable, IndexFilterable: filterable,
	}
}

func classWith(blockmax bool, props ...*models.Property) *models.Class {
	return &models.Class{
		Properties:          props,
		InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: blockmax},
	}
}

// TestResolveUpsertPlan_Searchable covers searchable outcome rows that need
// no DB (create / NO_OP / algorithm / one-change).
func TestResolveUpsertPlan_Searchable(t *testing.T) {
	h := &indexesHandlers{}
	cases := []struct {
		name     string
		prop     *models.Property
		blockmax bool
		body     *models.IndexUpsertRequest
		wantNoop bool
		wantMT   db.ReindexMigrationType
		wantErr  string
	}{
		{
			name:   "absent + tokenization -> create",
			prop:   textProp("t", "word", boolPtr(false), boolPtr(false)),
			body:   &models.IndexUpsertRequest{Tokenization: "whitespace"},
			wantMT: db.ReindexTypeEnableSearchable,
		},
		{
			name:    "absent + empty body -> 400 tokenization required",
			prop:    textProp("t", "word", boolPtr(false), boolPtr(false)),
			body:    &models.IndexUpsertRequest{},
			wantErr: "tokenization",
		},
		{
			name:     "present + identical tokenization -> NO_OP",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:     &models.IndexUpsertRequest{Tokenization: "word"},
			wantNoop: true,
		},
		{
			name:     "present + empty body -> NO_OP",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:     &models.IndexUpsertRequest{},
			wantNoop: true,
		},
		{
			name:     "on wand + algorithm blockmax -> change-algorithm",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(false)),
			blockmax: false,
			body:     &models.IndexUpsertRequest{Algorithm: "blockmax"},
			wantMT:   db.ReindexTypeChangeAlgorithm,
		},
		{
			name:     "on wand + algorithm alias bmw -> change-algorithm",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(false)),
			blockmax: false,
			body:     &models.IndexUpsertRequest{Algorithm: "bmw"},
			wantMT:   db.ReindexTypeChangeAlgorithm,
		},
		{
			name:     "already blockmax + algorithm blockmax -> NO_OP",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(false)),
			blockmax: true,
			body:     &models.IndexUpsertRequest{Algorithm: "blockmax"},
			wantNoop: true,
		},
		{
			name:    "algorithm wand -> 400 deprecated",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:    &models.IndexUpsertRequest{Algorithm: "wand"},
			wantErr: "deprecated",
		},
		{
			name:    "unknown algorithm -> 400",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:    &models.IndexUpsertRequest{Algorithm: "sparta"},
			wantErr: "unsupported algorithm",
		},
		{
			name:    "tokenization + algorithm both -> 400 one change",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:    &models.IndexUpsertRequest{Tokenization: "whitespace", Algorithm: "blockmax"},
			wantErr: "at most one",
		},
		{
			name:    "absent searchable + algorithm -> 400 no searchable index",
			prop:    textProp("t", "word", boolPtr(false), boolPtr(false)),
			body:    &models.IndexUpsertRequest{Algorithm: "blockmax"},
			wantErr: "searchable",
		},
		{
			name:    "non-text property -> 400 not a text type",
			prop:    &models.Property{Name: "n", DataType: []string{"int"}, IndexSearchable: boolPtr(false)},
			body:    &models.IndexUpsertRequest{Tokenization: "word"},
			wantErr: "not a text type",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(tc.blockmax, tc.prop)
			plan, err := h.resolveUpsertPlan(class, "C", tc.prop, "searchable", tc.body)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantNoop, plan.noop)
			assert.Equal(t, tc.wantMT, plan.migrationType)
		})
	}
}

// TestResolveUpsertPlan_Filterable covers the filterable outcome-matrix rows.
func TestResolveUpsertPlan_Filterable(t *testing.T) {
	h := &indexesHandlers{}
	cases := []struct {
		name     string
		prop     *models.Property
		body     *models.IndexUpsertRequest
		wantNoop bool
		wantMT   db.ReindexMigrationType
		wantErr  string
	}{
		{
			name:   "absent + empty body -> create with current tokenization",
			prop:   textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:   &models.IndexUpsertRequest{},
			wantMT: db.ReindexTypeEnableFilterable,
		},
		{
			name:   "absent + matching tokenization -> create",
			prop:   textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:   &models.IndexUpsertRequest{Tokenization: "word"},
			wantMT: db.ReindexTypeEnableFilterable,
		},
		{
			name:    "absent + divergent tokenization -> 400",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(false)),
			body:    &models.IndexUpsertRequest{Tokenization: "lowercase"},
			wantErr: "must match",
		},
		{
			name:     "present + empty body -> NO_OP",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(true)),
			body:     &models.IndexUpsertRequest{},
			wantNoop: true,
		},
		{
			name:     "present + identical tokenization -> NO_OP",
			prop:     textProp("t", "word", boolPtr(true), boolPtr(true)),
			body:     &models.IndexUpsertRequest{Tokenization: "word"},
			wantNoop: true,
		},
		{
			name:   "present + different tokenization -> filterable retokenize",
			prop:   textProp("t", "word", boolPtr(true), boolPtr(true)),
			body:   &models.IndexUpsertRequest{Tokenization: "lowercase"},
			wantMT: db.ReindexTypeChangeTokenizationFilterable,
		},
		{
			name:    "algorithm on filterable -> 400",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(true)),
			body:    &models.IndexUpsertRequest{Algorithm: "blockmax"},
			wantErr: "only valid for a searchable",
		},
		{
			name:    "blob property -> 400 unsupported",
			prop:    &models.Property{Name: "b", DataType: []string{"blob"}, IndexFilterable: boolPtr(false)},
			body:    &models.IndexUpsertRequest{},
			wantErr: "does not support",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(false, tc.prop)
			plan, err := h.resolveUpsertPlan(class, "C", tc.prop, "filterable", tc.body)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantNoop, plan.noop)
			assert.Equal(t, tc.wantMT, plan.migrationType)
		})
	}
}

// TestResolveUpsertPlan_RangeFilters covers rangeFilters (internal token
// "rangeable"): no config fields, create-or-NO_OP, numeric-only.
func TestResolveUpsertPlan_RangeFilters(t *testing.T) {
	h := &indexesHandlers{}
	numProp := func(rf *bool) *models.Property {
		return &models.Property{Name: "n", DataType: []string{"int"}, IndexRangeFilters: rf}
	}
	cases := []struct {
		name     string
		prop     *models.Property
		body     *models.IndexUpsertRequest
		wantNoop bool
		wantMT   db.ReindexMigrationType
		wantErr  string
	}{
		{
			name:   "absent + empty body -> create",
			prop:   numProp(boolPtr(false)),
			body:   &models.IndexUpsertRequest{},
			wantMT: db.ReindexTypeEnableRangeable,
		},
		{
			name:     "present + empty body -> NO_OP",
			prop:     numProp(boolPtr(true)),
			body:     &models.IndexUpsertRequest{},
			wantNoop: true,
		},
		{
			name:    "tokenization field -> 400 no config",
			prop:    numProp(boolPtr(false)),
			body:    &models.IndexUpsertRequest{Tokenization: "word"},
			wantErr: "no configuration fields",
		},
		{
			name:    "non-numeric property -> 400",
			prop:    textProp("t", "word", boolPtr(true), boolPtr(true)),
			body:    &models.IndexUpsertRequest{},
			wantErr: "numeric",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(false, tc.prop)
			plan, err := h.resolveUpsertPlan(class, "C", tc.prop, "rangeable", tc.body)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantNoop, plan.noop)
			assert.Equal(t, tc.wantMT, plan.migrationType)
		})
	}
}

// TestResolveRebuildPlan covers the rebuild preconditions per index type.
func TestResolveRebuildPlan(t *testing.T) {
	cases := []struct {
		name      string
		indexType string
		prop      *models.Property
		blockmax  bool
		wantMT    db.ReindexMigrationType
		wantErr   string
	}{
		{
			name:      "searchable on blockmax -> rebuild-searchable",
			indexType: "searchable",
			prop:      textProp("t", "word", boolPtr(true), boolPtr(false)),
			blockmax:  true,
			wantMT:    db.ReindexTypeRebuildSearchable,
		},
		{
			name:      "searchable on wand -> 400 blockmax first",
			indexType: "searchable",
			prop:      textProp("t", "word", boolPtr(true), boolPtr(false)),
			blockmax:  false,
			wantErr:   "WAND",
		},
		{
			name:      "searchable absent -> 400",
			indexType: "searchable",
			prop:      textProp("t", "word", boolPtr(false), boolPtr(false)),
			blockmax:  true,
			wantErr:   "searchable",
		},
		{
			name:      "filterable present -> repair-filterable",
			indexType: "filterable",
			prop:      textProp("t", "word", boolPtr(true), boolPtr(true)),
			wantMT:    db.ReindexTypeRepairFilterable,
		},
		{
			name:      "filterable absent -> 400",
			indexType: "filterable",
			prop:      textProp("t", "word", boolPtr(true), boolPtr(false)),
			wantErr:   "does not have a filterable index",
		},
		{
			name:      "rangeFilters present -> repair-rangeable",
			indexType: "rangeable",
			prop:      &models.Property{Name: "n", DataType: []string{"int"}, IndexRangeFilters: boolPtr(true)},
			wantMT:    db.ReindexTypeRepairRangeable,
		},
		{
			name:      "rangeFilters absent -> 400",
			indexType: "rangeable",
			prop:      &models.Property{Name: "n", DataType: []string{"int"}, IndexRangeFilters: boolPtr(false)},
			wantErr:   "does not have a rangeable index",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			class := classWith(tc.blockmax, tc.prop)
			// nil appState → searchablePropertyIsBlockmax falls back to the
			// class flag, so these class-granular cases behave as before.
			h := &indexesHandlers{}
			plan, err := h.resolveRebuildPlan(class, tc.prop, tc.indexType)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.False(t, plan.noop)
			assert.Equal(t, tc.wantMT, plan.migrationType)
		})
	}
}
