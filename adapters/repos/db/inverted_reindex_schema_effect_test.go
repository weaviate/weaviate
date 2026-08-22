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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// TestMigrationEffectStatus covers every migration type. The table is the
// gate the design asks for: a new type lands here with its own row.
//
// It asserts the commit predicate alongside the answer, because the split
// between them is what keeps a migration nothing can confirm from being
// committed: only a visible effect is positive evidence.
func TestMigrationEffectStatus(t *testing.T) {
	tests := []struct {
		name       string
		mtype      ReindexMigrationType
		target     string
		properties []string
		class      *models.Class
		want       migrationEffect
	}{
		{
			name: "change-tokenization: the property carries the target tokenization", mtype: ReindexTypeChangeTokenization,
			target: models.PropertyTokenizationLowercase, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title", Tokenization: models.PropertyTokenizationLowercase}}},
			want:  migrationEffectVisible,
		},
		{
			name: "change-tokenization: the property still carries the old one", mtype: ReindexTypeChangeTokenization,
			target: models.PropertyTokenizationLowercase, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title", Tokenization: models.PropertyTokenizationWord}}},
			want:  migrationEffectPending,
		},
		{
			name: "change-tokenization: one of two properties has not flipped", mtype: ReindexTypeChangeTokenization,
			target: models.PropertyTokenizationLowercase, properties: []string{"title", "body"},
			class: &models.Class{Properties: []*models.Property{
				{Name: "title", Tokenization: models.PropertyTokenizationLowercase},
				{Name: "body", Tokenization: models.PropertyTokenizationWord},
			}},
			want: migrationEffectPending,
		},
		{
			name: "change-tokenization-filterable", mtype: ReindexTypeChangeTokenizationFilterable,
			target: models.PropertyTokenizationField, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title", Tokenization: models.PropertyTokenizationField}}},
			want:  migrationEffectVisible,
		},
		{
			name: "enable-filterable: flag on", mtype: ReindexTypeEnableFilterable, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title", IndexFilterable: boolPtr(true)}}},
			want:  migrationEffectVisible,
		},
		{
			name: "enable-filterable: flag unset reads as not yet committed", mtype: ReindexTypeEnableFilterable, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title"}}},
			want:  migrationEffectPending,
		},
		{
			name: "enable-searchable: all three parts of the effect are visible", mtype: ReindexTypeEnableSearchable,
			target: models.PropertyTokenizationWord, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{
				Name: "title", IndexSearchable: boolPtr(true), SearchableBlockmax: boolPtr(true),
				Tokenization: models.PropertyTokenizationWord,
			}}},
			want: migrationEffectVisible,
		},
		{
			name: "enable-searchable: the blockmax stamp is missing", mtype: ReindexTypeEnableSearchable,
			target: models.PropertyTokenizationWord, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{
				Name: "title", IndexSearchable: boolPtr(true), Tokenization: models.PropertyTokenizationWord,
			}}},
			want: migrationEffectPending,
		},
		{
			name: "change-algorithm: the per-property stamp", mtype: ReindexTypeChangeAlgorithm, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title", SearchableBlockmax: boolPtr(true)}}},
			want:  migrationEffectVisible,
		},
		{
			name: "change-algorithm: the class flag alone is enough", mtype: ReindexTypeChangeAlgorithm, properties: []string{"title"},
			class: &models.Class{
				InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true},
				Properties:          []*models.Property{{Name: "title"}},
			},
			want: migrationEffectVisible,
		},
		{
			name: "change-algorithm: neither", mtype: ReindexTypeChangeAlgorithm, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title"}}},
			want:  migrationEffectPending,
		},
		{
			name: "enable-rangeable", mtype: ReindexTypeEnableRangeable, properties: []string{"price"},
			class: &models.Class{Properties: []*models.Property{{Name: "price", IndexRangeFilters: boolPtr(true)}}},
			want:  migrationEffectVisible,
		},
		{
			name: "enable-rangeable: flag not set", mtype: ReindexTypeEnableRangeable, properties: []string{"price"},
			class: &models.Class{Properties: []*models.Property{{Name: "price"}}},
			want:  migrationEffectPending,
		},
		{
			name: "repair-rangeable reads the same flag", mtype: ReindexTypeRepairRangeable, properties: []string{"price"},
			class: &models.Class{Properties: []*models.Property{{Name: "price", IndexRangeFilters: boolPtr(true)}}},
			want:  migrationEffectVisible,
		},
		{
			name: "repair-filterable has no flag anywhere, so no schema read settles it", mtype: ReindexTypeRepairFilterable, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title"}}},
			want:  migrationEffectUnobservable,
		},
		{
			name: "rebuild-searchable has none either", mtype: ReindexTypeRebuildSearchable, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title"}}},
			want:  migrationEffectUnobservable,
		},
		{
			name: "the only property was deleted, so its flag went with it", mtype: ReindexTypeEnableFilterable, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "body", IndexFilterable: boolPtr(true)}}},
			want:  migrationEffectUnobservable,
		},
		{
			name: "one property deleted, the other still not flipped", mtype: ReindexTypeEnableFilterable, properties: []string{"title", "body"},
			class: &models.Class{Properties: []*models.Property{{Name: "body"}}},
			want:  migrationEffectPending,
		},
		{
			name: "one property deleted, the other flipped: the survivor is the evidence", mtype: ReindexTypeEnableFilterable, properties: []string{"title", "body"},
			class: &models.Class{Properties: []*models.Property{{Name: "body", IndexFilterable: boolPtr(true)}}},
			want:  migrationEffectVisible,
		},
		{
			name: "change-algorithm: every property deleted but the class flag still stands", mtype: ReindexTypeChangeAlgorithm, properties: []string{"title"},
			class: &models.Class{
				InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true},
			},
			want: migrationEffectVisible,
		},
		{
			name: "change-algorithm: every property deleted and no class flag", mtype: ReindexTypeChangeAlgorithm, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "body"}}},
			want:  migrationEffectUnobservable,
		},
		{
			name: "no properties and a type whose effect is per property: nothing to read", mtype: ReindexTypeEnableFilterable,
			class: &models.Class{}, want: migrationEffectPending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject := MigrationSubject{
				MigrationType:      tt.mtype,
				Properties:         tt.properties,
				TargetTokenization: tt.target,
			}
			effect, missing := migrationEffectStatus(tt.class, subject)
			require.Equal(t, tt.want, effect)
			if tt.want != migrationEffectPending {
				require.Empty(t, missing, "only a pending effect names properties")
			}
			// The split: an effect the schema cannot show is not evidence a
			// task committed, even though it is enough to let the closure
			// sweep retire the record.
			require.Equal(t, tt.want == migrationEffectVisible,
				migrationEffectConfirmsCommit(tt.class, subject))
		})
	}
}
