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

func TestMigrationEffectStatus(t *testing.T) {
	tests := []struct {
		name        string
		mtype       ReindexMigrationType
		target      string
		properties  []string
		class       *models.Class
		want        migrationEffect
		wantMissing []string
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
			class:       &models.Class{Properties: []*models.Property{{Name: "title", Tokenization: models.PropertyTokenizationWord}}},
			want:        migrationEffectPending,
			wantMissing: []string{"title"},
		},
		{
			name: "change-tokenization: one of two properties has not flipped", mtype: ReindexTypeChangeTokenization,
			target: models.PropertyTokenizationLowercase, properties: []string{"title", "body"},
			class: &models.Class{Properties: []*models.Property{
				{Name: "title", Tokenization: models.PropertyTokenizationLowercase},
				{Name: "body", Tokenization: models.PropertyTokenizationWord},
			}},
			want:        migrationEffectPending,
			wantMissing: []string{"body"},
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
			class:       &models.Class{Properties: []*models.Property{{Name: "title"}}},
			want:        migrationEffectPending,
			wantMissing: []string{"title"},
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
			want:        migrationEffectPending,
			wantMissing: []string{"title"},
		},
		{
			name: "change-algorithm: the per-property stamp", mtype: ReindexTypeChangeAlgorithm, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title", SearchableBlockmax: boolPtr(true)}}},
			want:  migrationEffectVisible,
		},
		{
			// The class flag has a writer outside the migration and defaults to
			// true, so it cannot answer for an unstamped property.
			name: "change-algorithm: the class flag does not answer for an unstamped property", mtype: ReindexTypeChangeAlgorithm, properties: []string{"title"},
			class: &models.Class{
				InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true},
				Properties:          []*models.Property{{Name: "title"}},
			},
			want:        migrationEffectPending,
			wantMissing: []string{"title"},
		},
		{
			name: "enable-rangeable", mtype: ReindexTypeEnableRangeable, properties: []string{"price"},
			class: &models.Class{Properties: []*models.Property{{Name: "price", IndexRangeFilters: boolPtr(true)}}},
			want:  migrationEffectVisible,
		},
		{
			name: "enable-rangeable: flag not set", mtype: ReindexTypeEnableRangeable, properties: []string{"price"},
			class:       &models.Class{Properties: []*models.Property{{Name: "price"}}},
			want:        migrationEffectPending,
			wantMissing: []string{"price"},
		},
		{
			// IndexRangeFilters is what put the repair on the list, and the
			// repair writes no other flag, so reading it back proves nothing.
			name: "repair-rangeable reads its own entry condition, so no schema read settles it", mtype: ReindexTypeRepairRangeable, properties: []string{"price"},
			class: &models.Class{Properties: []*models.Property{{Name: "price", IndexRangeFilters: boolPtr(true)}}},
			want:  migrationEffectUnobservable,
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
			name: "the only property is not in the applied schema yet", mtype: ReindexTypeEnableFilterable, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "body", IndexFilterable: boolPtr(true)}}},
			want:  migrationEffectUnobservable,
		},
		{
			name: "one property not in the applied schema yet, the other not flipped", mtype: ReindexTypeEnableFilterable, properties: []string{"title", "body"},
			class: &models.Class{Properties: []*models.Property{{Name: "body"}}},
			want:  migrationEffectUnobservable,
		},
		{
			name: "change-algorithm: every property deleted, so the stamp is gone with them", mtype: ReindexTypeChangeAlgorithm, properties: []string{"title"},
			class: &models.Class{
				InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true},
			},
			want: migrationEffectUnobservable,
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
				TargetTokenization: tt.target,
			}
			for _, prop := range tt.properties {
				setMigrationDir(&subject, prop, func(*MigrationPropertyDirs) {})
			}
			effect, missing := migrationEffectStatus(tt.class, subject)
			require.Equal(t, tt.want, effect)
			require.Equal(t, tt.wantMissing, missing,
				"the properties a pending read blamed")
			require.Equal(t, tt.want == migrationEffectVisible,
				migrationEffectConfirmsCommit(tt.class, subject))
		})
	}
}
