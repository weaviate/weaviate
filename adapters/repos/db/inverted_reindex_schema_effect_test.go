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

// TestMigrationEffectSatisfied covers every migration type. The table is the
// gate the design asks for: a new type lands here with its own row.
func TestMigrationEffectSatisfied(t *testing.T) {
	tests := []struct {
		name       string
		mtype      ReindexMigrationType
		target     string
		properties []string
		class      *models.Class
		want       bool
	}{
		{
			name: "change-tokenization: the property carries the target tokenization", mtype: ReindexTypeChangeTokenization,
			target: models.PropertyTokenizationLowercase, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title", Tokenization: models.PropertyTokenizationLowercase}}},
			want:  true,
		},
		{
			name: "change-tokenization: the property still carries the old one", mtype: ReindexTypeChangeTokenization,
			target: models.PropertyTokenizationLowercase, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title", Tokenization: models.PropertyTokenizationWord}}},
			want:  false,
		},
		{
			name: "change-tokenization: one of two properties has not flipped", mtype: ReindexTypeChangeTokenization,
			target: models.PropertyTokenizationLowercase, properties: []string{"title", "body"},
			class: &models.Class{Properties: []*models.Property{
				{Name: "title", Tokenization: models.PropertyTokenizationLowercase},
				{Name: "body", Tokenization: models.PropertyTokenizationWord},
			}},
			want: false,
		},
		{
			name: "change-tokenization-filterable", mtype: ReindexTypeChangeTokenizationFilterable,
			target: models.PropertyTokenizationField, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title", Tokenization: models.PropertyTokenizationField}}},
			want:  true,
		},
		{
			name: "enable-filterable: flag on", mtype: ReindexTypeEnableFilterable, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title", IndexFilterable: boolPtr(true)}}},
			want:  true,
		},
		{
			name: "enable-filterable: flag unset reads as not yet committed", mtype: ReindexTypeEnableFilterable, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title"}}},
			want:  false,
		},
		{
			name: "enable-searchable: all three parts of the effect are visible", mtype: ReindexTypeEnableSearchable,
			target: models.PropertyTokenizationWord, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{
				Name: "title", IndexSearchable: boolPtr(true), SearchableBlockmax: boolPtr(true),
				Tokenization: models.PropertyTokenizationWord,
			}}},
			want: true,
		},
		{
			name: "enable-searchable: the blockmax stamp is missing", mtype: ReindexTypeEnableSearchable,
			target: models.PropertyTokenizationWord, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{
				Name: "title", IndexSearchable: boolPtr(true), Tokenization: models.PropertyTokenizationWord,
			}}},
			want: false,
		},
		{
			name: "change-algorithm: the per-property stamp", mtype: ReindexTypeChangeAlgorithm, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title", SearchableBlockmax: boolPtr(true)}}},
			want:  true,
		},
		{
			name: "change-algorithm: the class flag alone is enough", mtype: ReindexTypeChangeAlgorithm, properties: []string{"title"},
			class: &models.Class{
				InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: true},
				Properties:          []*models.Property{{Name: "title"}},
			},
			want: true,
		},
		{
			name: "change-algorithm: neither", mtype: ReindexTypeChangeAlgorithm, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title"}}},
			want:  false,
		},
		{
			name: "enable-rangeable", mtype: ReindexTypeEnableRangeable, properties: []string{"price"},
			class: &models.Class{Properties: []*models.Property{{Name: "price", IndexRangeFilters: boolPtr(true)}}},
			want:  true,
		},
		{
			name: "enable-rangeable: flag not set", mtype: ReindexTypeEnableRangeable, properties: []string{"price"},
			class: &models.Class{Properties: []*models.Property{{Name: "price"}}},
			want:  false,
		},
		{
			name: "repair-rangeable reads the same flag", mtype: ReindexTypeRepairRangeable, properties: []string{"price"},
			class: &models.Class{Properties: []*models.Property{{Name: "price", IndexRangeFilters: boolPtr(true)}}},
			want:  true,
		},
		{
			name: "repair-filterable is vacuous: post-condition equals pre-condition", mtype: ReindexTypeRepairFilterable, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title"}}},
			want:  true,
		},
		{
			name: "rebuild-searchable is vacuous too", mtype: ReindexTypeRebuildSearchable, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "title"}}},
			want:  true,
		},
		{
			name: "a property deleted after the migration counts as settled", mtype: ReindexTypeEnableFilterable, properties: []string{"title"},
			class: &models.Class{Properties: []*models.Property{{Name: "body", IndexFilterable: boolPtr(true)}}},
			want:  true,
		},
		{
			name: "one property deleted, the other still not flipped", mtype: ReindexTypeEnableFilterable, properties: []string{"title", "body"},
			class: &models.Class{Properties: []*models.Property{{Name: "body"}}},
			want:  false,
		},
		{
			name: "no properties and a type whose effect is per property: nothing to read", mtype: ReindexTypeEnableFilterable,
			class: &models.Class{}, want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject := MigrationSubject{
				MigrationType:      tt.mtype,
				Properties:         tt.properties,
				TargetTokenization: tt.target,
			}
			require.Equal(t, tt.want, migrationEffectSatisfied(tt.class, subject))
		})
	}
}
