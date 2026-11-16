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

	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
)

// The overlay reads each task's strategy, so a bare task struct is not enough.
func overlayTasks(strategies ...MigrationStrategy) []*ShardReindexTaskGeneric {
	tasks := make([]*ShardReindexTaskGeneric, len(strategies))
	for i, strategy := range strategies {
		tasks[i] = &ShardReindexTaskGeneric{strategy: strategy}
	}
	return tasks
}

// Every [IsSemanticMigration] member must have a row here saying what
// overlay it installs.
func TestPropertyOverlayFor_SemanticFamilyCoverage(t *testing.T) {
	tests := []struct {
		name         string
		migration    ReindexMigrationType
		strategies   []MigrationStrategy
		tokenization string
		wantOverlay  inverted.PropertyOverlay
	}{
		{
			name:         "change-tokenization moves the tokenization only",
			migration:    ReindexTypeChangeTokenization,
			strategies:   []MigrationStrategy{&SearchableRetokenizeStrategy{}},
			tokenization: "field",
			wantOverlay:  inverted.PropertyOverlay{Tokenization: "field"},
		},
		{
			name:       "change-tokenization without a target tokenization installs nothing",
			migration:  ReindexTypeChangeTokenization,
			strategies: []MigrationStrategy{&SearchableRetokenizeStrategy{}},
		},
		{
			name:         "change-tokenization-filterable does the same on the filterable bucket",
			migration:    ReindexTypeChangeTokenizationFilterable,
			strategies:   []MigrationStrategy{&FilterableRetokenizeStrategy{}},
			tokenization: "word",
			wantOverlay:  inverted.PropertyOverlay{Tokenization: "word"},
		},
		{
			name:        "enable-filterable forces the filterable flag",
			migration:   ReindexTypeEnableFilterable,
			strategies:  []MigrationStrategy{&EnableFilterableStrategy{}},
			wantOverlay: inverted.PropertyOverlay{ForceFilterable: true},
		},
		{
			name:        "enable-searchable forces the searchable flag and its tokenization",
			migration:   ReindexTypeEnableSearchable,
			strategies:  []MigrationStrategy{&EnableSearchableStrategy{tokenization: "field"}},
			wantOverlay: inverted.PropertyOverlay{ForceSearchable: true, Tokenization: "field"},
		},
		{
			name:        "enable-rangeable forces the rangeable flag",
			migration:   ReindexTypeEnableRangeable,
			strategies:  []MigrationStrategy{&FilterableToRangeableStrategy{}},
			wantOverlay: inverted.PropertyOverlay{ForceRangeable: true},
		},
		{
			name:       "map-to-blockmax is semantic but changes no analyzer input",
			migration:  ReindexTypeChangeAlgorithm,
			strategies: []MigrationStrategy{&MapToBlockmaxStrategy{}},
		},
	}

	covered := map[ReindexMigrationType]bool{}
	for _, tc := range tests {
		covered[tc.migration] = true
		t.Run(tc.name, func(t *testing.T) {
			payload := &ReindexTaskPayload{
				MigrationType:      tc.migration,
				TargetTokenization: tc.tokenization,
				Properties:         []string{"name"},
			}
			got := propertyOverlayFor(payload, overlayTasks(tc.strategies...))
			if tc.wantOverlay.Empty() {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, map[string]inverted.PropertyOverlay{"name": tc.wantOverlay}, got)
		})
	}

	for _, mt := range allReindexMigrationTypesForTest {
		if IsSemanticMigration(mt) {
			assert.Truef(t, covered[mt], "semantic migration %q has no row saying what overlay it installs", mt)
		}
	}
}
