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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/models"
)

// TestAnalyzerOverlayShape pins each MigrationStrategy.AnalyzerOverlay
// return shape. The overlay is what keeps from-scratch strategies
// (enable-filterable / -searchable / -rangeable) from backfilling
// empty buckets while the schema flag is still false; a wrong
// Tokenization or Force* flag here surfaces as a silent-data-loss
// FINISHED migration. weaviate/0-weaviate-issues#243 gap 1.
func TestAnalyzerOverlayShape(t *testing.T) {
	const (
		analyzerOverlayPropA   = "title"
		analyzerOverlayPropB   = "body"
		analyzerOverlayMissing = "not_targeted"
	)

	type analyzerOverlayCase struct {
		name           string
		makeOverlay    func(props []string) map[string]inverted.PropertyOverlay
		props          []string
		wantNil        bool
		wantPropKeys   []string
		wantPerKey     inverted.PropertyOverlay
		wantTokenAware bool // when true, also assert Tokenization on each entry
	}

	cases := []analyzerOverlayCase{
		// EnableSearchable: forces searchable=true AND the target tokenization.
		// A regression to Tokenization:"" here would silently build a
		// searchable bucket whose posting lists key off the WRONG tokens
		// (stored tokenization, which on a schema-disabled prop may be
		// the zero value).
		{
			name: "EnableSearchable_twoProps_forcesSearchableAndTokenization",
			makeOverlay: (&EnableSearchableStrategy{
				tokenization: models.PropertyTokenizationField,
			}).AnalyzerOverlay,
			props:        []string{analyzerOverlayPropA, analyzerOverlayPropB},
			wantPropKeys: []string{analyzerOverlayPropA, analyzerOverlayPropB},
			wantPerKey: inverted.PropertyOverlay{
				ForceSearchable: true,
				Tokenization:    models.PropertyTokenizationField,
			},
			wantTokenAware: true,
		},
		{
			name: "EnableSearchable_wordTokenization",
			makeOverlay: (&EnableSearchableStrategy{
				tokenization: models.PropertyTokenizationWord,
			}).AnalyzerOverlay,
			props:        []string{analyzerOverlayPropA},
			wantPropKeys: []string{analyzerOverlayPropA},
			wantPerKey: inverted.PropertyOverlay{
				ForceSearchable: true,
				Tokenization:    models.PropertyTokenizationWord,
			},
			wantTokenAware: true,
		},
		{
			name: "EnableSearchable_emptyProps_returnsNil",
			makeOverlay: (&EnableSearchableStrategy{
				tokenization: models.PropertyTokenizationField,
			}).AnalyzerOverlay,
			props:   []string{},
			wantNil: true,
		},

		// EnableFilterable: forces filterable=true. No tokenization (the
		// filterable bucket is RoaringSet on the raw analyzer items, not
		// a tokenization-shifted rebuild).
		{
			name:         "EnableFilterable_twoProps_forcesFilterable",
			makeOverlay:  (&EnableFilterableStrategy{}).AnalyzerOverlay,
			props:        []string{analyzerOverlayPropA, analyzerOverlayPropB},
			wantPropKeys: []string{analyzerOverlayPropA, analyzerOverlayPropB},
			wantPerKey: inverted.PropertyOverlay{
				ForceFilterable: true,
			},
		},
		{
			name:        "EnableFilterable_emptyProps_returnsNil",
			makeOverlay: (&EnableFilterableStrategy{}).AnalyzerOverlay,
			props:       []string{},
			wantNil:     true,
		},

		// FilterableToRangeable: forces rangeable=true. Same rationale as
		// EnableFilterable — the rangeable bucket is built from raw values.
		{
			name:         "FilterableToRangeable_twoProps_forcesRangeable",
			makeOverlay:  (&FilterableToRangeableStrategy{}).AnalyzerOverlay,
			props:        []string{analyzerOverlayPropA, analyzerOverlayPropB},
			wantPropKeys: []string{analyzerOverlayPropA, analyzerOverlayPropB},
			wantPerKey: inverted.PropertyOverlay{
				ForceRangeable: true,
			},
		},
		{
			name:        "FilterableToRangeable_emptyProps_returnsNil",
			makeOverlay: (&FilterableToRangeableStrategy{}).AnalyzerOverlay,
			props:       []string{},
			wantNil:     true,
		},

		// RebuildSearchable: explicitly returns nil. A regression that
		// returned a non-nil overlay (e.g. with Tokenization:"") would
		// silently REWRITE the existing searchable bucket with the wrong
		// tokenization — turning a "rebuild" verb into an unintended
		// retokenization. Pin nil-return.
		{
			name:        "RebuildSearchable_alwaysNil",
			makeOverlay: (&RebuildSearchableStrategy{}).AnalyzerOverlay,
			props:       []string{analyzerOverlayPropA, analyzerOverlayPropB},
			wantNil:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.makeOverlay(tc.props)

			if tc.wantNil {
				assert.Nil(t, got, "expected nil overlay for %s (props=%v)", tc.name, tc.props)
				return
			}

			require.NotNil(t, got, "overlay must not be nil for %s", tc.name)
			require.Len(t, got, len(tc.wantPropKeys),
				"unexpected number of overlay entries for %s: got=%v", tc.name, got)

			for _, want := range tc.wantPropKeys {
				entry, ok := got[want]
				require.True(t, ok, "missing overlay entry for prop %q in %s", want, tc.name)
				assert.Equal(t, tc.wantPerKey.ForceFilterable, entry.ForceFilterable,
					"ForceFilterable mismatch for prop %q in %s", want, tc.name)
				assert.Equal(t, tc.wantPerKey.ForceSearchable, entry.ForceSearchable,
					"ForceSearchable mismatch for prop %q in %s", want, tc.name)
				assert.Equal(t, tc.wantPerKey.ForceRangeable, entry.ForceRangeable,
					"ForceRangeable mismatch for prop %q in %s", want, tc.name)
				if tc.wantTokenAware {
					assert.Equal(t, tc.wantPerKey.Tokenization, entry.Tokenization,
						"Tokenization mismatch for prop %q in %s "+
							"(empty tokenization on a searchable bucket = silent data loss)",
						want, tc.name)
				} else {
					// Strategies that don't force tokenization MUST leave it
					// empty so the analyzer falls back to the stored value.
					assert.Empty(t, entry.Tokenization,
						"Tokenization should be empty for prop %q in %s "+
							"(non-token-aware strategy leaking a tokenization is a bug)",
						want, tc.name)
				}
			}

			// Selection-set guard: untargeted prop names MUST NOT appear.
			_, leaked := got[analyzerOverlayMissing]
			assert.False(t, leaked,
				"overlay leaked an un-requested prop %q in %s", analyzerOverlayMissing, tc.name)
		})
	}
}

// TestAnalyzerOverlayShape_NoAnalyzerOverlayDefault pins the embedded
// `noAnalyzerOverlay` default returned by every strategy that doesn't
// shadow it (MapToBlockmax, RoaringSetRefresh, SearchableRetokenize,
// FilterableRetokenize). A regression to a non-nil return here would
// silently apply an overlay where none is intended — most dangerous on
// retokenize, where it could double-shift the tokenization and rebuild
// the wrong terms.
func TestAnalyzerOverlayShape_NoAnalyzerOverlayDefault(t *testing.T) {
	analyzerOverlayDefaults := []struct {
		name string
		// Use the interface so the test covers the actual dispatch path
		// (embedded method via the strategy struct), not the bare
		// noAnalyzerOverlay value.
		strategy MigrationStrategy
	}{
		{
			name:     "MapToBlockmax_noOverlay",
			strategy: &MapToBlockmaxStrategy{},
		},
		{
			name:     "RoaringSetRefresh_noOverlay",
			strategy: &RoaringSetRefreshStrategy{},
		},
		{
			name:     "SearchableRetokenize_noOverlay",
			strategy: &SearchableRetokenizeStrategy{propName: "title"},
		},
		{
			name:     "FilterableRetokenize_noOverlay",
			strategy: &FilterableRetokenizeStrategy{propName: "title"},
		},
	}

	for _, tc := range analyzerOverlayDefaults {
		t.Run(tc.name, func(t *testing.T) {
			// Multi-prop, single-prop, and empty all return nil.
			assert.Nil(t, tc.strategy.AnalyzerOverlay(nil),
				"%s: AnalyzerOverlay(nil) must be nil", tc.name)
			assert.Nil(t, tc.strategy.AnalyzerOverlay([]string{}),
				"%s: AnalyzerOverlay(empty) must be nil", tc.name)
			assert.Nil(t, tc.strategy.AnalyzerOverlay([]string{"title", "body"}),
				"%s: AnalyzerOverlay(props) must be nil "+
					"(non-nil here would silently shift tokenization in a same-schema rebuild)",
				tc.name)
		})
	}
}

// TestAnalyzerOverlayShape_BareNoAnalyzerOverlay covers the embedded type
// directly so a refactor that replaces the embed with a different default
// implementation still fails this test loudly.
func TestAnalyzerOverlayShape_BareNoAnalyzerOverlay(t *testing.T) {
	var analyzerOverlayBase noAnalyzerOverlay
	assert.Nil(t, analyzerOverlayBase.AnalyzerOverlay(nil))
	assert.Nil(t, analyzerOverlayBase.AnalyzerOverlay([]string{}))
	assert.Nil(t, analyzerOverlayBase.AnalyzerOverlay([]string{"any", "props"}))
}
