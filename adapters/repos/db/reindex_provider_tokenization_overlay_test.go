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
)

// Unit coverage for the #216 Gap B overlay set/clear lifecycle without a
// full provider+DB+index. Key invariant: the overlay is set only when
// the per-prop hook fires, never eagerly at wiring time.

// The wiring reads each task's strategy, so a bare task struct is not enough.
func overlayTasks(strategies ...MigrationStrategy) []*ShardReindexTaskGeneric {
	tasks := make([]*ShardReindexTaskGeneric, len(strategies))
	for i, strategy := range strategies {
		if strategy == nil {
			continue
		}
		tasks[i] = &ShardReindexTaskGeneric{strategy: strategy}
	}
	return tasks
}

// fireAllPropHooks simulates a swap loop where every prop flipped.
func fireAllPropHooks(tasks []*ShardReindexTaskGeneric, props []string) int {
	fired := 0
	for _, task := range tasks {
		if task == nil || task.onPropSwapped == nil {
			continue
		}
		for _, propName := range props {
			task.onPropSwapped(propName)
			fired++
		}
	}
	return fired
}

// Every [IsSemanticMigration] member must have a row here saying what
// overlay it installs.
func TestMaybeWirePerPropOverlaySet_SemanticFamilyCoverage(t *testing.T) {
	tests := []struct {
		name         string
		migration    ReindexMigrationType
		strategy     MigrationStrategy
		tokenization string
		wantWired    bool
		wantOverlay  inverted.PropertyOverlay
	}{
		{
			name:         "change-tokenization moves the tokenization only",
			migration:    ReindexTypeChangeTokenization,
			strategy:     &SearchableRetokenizeStrategy{},
			tokenization: "field",
			wantWired:    true,
			wantOverlay:  inverted.PropertyOverlay{Tokenization: "field"},
		},
		{
			name:         "change-tokenization-filterable does the same on the filterable bucket",
			migration:    ReindexTypeChangeTokenizationFilterable,
			strategy:     &FilterableRetokenizeStrategy{},
			tokenization: "word",
			wantWired:    true,
			wantOverlay:  inverted.PropertyOverlay{Tokenization: "word"},
		},
		{
			name:        "enable-filterable forces the filterable flag",
			migration:   ReindexTypeEnableFilterable,
			strategy:    &EnableFilterableStrategy{},
			wantWired:   true,
			wantOverlay: inverted.PropertyOverlay{ForceFilterable: true},
		},
		{
			name:        "enable-searchable forces the searchable flag and its tokenization",
			migration:   ReindexTypeEnableSearchable,
			strategy:    &EnableSearchableStrategy{tokenization: "field"},
			wantWired:   true,
			wantOverlay: inverted.PropertyOverlay{ForceSearchable: true, Tokenization: "field"},
		},
		{
			name:        "enable-rangeable forces the rangeable flag",
			migration:   ReindexTypeEnableRangeable,
			strategy:    &FilterableToRangeableStrategy{},
			wantWired:   true,
			wantOverlay: inverted.PropertyOverlay{ForceRangeable: true},
		},
		{
			name:      "map-to-blockmax is semantic but changes no analyzer input",
			migration: ReindexTypeChangeAlgorithm,
			strategy:  &MapToBlockmaxStrategy{},
			wantWired: false,
		},
	}

	covered := map[ReindexMigrationType]bool{}
	for _, tc := range tests {
		covered[tc.migration] = true
		t.Run(tc.name, func(t *testing.T) {
			s := &Shard{}
			tasks := overlayTasks(tc.strategy)
			payload := &ReindexTaskPayload{
				MigrationType:      tc.migration,
				TargetTokenization: tc.tokenization,
				Properties:         []string{"name"},
			}
			maybeWirePerPropOverlaySet(s, payload, tasks)
			if !tc.wantWired {
				assert.Nil(t, tasks[0].onPropSwapped)
				assert.Nil(t, s.SnapshotPropertyOverlay([]string{"name"}))
				return
			}

			assert.Nil(t, s.SnapshotPropertyOverlay([]string{"name"}),
				"wiring must not pre-set the overlay; that's the bug being fixed")
			fireAllPropHooks(tasks, payload.Properties)
			assert.Equal(t, tc.wantOverlay, s.SnapshotPropertyOverlay([]string{"name"})["name"])
		})
	}

	for _, mt := range allReindexMigrationTypesForTest {
		if IsSemanticMigration(mt) {
			assert.Truef(t, covered[mt], "semantic migration %q has no row saying what overlay it installs", mt)
		}
	}
}

func TestMaybeWirePerPropOverlaySet_EmptyTargetTokenization_NoOp(t *testing.T) {
	s := &Shard{}
	tasks := overlayTasks(&SearchableRetokenizeStrategy{})
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "", // payload missing target
		Properties:         []string{"name"},
	}
	maybeWirePerPropOverlaySet(s, payload, tasks)
	assert.Nil(t, tasks[0].onPropSwapped,
		"empty target tokenization must skip wiring — better than writing an empty override")
	assert.Equal(t, "word", s.TokenizationFor("name", "word"))
}

func TestMaybeWirePerPropOverlaySet_NilInputs_NoOp(t *testing.T) {
	// Pure guard against nil-deref under unexpected call sites; both
	// inputs are non-nil in production but defensive checks let the
	// helper be tested via unit tests without bringing up a real
	// shard.
	maybeWirePerPropOverlaySet(nil, &ReindexTaskPayload{}, nil)
	maybeWirePerPropOverlaySet(&Shard{}, nil, nil)
}

func TestMaybeWirePerPropOverlaySet_NilTaskInSlice_Skipped(t *testing.T) {
	// A nil task entry must not panic — defensive, mirrors the
	// production loop's nil guard.
	s := &Shard{}
	tasks := overlayTasks(nil, &SearchableRetokenizeStrategy{})
	payload := &ReindexTaskPayload{
		MigrationType:      ReindexTypeChangeTokenization,
		TargetTokenization: "field",
		Properties:         []string{"name"},
	}
	maybeWirePerPropOverlaySet(s, payload, tasks)
	require.NotNil(t, tasks[1].onPropSwapped, "non-nil task must get the hook")
	fireAllPropHooks(tasks, payload.Properties)
	assert.Equal(t, "field", s.TokenizationFor("name", "word"))
}
