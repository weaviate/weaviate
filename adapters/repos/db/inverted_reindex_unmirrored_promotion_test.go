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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// While the pointer still serves from the canonical directory it is the only
// complete copy, and an unmirrored boot took writes the staged copy never saw.
// Renaming over it is the data loss the stamp exists to stop.
func TestPromotionRefusesToReplaceALiveCanonicalDirAfterAnUnmirroredBoot(t *testing.T) {
	tests := []struct {
		name          string
		unmirrored    bool
		canonicalDir  bool
		displaced     string
		wantState     MigrationState
		wantCanonical string
		wantWithheld  bool
	}{
		{
			name:         "unmirrored: the displaced dir holds writes the staged copy missed, promote nothing",
			unmirrored:   true,
			displaced:    "property_title",
			wantState:    MigrationStateSwapped,
			wantWithheld: true,
		},
		{
			name:          "mirrored: the staged copy is current, promote over the canonical dir",
			canonicalDir:  true,
			wantState:     MigrationStatePromoted,
			wantCanonical: "property_title__g42_ingest",
		},
		{
			name:          "unmirrored: the canonical dir holds writes the staged copy missed, promote nothing",
			unmirrored:    true,
			canonicalDir:  true,
			wantState:     MigrationStateSwapped,
			wantCanonical: "property_title_searchable",
			wantWithheld:  true,
		},
		{
			name:          "unmirrored, but the flip already moved the pointer off the canonical dir: nothing to lose",
			unmirrored:    true,
			wantState:     MigrationStatePromoted,
			wantCanonical: "property_title__g42_ingest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newReconcileFixture(t)
			f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

			subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
			subject.Unmirrored = tt.unmirrored
			canonical := subject.Props["title"].Canonical

			present := []string{"property_title__g42_ingest"}
			if tt.canonicalDir {
				present = append(present, canonical)
			}
			displaced := canonical
			if tt.displaced != "" {
				displaced = tt.displaced
				present = append(present, displaced)
			}
			f.mkdirs(present...)
			f.put(NewMigrationRecordSwapped(subject, []string{"title"},
				map[string]string{"title": displaced}))

			r := f.reconcile()

			state, present2 := f.state(subject.Key)
			require.True(t, present2)
			require.Equal(t, tt.wantState, state)
			if tt.wantCanonical != "" {
				// mkdirs stamps each directory's name into its segment file, so this
				// reads which one now answers to the canonical name.
				require.Equal(t, tt.wantCanonical, f.contentOf(canonical))
			}
			if tt.displaced != "" {
				require.Equal(t, tt.displaced, f.contentOf(tt.displaced), "the displaced dir is the only complete copy")
			}
			if tt.wantWithheld {
				require.Equal(t, 1, r.WedgedCount())
				require.NotEmpty(t, f.errorLines("no double-write mirror armed"))
				require.Empty(t, f.disarmed)
			}
		})
	}
}

// Nothing else arms the mirror for a migration awaiting its flip, so the stamp
// is the only thing carrying that across the restart to the promotion.
func TestRecoveryWalkStampsAMigrationItCouldNotArm(t *testing.T) {
	// The writer accepts this record; recovery cannot build a task from it.
	noTokenization := func(s MigrationSubject) MigrationSubject {
		s.MigrationType = ReindexTypeEnableSearchable
		s.TargetTokenization = ""
		return s
	}

	tests := []struct {
		name           string
		rec            func(MigrationSubject) MigrationRecord
		wantUnmirrored bool
	}{
		{
			name: "merged, record builds a task: the walk arms the mirror itself",
			rec:  func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
		},
		{
			name:           "merged, record builds no task, so no mirror",
			rec:            func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(noTokenization(s)) },
			wantUnmirrored: true,
		},
		{
			name: "promoted: the canonical name already holds the migrated data",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordPromoted(s, s.Properties(), map[string]string{"title": s.Props["title"].Canonical})
			},
		},
		{
			name: "iterating: the scheduler restarts the unit and arms the mirror itself",
			rec:  func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterating(s, MigrationCheckpoint{}) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			lsm := filepath.Join(root, "books_abc", "shard-1", "lsm")
			require.NoError(t, os.MkdirAll(lsm, 0o777))

			logger, _ := test.NewNullLogger()
			subject := testMigrationSubject(42, StrategyCodeFilterableRoaringsetRefresh, "title")
			subject.MigrationType = ReindexTypeRepairFilterable

			store := NewMigrationRecordStore(lsm, logger)
			require.NoError(t, store.Load())
			require.NoError(t, store.Put(tt.rec(subject)))

			_, err := DiscoverInFlightReindexTasks(root, true, logger, nil)
			require.NoError(t, err)

			reread := NewMigrationRecordStore(lsm, logger)
			require.NoError(t, reread.Load())
			rec, ok := reread.Get(subject.Key)
			require.True(t, ok)
			require.Equal(t, tt.wantUnmirrored, rec.Subject().Unmirrored)
		})
	}
}
