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

// The canonical directory is the property's only complete copy while the
// pointer still serves from it. A boot that could not arm the mirror took
// writes into it that the staged copy never saw, so renaming over it is the
// data loss the stamp exists to stop.
func TestPromotionRefusesToReplaceALiveCanonicalDirAfterAnUnmirroredBoot(t *testing.T) {
	tests := []struct {
		name          string
		unmirrored    bool
		canonicalDir  bool
		wantState     MigrationState
		wantCanonical string
	}{
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
			wantCanonical: "property_title",
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

			present := []string{"property_title__g42_ingest"}
			if tt.canonicalDir {
				present = append(present, "property_title")
			}
			f.mkdirs(present...)
			f.put(NewMigrationRecordSwapped(subject, []string{"title"},
				map[string]string{"title": "property_title"}))

			f.reconcile()

			state, present2 := f.state(subject.Key)
			require.True(t, present2)
			require.Equal(t, tt.wantState, state)
			// mkdirs stamps each directory's own name into its segment file, so
			// this reads which directory now answers to the canonical name.
			require.Equal(t, tt.wantCanonical, f.contentOf("property_title"))
		})
	}
}

func TestPromotionSaysWhyItWithheldAfterAnUnmirroredBoot(t *testing.T) {
	f := newReconcileFixture(t)
	f.class = testClassWithTokenization(models.PropertyTokenizationWord, "title")

	subject := testMigrationSubject(42, StrategyCodeSearchableRetokenize, "title")
	subject.Unmirrored = true
	f.mkdirs("property_title__g42_ingest", "property_title")
	f.put(NewMigrationRecordSwapped(subject, []string{"title"},
		map[string]string{"title": "property_title"}))

	r := f.reconcile()

	require.Equal(t, 1, r.WedgedCount())
	require.NotEmpty(t, f.errorLines("no double-write mirror armed"))
}

// A boot that cannot build a task for a migration awaiting its flip arms no
// mirror for it, and nothing else does. The stamp is the only thing that
// carries that across the restart to the promotion.
func TestRecoveryWalkStampsAMigrationItCouldNotArm(t *testing.T) {
	const trackerDir = "filterable_roaringset_refresh_title_1"

	payload := `{"taskID":"Books:repair-filterable:title:ab12","taskVersion":42,` +
		`"unitID":"shard-1__node-0","payload":{"migrationType":"repair-filterable",` +
		`"collection":"Books","properties":["title"]}}`

	tests := []struct {
		name           string
		rec            func(MigrationSubject) MigrationRecord
		payload        string
		wantUnmirrored bool
	}{
		{
			name:    "merged, payload readable: the walk arms the mirror itself",
			rec:     func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			payload: payload,
		},
		{
			name:           "merged, payload truncated: no task, so no mirror",
			rec:            func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			payload:        "{",
			wantUnmirrored: true,
		},
		{
			name:           "merged, no payload at all",
			rec:            func(s MigrationSubject) MigrationRecord { return NewMigrationRecordMerged(s) },
			wantUnmirrored: true,
		},
		{
			name: "swapped but not promoted, no payload",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordSwapped(s, s.Properties, map[string]string{"title": s.CanonicalDirs["title"]})
			},
			wantUnmirrored: true,
		},
		{
			name: "promoted: the canonical name already holds the migrated data",
			rec: func(s MigrationSubject) MigrationRecord {
				return NewMigrationRecordPromoted(s, s.Properties, map[string]string{"title": s.CanonicalDirs["title"]})
			},
		},
		{
			name:    "iterating: the scheduler restarts the unit and arms the mirror itself",
			rec:     func(s MigrationSubject) MigrationRecord { return NewMigrationRecordIterating(s, MigrationCheckpoint{}) },
			payload: payload,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			lsm := filepath.Join(root, "books_abc", "shard-1", "lsm")
			migDir := filepath.Join(lsm, ".migrations", trackerDir)
			require.NoError(t, os.MkdirAll(migDir, 0o777))
			if tt.payload != "" {
				require.NoError(t, os.WriteFile(
					filepath.Join(migDir, reindexRecoveryPayloadFile), []byte(tt.payload), 0o600))
			}

			logger, _ := test.NewNullLogger()
			subject := testMigrationSubject(42, StrategyCodeFilterableRoaringsetRefresh, "title")
			subject.TrackerDir = trackerDir
			subject.MigrationType = ReindexTypeRepairFilterable

			store := NewMigrationRecordStore(lsm, logger)
			require.NoError(t, store.Load())
			require.NoError(t, store.Put(tt.rec(subject)))

			_, err := DiscoverInFlightReindexTasks(root, logger, nil)
			require.NoError(t, err)

			reread := NewMigrationRecordStore(lsm, logger)
			require.NoError(t, reread.Load())
			rec, ok := reread.Get(subject.Key)
			require.True(t, ok)
			require.Equal(t, tt.wantUnmirrored, rec.Subject().Unmirrored)
		})
	}
}
