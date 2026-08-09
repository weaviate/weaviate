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
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// A swap that got as far as swapped.mig has already renamed the canonical
// dir to backup_<gen>, so there is no pre-migration dir left under the
// canonical name to fall back to. Promotion of the ingest dir is the
// second half of an operation that already committed, not a decision.
//
// These tests cover the window between the rename and markTidied. The
// merged-only window (canonical dir still in place, refusal lossless) is
// covered by TestFinalize_MergedResiduePromotionScoping.

func repairRangeableSwappedResidue() residueShape {
	shape := rangeableResidue()
	shape.payload.MigrationType = ReindexTypeRepairRangeable
	return shape
}

// writeSwappedResidue lays down the disk state a node has after its
// runtime swap renamed the canonical dir away but died before
// markTidied: tracker with merged.mig + swapped.mig, the ingest dir
// holding the migrated data, the pre-migration data at backup_<gen>,
// and no canonical dir at all.
func writeSwappedResidue(t *testing.T, shape residueShape) (lsmPath, backupDir string) {
	t.Helper()
	lsmPath = writeMergedResidue(t, shape, true)
	touchSentinel(t, filepath.Join(lsmPath, ".migrations", shape.dirName, "swapped.mig"))

	_, gen, ok := parseMigrationDirName(shape.dirName)
	require.True(t, ok, "a shape's dir name must carry its generation")
	backupDir = shape.canonicalDir + backupSuffixForShape(shape) + "_" + strconv.Itoa(gen)
	require.NoError(t, os.Rename(
		filepath.Join(lsmPath, shape.canonicalDir), filepath.Join(lsmPath, backupDir)))
	return lsmPath, backupDir
}

// backupSuffixForShape reads the backup suffix straight out of the
// production suffix table, so a renamed suffix cannot make these tests
// silently exercise a directory nothing else writes.
func backupSuffixForShape(shape residueShape) string {
	suffixes := migrationSuffixes(shape.dirName)
	if suffixes == nil {
		panic("no suffixes for " + shape.dirName)
	}
	return suffixes.backupSuffix
}

// TestFinalize_SwappedResidueIsAlwaysPromoted is the receipt for the
// mid-repair restart: a shard whose swap committed must come back with
// its canonical dir holding the migrated data, whatever the task's
// liveness and whatever the schema says. Leaving it unpromoted lets
// shard init create an empty canonical bucket beside a populated ingest
// dir, and the property serves zero rows until a later restart.
func TestFinalize_SwappedResidueIsAlwaysPromoted(t *testing.T) {
	shapes := map[string]residueShape{
		"repair-rangeable": repairRangeableSwappedResidue(),
		"enable-rangeable": rangeableResidue(),
		"tokenization":     tokenizationResidue(),
	}
	livenesses := map[string]ReindexTaskLiveness{
		"live task":        ReindexTaskLivenessLive,
		"dead task":        ReindexTaskLivenessDead,
		"unknown liveness": ReindexTaskLivenessUnknown,
	}

	for shapeName, shape := range shapes {
		for livenessName, liveness := range livenesses {
			for _, agrees := range []bool{true, false} {
				name := shapeName + "/" + livenessName
				if agrees {
					name += "/schema agrees"
				} else {
					name += "/schema disagrees"
				}
				t.Run(name, func(t *testing.T) {
					lsmPath, backupDir := writeSwappedResidue(t, shape)
					prop := shape.disagreeing
					if agrees {
						prop = shape.agreeing
					}
					logger, _ := test.NewNullLogger()

					FinalizeCompletedMigrations(lsmPath, classWith(prop), fixedLiveness(liveness), logger)

					got, err := os.ReadFile(filepath.Join(lsmPath, shape.canonicalDir, "segment.db"))
					require.NoError(t, err,
						"the canonical dir must exist after finalize, or shard init creates an empty one")
					require.Equal(t, "migrated", string(got))

					_, err = os.Stat(filepath.Join(lsmPath, shape.ingestDir))
					require.True(t, os.IsNotExist(err), "the ingest dir was promoted, so it is gone")
					_, err = os.Stat(filepath.Join(lsmPath, backupDir))
					require.True(t, os.IsNotExist(err), "the backup dir is superseded by the promotion")
					_, err = os.Stat(filepath.Join(lsmPath, ".migrations", shape.dirName))
					require.True(t, os.IsNotExist(err), "the tracker has done its job")
				})
			}
		}
	}
}

// TestFinalize_SwappedResidueKeepsTheTrackerSelfConsistent pins that the
// promotion writes tidied.mig into the tracker before renaming, so a
// crash between the two leaves a state the next startup reads the same
// way rather than one that looks merged-only again.
func TestFinalize_SwappedResidueWritesTidiedBeforePromoting(t *testing.T) {
	shape := repairRangeableSwappedResidue()
	lsmPath, _ := writeSwappedResidue(t, shape)
	migDir := filepath.Join(lsmPath, ".migrations", shape.dirName)

	// A tracker dir the process cannot write to makes the sentinel step
	// fail; the rename must not happen without it.
	require.NoError(t, os.Chmod(migDir, 0o555))
	t.Cleanup(func() { _ = os.Chmod(migDir, 0o755) })

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, classWith(shape.agreeing), fixedLiveness(ReindexTaskLivenessLive), logger)

	_, err := os.Stat(filepath.Join(lsmPath, shape.ingestDir))
	require.NoError(t, err, "without its sentinels the generation must be left untouched for the next startup")
}

// TestFinalize_PromotionLeavesAFinalizedMarker pins the record the
// promotion leaves in place of the tracker it removes. Without it a task
// still running for that generation cannot tell "startup did my work"
// from "this shard never ran the migration", and fails the cluster's
// migration on a shard whose data is correct. Markers below the promoted
// generation are swept at the same time, so they do not pile up within a
// rising sequence of generations.
func TestFinalize_PromotionLeavesAFinalizedMarker(t *testing.T) {
	// Generation 2 so there is a lower generation for the sweep to find.
	shape := repairRangeableSwappedResidue()
	shape.dirName = "filterable_to_rangeable_price_2"
	shape.ingestDir = "property_price_rangeable__rangeable_ingest_2"
	lsmPath, _ := writeSwappedResidue(t, shape)

	// The sweep runs on the pre-promotion dir listing and on the
	// generation finalize settled on. The helper's own test supplies
	// both by hand; only this path derives them.
	superseded := migrationFinalizedMarkerPath(lsmPath, "filterable_to_rangeable_price_1")
	otherNamespace := migrationFinalizedMarkerPath(lsmPath, "filterable_to_rangeable_weight_1")
	touchSentinel(t, superseded)
	touchSentinel(t, otherNamespace)

	logger, _ := test.NewNullLogger()
	FinalizeCompletedMigrations(lsmPath, classWith(shape.agreeing), fixedLiveness(ReindexTaskLivenessLive), logger)

	require.FileExists(t, migrationFinalizedMarkerPath(lsmPath, shape.dirName),
		"the promoted generation must leave a marker behind")
	require.NoFileExists(t, superseded,
		"a marker below the promoted generation must be swept, or markers pile up")
	require.FileExists(t, otherNamespace,
		"another namespace's marker is not this promotion's business")
}

// TestRemoveStaleFinalizedMarkers pins how narrow the sweep is: one
// namespace's markers below the generation just promoted are gone,
// everything else is left alone, including a higher generation of the
// same namespace.
func TestRemoveStaleFinalizedMarkers(t *testing.T) {
	namespace := "filterable_to_rangeable_price"
	tests := []struct {
		name     string
		marker   string
		survives bool
	}{
		{name: "superseded generation", marker: namespace + "_1", survives: false},
		{name: "the generation just promoted", marker: namespace + "_3", survives: true},
		{name: "a later generation", marker: namespace + "_4", survives: true},
		{name: "another namespace", marker: "filterable_to_rangeable_weight_1", survives: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := t.TempDir()
			migrationsDir := filepath.Join(lsmPath, ".migrations")
			require.NoError(t, os.MkdirAll(migrationsDir, 0o755))
			path := migrationFinalizedMarkerPath(lsmPath, tc.marker)
			touchSentinel(t, path)
			entries, err := os.ReadDir(migrationsDir)
			require.NoError(t, err)

			logger, _ := test.NewNullLogger()
			removeStaleFinalizedMarkers(migrationsDir, entries, namespace, 3, logger)

			if tc.survives {
				require.FileExists(t, path)
				return
			}
			require.NoFileExists(t, path)
		})
	}
}

// TestMigrationAlreadyFinalized covers the rule that keeps a marker from
// acking a migration that never ran.
//
// A marker outlives its generation: generation numbers are reused, and a
// namespace with no tracker dir is never swept. So a later migration can
// find a marker carrying its own exact name. What separates it from a
// generation startup really did promote is the tracker dir: production
// writes payload.mig into it before any phase runs, so a live generation
// always has a file there.
func TestMigrationAlreadyFinalized(t *testing.T) {
	tests := []struct {
		name string
		// marker is the <dirName>.finalized.mig file a promotion leaves.
		marker bool
		// trackerFiles are laid down in the tracker dir; a non-nil empty
		// slice creates the dir and leaves it empty.
		trackerFiles       []string
		want               bool
		wantTrackerDirGone bool
	}{
		{
			name:         "no marker, live tracker",
			trackerFiles: []string{reindexRecoveryPayloadFile},
			want:         false,
		},
		{
			name:         "marker plus a tracker that already holds payload.mig",
			marker:       true,
			trackerFiles: []string{reindexRecoveryPayloadFile},
			want:         false,
		},
		{
			name:         "marker plus a tracker mid-reindex",
			marker:       true,
			trackerFiles: []string{reindexRecoveryPayloadFile, "started.mig"},
			want:         false,
		},
		{
			name:               "marker plus an empty tracker dir",
			marker:             true,
			trackerFiles:       []string{},
			want:               true,
			wantTrackerDirGone: true,
		},
		{
			name:               "marker, tracker dir gone",
			marker:             true,
			want:               true,
			wantTrackerDirGone: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			task := &ShardReindexTaskGeneric{
				logger: logger,
				strategy: &FilterableToRangeableStrategy{
					propNames:  []string{filterableToRangeablePropName},
					generation: 1,
				},
			}

			lsmPath := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, ".migrations"), 0o755))
			migDir := filepath.Join(lsmPath, ".migrations", task.MigrationDirName())
			if tc.trackerFiles != nil {
				require.NoError(t, os.MkdirAll(migDir, 0o755))
				for _, name := range tc.trackerFiles {
					touchSentinel(t, filepath.Join(migDir, name))
				}
			}
			if tc.marker {
				touchSentinel(t, migrationFinalizedMarkerPath(lsmPath, task.MigrationDirName()))
			}

			require.Equal(t, tc.want, task.migrationAlreadyFinalized(lsmPath))

			if tc.wantTrackerDirGone {
				require.NoDirExists(t, migDir,
					"an empty tracker dir must not survive — it blinds the startup damage audit")
			}
		})
	}
}

// TestSwappedResidueRecordShape guards the fixture against drift: the
// payload the helper writes must be the one the production loader reads.
func TestSwappedResidueRecordShape(t *testing.T) {
	shape := repairRangeableSwappedResidue()
	lsmPath, _ := writeSwappedResidue(t, shape)

	raw, err := os.ReadFile(filepath.Join(lsmPath, ".migrations", shape.dirName, reindexRecoveryPayloadFile))
	require.NoError(t, err)
	var rec reindexRecoveryRecord
	require.NoError(t, json.Unmarshal(raw, &rec))
	require.Equal(t, ReindexTypeRepairRangeable, rec.Payload.MigrationType)
}
