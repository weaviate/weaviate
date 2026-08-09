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

	backupDir = shape.canonicalDir + backupSuffixForShape(shape) + "_1"
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
