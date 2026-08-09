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
)

// A finalize that cannot promote its ingest dir must not leave the marker
// behind that says it did. The marker is what
// ShardReindexTaskGeneric.migrationAlreadyFinalized reads, and a task that
// reads it acks its swap as complete — for enable-rangeable that ack is
// what lets the schema flip to IndexRangeFilters=true over a replica whose
// canonical dir was never created, so range filters serve zero rows.
func TestFinalize_FailedPromotionLeavesNoAck(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions, so the I/O failure cannot be injected")
	}

	shape := rangeableResidue()

	tests := []struct {
		name string
		// setup lays down the residue and returns the lsm path. It runs
		// before the lsm dir is made read-only.
		setup func(t *testing.T) string
		// readOnlyLsm injects the I/O failure by taking write permission
		// away from the lsm dir; .migrations/ underneath stays writable,
		// so this is a localized failure, not a full-disk one.
		readOnlyLsm bool
		wantPromote bool
	}{
		{
			name: "backup removal fails",
			setup: func(t *testing.T) string {
				lsmPath, _ := writeTidiedSwappedResidue(t, shape)
				return lsmPath
			},
			readOnlyLsm: true,
		},
		{
			name: "ingest rename fails",
			setup: func(t *testing.T) string {
				lsmPath, backupDir := writeTidiedSwappedResidue(t, shape)
				// No backup dir, so the loop reaches the rename directly.
				require.NoError(t, os.RemoveAll(filepath.Join(lsmPath, backupDir)))
				return lsmPath
			},
			readOnlyLsm: true,
		},
		{
			name: "promotion succeeds",
			setup: func(t *testing.T) string {
				lsmPath, _ := writeTidiedSwappedResidue(t, shape)
				return lsmPath
			},
			wantPromote: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lsmPath := tc.setup(t)
			if tc.readOnlyLsm {
				require.NoError(t, os.Chmod(lsmPath, 0o500))
				t.Cleanup(func() { os.Chmod(lsmPath, 0o755) })
			}
			logger, _ := test.NewNullLogger()

			FinalizeCompletedMigrations(lsmPath, classWith(shape.agreeing), nil, logger)

			migDir := filepath.Join(lsmPath, ".migrations", shape.dirName)
			marker := migrationFinalizedMarkerPath(lsmPath, shape.dirName)
			task := &ShardReindexTaskGeneric{
				logger:   logger,
				strategy: &FilterableToRangeableStrategy{propNames: []string{"price"}, generation: 1},
			}
			require.Equal(t, shape.dirName, task.MigrationDirName(),
				"the task under test must read the tracker this fixture writes")

			if tc.wantPromote {
				require.DirExists(t, filepath.Join(lsmPath, shape.canonicalDir))
				require.NoDirExists(t, migDir)
				require.FileExists(t, marker)
				require.True(t, task.migrationAlreadyFinalized(lsmPath))
				return
			}

			require.NoDirExists(t, filepath.Join(lsmPath, shape.canonicalDir),
				"the promotion failed, so there is no canonical dir")
			require.DirExists(t, migDir,
				"the tracker must survive so the next shard init retries the promotion")
			require.NoFileExists(t, marker,
				"the marker claims the generation was promoted; it was not")
			require.False(t, task.migrationAlreadyFinalized(lsmPath),
				"a task still running for this generation must not ack a swap that never landed")
		})
	}
}

// writeTidiedSwappedResidue is the mainline post-swap shape every
// successful runtime swap leaves for the next shard init: tracker with
// swapped.mig and tidied.mig, the migrated data in the ingest dir, the
// pre-migration data in the backup dir, and no canonical dir yet.
func writeTidiedSwappedResidue(t *testing.T, shape residueShape) (lsmPath, backupDir string) {
	t.Helper()
	lsmPath, backupDir = writeSwappedResidue(t, shape)
	touchSentinel(t, filepath.Join(lsmPath, ".migrations", shape.dirName, "tidied.mig"))
	return lsmPath, backupDir
}
