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

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// One property is the only shape a submission can carry: the upsert handler
// builds every payload as []string{propertyName}.
const dirOwnershipProp = "title"

type dirOwnershipCase struct {
	name         string
	migration    ReindexMigrationType
	tokenization string
	strategy     string
}

// Every strategy, so a naming rule that holds for some of them cannot pass here.
func dirOwnershipCases() []dirOwnershipCase {
	return []dirOwnershipCase{
		{name: "change-algorithm", migration: ReindexTypeChangeAlgorithm},
		{name: "rebuild-searchable", migration: ReindexTypeRebuildSearchable},
		{name: "repair-filterable", migration: ReindexTypeRepairFilterable},
		{name: "enable-rangeable", migration: ReindexTypeEnableRangeable},
		{name: "repair-rangeable", migration: ReindexTypeRepairRangeable},
		{name: "enable-filterable", migration: ReindexTypeEnableFilterable},
		{name: "enable-searchable", migration: ReindexTypeEnableSearchable, tokenization: "word"},
		{
			name: "change-tokenization", migration: ReindexTypeChangeTokenization,
			tokenization: "field", strategy: "MapCollection",
		},
		{
			name: "change-tokenization-filterable", migration: ReindexTypeChangeTokenizationFilterable,
			tokenization: "field", strategy: "MapCollection",
		},
	}
}

func (c dirOwnershipCase) payload() *ReindexTaskPayload {
	return &ReindexTaskPayload{
		MigrationType:      c.migration,
		Collection:         "Books",
		Properties:         []string{dirOwnershipProp},
		TargetTokenization: c.tokenization,
		BucketStrategy:     c.strategy,
	}
}

// workingCopyDirs names the bucket directories a migration opens in the shard's
// LSM root, derived from the strategy instance the migration will run with so
// the names cannot drift from the production namers.
func workingCopyDirs(tasks []*ShardReindexTaskGeneric) []string {
	var out []string
	for _, task := range tasks {
		s := task.strategy
		bucket := s.SourceBucketName(dirOwnershipProp)
		out = append(out, bucket+s.ReindexSuffix(), bucket+s.IngestSuffix(), bucket+s.BackupSuffix())
	}
	return out
}

// The cancel cleanup removes a migration's bucket working copies before its
// tracker directory and only logs a removal that fails, so a working copy can
// outlive the tracker that named it. The next submission must not open its own
// working copy on those surviving files.
func TestRetryAvoidsWorkingCopiesThatOutlivedTheirTracker(t *testing.T) {
	for _, tc := range dirOwnershipCases() {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newTestProvider(t)
			lsm := t.TempDir()

			abandoned, err := p.createReindexTasks(taskDescAt(7), tc.payload(), lsm, false)
			require.NoError(t, err)
			require.NotEmpty(t, abandoned)

			survivors := map[string]bool{}
			for _, dir := range workingCopyDirs(abandoned) {
				require.NoError(t, os.MkdirAll(filepath.Join(lsm, dir), 0o777))
				survivors[dir] = true
			}
			require.NoDirExists(t, filepath.Join(lsm, migrationsDir),
				"the tracker is the part of the earlier attempt the cleanup did remove")

			retry, err := p.createReindexTasks(taskDescAt(9), tc.payload(), lsm, false)
			require.NoError(t, err)
			require.NotEmpty(t, retry)

			for _, dir := range workingCopyDirs(retry) {
				require.Falsef(t, survivors[dir],
					"the new migration opens working copy %q over the earlier attempt's files", dir)
			}
		})
	}
}

func taskDescAt(version uint64) distributedtask.TaskDescriptor {
	return distributedtask.TaskDescriptor{ID: "Books:migrate:ab12", Version: version}
}

// Regression guard, not the proof above: the rehydrate path builds the strategy
// a restart lost the instance of, so it has to land on the very directories the
// first instance wrote.
func TestRehydrateRebuildsTheDirectoryNamesTheMigrationWrote(t *testing.T) {
	for _, tc := range dirOwnershipCases() {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newTestProvider(t)
			lsm := t.TempDir()
			desc := taskDescAt(11)

			started, err := p.createReindexTasks(desc, tc.payload(), lsm, false)
			require.NoError(t, err)
			require.NotEmpty(t, started)

			gone, err := p.createReindexTasks(desc, tc.payload(), lsm, true)
			require.NoError(t, err)
			require.Empty(t, gone, "no tracker directory on disk means nothing to resume")

			for _, task := range started {
				require.NoError(t, os.MkdirAll(task.migrationPath(lsm), 0o777))
			}
			resumed, err := p.createReindexTasks(desc, tc.payload(), lsm, true)
			require.NoError(t, err)
			require.NotEmpty(t, resumed, "the tracker on disk is the one the rehydrate looks for")
			require.Equal(t, workingCopyDirs(started), workingCopyDirs(resumed))
		})
	}
}
