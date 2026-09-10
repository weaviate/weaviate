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
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// recoverableDirOwnershipCases drops rebuild-searchable, the one migration
// type [buildRecoveryTasks] has no arm for. That gap is older than the
// generation question these tests are about, and it is not this file's to
// pin.
func recoverableDirOwnershipCases() []dirOwnershipCase {
	var out []dirOwnershipCase
	for _, c := range dirOwnershipCases() {
		if c.migration == ReindexTypeRebuildSearchable {
			continue
		}
		out = append(out, c)
	}
	return out
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

// A version outside [1, MaxInt] would build a task whose rebuilt data is
// never promoted, while the completion marker and schema flag already say
// the migration succeeded. Fail the unit instead.
func TestCreateReindexTasksRejectsUnusableGeneration(t *testing.T) {
	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeRepairFilterable,
		Collection:    "Books",
		Properties:    []string{dirOwnershipProp},
	}

	for _, tc := range []struct {
		name    string
		version uint64
		wantDir string
	}{
		{name: "zero names the canonical bucket", version: 0},
		{name: "past what an int holds", version: math.MaxUint64},
		{name: "lowest live generation", version: 1, wantDir: MigrationDirFilterableRoaringsetRefresh + "_1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newTestProvider(t)

			tasks, err := p.createReindexTasks(taskDescAt(tc.version), payload, t.TempDir(), false)

			if tc.wantDir == "" {
				require.Error(t, err)
				require.Empty(t, tasks)
				return
			}
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			require.Equal(t, tc.wantDir, tasks[0].strategy.MigrationDirName())
		})
	}
}

// seedInFlightMigration lays out an in-flight migration's on-disk state at
// dirGen and returns the tracker dirs it wrote. recordedVersion, kept
// separate from dirGen, simulates a node that numbered dirs per-node
// before this branch switched to task-version generations.
func seedInFlightMigration(t *testing.T, p *ReindexProvider, lsmPath string,
	c dirOwnershipCase, dirGen, recordedVersion uint64,
) []string {
	t.Helper()
	tasks, err := p.createReindexTasks(taskDescAt(dirGen), c.payload(), lsmPath, false)
	require.NoError(t, err)
	require.NotEmpty(t, tasks)

	encoded, err := json.Marshal(reindexRecoveryRecord{
		TaskID:      taskDescAt(recordedVersion).ID,
		TaskVersion: recordedVersion,
		UnitID:      "unit-1",
		Payload:     *c.payload(),
	})
	require.NoError(t, err)

	var dirs []string
	for _, task := range tasks {
		dir := task.migrationPath(lsmPath)
		require.NoError(t, os.MkdirAll(dir, 0o777))
		require.NoError(t, os.WriteFile(filepath.Join(dir, reindexRecoveryPayloadFile), encoded, 0o666))
		// started and reindexed without tidied is the window recovery exists
		// for: the iteration is over, the swap has not run.
		for _, sentinel := range []string{"started.mig", "reindexed.mig"} {
			require.NoError(t, os.WriteFile(filepath.Join(dir, sentinel), nil, 0o666))
		}
		dirs = append(dirs, task.strategy.MigrationDirName())
	}
	sort.Strings(dirs)
	return dirs
}

// recoveredMigrationDirs is every tracker directory the recovered strategy
// instances name, deduplicated: a semantic migration rebuilds both of its
// tasks from each of its directories.
func recoveredMigrationDirs(recovered []RecoveredReindex) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range recovered {
		for _, task := range r.Tasks {
			name := task.strategy.MigrationDirName()
			if seen[name] {
				continue
			}
			seen[name] = true
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Recovery must read the generation from the directory name, like every
// other reader — payload.mig is copied into every tracker dir for a task
// and can't tell them apart.
func TestRecoveryNamesTheDirectoriesItRecoveredFrom(t *testing.T) {
	for _, tc := range recoverableDirOwnershipCases() {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newTestProvider(t)
			root := t.TempDir()
			lsm := filepath.Join(root, "books", "shard-1", "lsm")
			require.NoError(t, os.MkdirAll(lsm, 0o777))

			onDisk := seedInFlightMigration(t, p, lsm, tc, 3, 41)

			recovered, err := DiscoverInFlightReindexTasks(root, p.logger, nil)
			require.NoError(t, err)
			require.NotEmpty(t, recovered, "an unswapped migration is in flight on this shard")

			require.Equal(t, onDisk, recoveredMigrationDirs(recovered),
				"recovery must rebuild the strategies that name the directories on disk")

			for _, r := range recovered {
				require.Equal(t, uint64(41), r.Descriptor.Version,
					"the descriptor still carries the version the cluster knows the task by")
			}
		})
	}
}

// A tracker dir with no generation suffix must be skipped, not given an
// invented one — that would name sidecar buckets that don't exist and
// leave the dir reported in-flight forever.
func TestRecoverySkipsATrackerDirectoryThatNamesNoGeneration(t *testing.T) {
	for _, tc := range recoverableDirOwnershipCases() {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newTestProvider(t)
			root := t.TempDir()
			lsm := filepath.Join(root, "books", "shard-1", "lsm")
			require.NoError(t, os.MkdirAll(lsm, 0o777))

			for _, dir := range seedInFlightMigration(t, p, lsm, tc, 3, 41) {
				base := strings.TrimSuffix(dir, genSuffix(3))
				require.NotEqual(t, dir, base)
				require.NoError(t, os.Rename(
					filepath.Join(lsm, migrationsDir, dir),
					filepath.Join(lsm, migrationsDir, base)))
			}

			recovered, err := DiscoverInFlightReindexTasks(root, p.logger, nil)
			require.NoError(t, err)
			require.Empty(t, recoveredMigrationDirs(recovered),
				"a directory name with no generation gives recovery nothing to rebuild from")
		})
	}
}
