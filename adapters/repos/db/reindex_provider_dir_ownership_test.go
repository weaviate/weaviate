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
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// One property is the only shape a submission can carry: the upsert handler
// builds every payload as []string{propertyName}.
const (
	dirOwnershipProp = "title"
	dirOwnershipUnit = "shard-1__node-0"
)

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
			tokenization: "field", strategy: lsmkv.StrategyMapCollection,
		},
		{
			name: "change-tokenization-filterable", migration: ReindexTypeChangeTokenizationFilterable,
			tokenization: "field", strategy: lsmkv.StrategyMapCollection,
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
		out = append(out, bucket+s.ReindexSuffix(), bucket+s.IngestSuffix())
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

			abandoned, err := p.createReindexTasks(taskDescAt(7), dirOwnershipUnit, tc.payload())
			require.NoError(t, err)
			require.NotEmpty(t, abandoned)

			survivors := map[string]bool{}
			for _, dir := range workingCopyDirs(abandoned) {
				require.NoError(t, os.MkdirAll(filepath.Join(lsm, dir), 0o777))
				survivors[dir] = true
			}
			require.NoDirExists(t, filepath.Join(lsm, migrationsDir),
				"the tracker is the part of the earlier attempt the cleanup did remove")

			retry, err := p.createReindexTasks(taskDescAt(9), dirOwnershipUnit, tc.payload())
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
// first instance wrote. The records alone say which halves are still in flight.
func TestRehydrateRebuildsTheDirectoryNamesTheMigrationWrote(t *testing.T) {
	tests := []struct {
		name       string
		recorded   bool
		unreadable bool
		wantSkip   bool
		wantErrs   bool
	}{
		{name: "records, no tracker directory", recorded: true},
		{name: "no record: the migration already finalized here", wantSkip: true},
		{
			// Read as absent, it would report the unit finalized and flip the schema.
			name: "a record this build cannot read", recorded: true, unreadable: true, wantErrs: true,
		},
	}

	for _, tt := range tests {
		for _, tc := range dirOwnershipCases() {
			t.Run(tt.name+"/"+tc.name, func(t *testing.T) {
				ctx := testCtx()
				className := "Rehydrate" + uuid.NewString()[:8]
				shd, idx := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{dirOwnershipProp}),
					enthnsw.UserConfig{Skip: true}, false, false, false)
				shard := shd.(*Shard)
				defer shard.Shutdown(context.Background())
				registerIndex(idx, className)
				logger, _ := logrustest.NewNullLogger()
				p := NewReindexProvider(idx.db, nil, nil, logger, "node1", nil, ctx)

				unitID := shard.migrationUnit()
				payload := tc.payload()
				payload.UnitToShard = map[string]string{unitID: shard.Name()}
				desc := taskDescAt(11)

				started, err := p.createReindexTasks(desc, unitID, payload)
				require.NoError(t, err)
				store := shard.migrationRecordStore()
				if tt.recorded {
					for _, task := range started {
						subject := task.migrationSubject(shard, []string{dirOwnershipProp}, time.Now())
						require.NoError(t, store.Put(NewMigrationRecordIterated(subject)))
					}
				}
				if tt.unreadable {
					plantUnreadableRecord(t, store.Dir())
					require.NoError(t, store.Load())
				}

				got := p.resolveUnitForPhase(ctx, &distributedtask.Task{TaskDescriptor: desc},
					payload, unitID, idx, logger)

				switch {
				case tt.wantErrs:
					require.NotEmpty(t, got.Errs)
					require.False(t, got.Skip)
				case tt.wantSkip:
					require.True(t, got.Skip)
				default:
					require.Empty(t, got.Errs)
					require.Equal(t, workingCopyDirs(started), workingCopyDirs(got.UnitTasks))
				}
			})
		}
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

			tasks, err := p.createReindexTasks(taskDescAt(tc.version), dirOwnershipUnit, payload)

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
