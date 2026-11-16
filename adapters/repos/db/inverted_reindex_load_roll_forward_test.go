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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

const (
	tokWord  = models.PropertyTokenizationWord
	tokField = models.PropertyTokenizationField
)

func TestALoadFinishesAHalfWrittenSwapDecision(t *testing.T) {
	promoted := []MigrationState{MigrationStatePromoted, MigrationStatePromoted}
	tests := []struct {
		name        string
		decided     int
		breakOther  bool
		wantTok     string
		wantStates  []MigrationState
		wantSwapErr bool
	}{
		{name: "the searchable half is decided", decided: 0, wantTok: tokWord, wantStates: promoted},
		{name: "the filterable half is decided", decided: 1, wantTok: tokWord, wantStates: promoted},
		// A half that cannot follow holds its decided sibling back, so the shard keeps the old pair.
		{
			name: "the undecided half's staged data is gone", decided: 0, breakOther: true, wantTok: tokField,
			wantStates: []MigrationState{MigrationStateSwapped, MigrationStateMerged}, wantSwapErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newTokenizationChange(t)
			f.decide(t, f.tasks[tc.decided])
			if tc.breakOther {
				f.removeStaged(t, f.tasks[1-tc.decided])
			}
			hook := f.captureLogs()

			f.reload(t)
			assert.Equal(t, tc.wantTok, f.tokenization())
			assert.Equal(t, allFound, f.counts(t), "before any swap tick")
			assert.Equal(t, tc.wantStates, f.states(t))
			if !tc.breakOther {
				assert.Empty(t, errorLines(hook), "a load that finishes the unit reports nothing wrong")
			}

			assert.Equal(t, tc.wantSwapErr, len(f.swap(f.freshTasks(t)).Errs) > 0)
			assert.Equal(t, tc.wantTok, f.tokenization())
			assert.Equal(t, allFound, f.counts(t))
		})
	}
}

// Both halves are decided, so a load promotes them; a half that cannot be
// promoted must not leave its sibling serving alone.
func TestALoadPromotesAUnitWholeOrNotAtAll(t *testing.T) {
	swapped := []MigrationState{MigrationStateSwapped, MigrationStateSwapped}
	tests := []struct {
		name       string
		breakHalf  func(t *testing.T, f *tokenizationChange, half *ShardReindexTaskGeneric) (repair func())
		wantTok    string
		wantStates func(broken int) []MigrationState
	}{
		{name: "a half missed writes", breakHalf: stampUnmirrored, wantTok: tokField, wantStates: alwaysSwapped(swapped)},
		{name: "a half's staged data is gone", breakHalf: removeStagedHalf, wantTok: tokField, wantStates: alwaysSwapped(swapped)},
		{name: "a half's promotion fails part way", breakHalf: blockCanonicalClear, wantTok: tokWord, wantStates: brokenStaysSwapped},
	}
	for _, tc := range tests {
		for i := range 4 {
			broken, finished := i%2, i >= 2
			t.Run(fmt.Sprintf("%s/%s/task finished=%t", tc.name, halfName(broken), finished), func(t *testing.T) {
				f := newTokenizationChange(t)
				f.decide(t, f.tasks[0])
				f.decide(t, f.tasks[1])
				repair := tc.breakHalf(t, f, f.tasks[broken])
				live := tokField
				if finished {
					f.class.Properties[0].Tokenization = tokWord
					live = tokWord
				}

				f.reload(t)
				assert.Equal(t, tc.wantTok, queryTokenization(f.shard, tokChangeProp, live))
				assert.Equal(t, allFound, f.counts(t))
				if !finished { // a finished task's promoted records are retired at load
					assert.Equal(t, tc.wantStates(broken), f.states(t))
				}

				repair()
				f.reload(t)
				assert.Equal(t, allFound, f.counts(t), "after the cause is repaired")
			})
		}
	}

	t.Run("the staged data of the half that failed cannot be opened", func(t *testing.T) {
		f := newTokenizationChange(t)
		f.decide(t, f.tasks[0])
		f.decide(t, f.tasks[1])
		repairClear := blockCanonicalClear(t, f, f.tasks[1])
		staged := filepath.Join(f.shard.pathLSM(), f.tasks[1].ingestBucketName(tokChangeProp))
		name := f.shard.Name()
		require.NoError(t, f.shard.Shutdown(f.ctx))
		simulateProcessRestartBucketCleanup(t, f.shard.pathLSM())
		require.NoError(t, os.Chmod(staged, 0))
		t.Cleanup(func() { os.Chmod(staged, 0o700) })

		_, err := f.idx.initShard(f.ctx, name, f.class, nil, true, true)
		require.Error(t, err, "a shard that cannot serve the unit whole must not load")

		require.NoError(t, os.Chmod(staged, 0o700))
		repairClear()
		simulateProcessRestartBucketCleanup(t, f.shard.pathLSM())
		f.shard = openShardFromDisk(t, f.ctx, f.idx, f.class, name)
		assert.Equal(t, tokWord, f.tokenization())
		assert.Equal(t, allFound, f.counts(t))
	})
}

func halfName(broken int) string {
	return []string{"searchable", "filterable"}[broken]
}

func alwaysSwapped(states []MigrationState) func(int) []MigrationState {
	return func(int) []MigrationState { return states }
}

func brokenStaysSwapped(broken int) []MigrationState {
	states := []MigrationState{MigrationStatePromoted, MigrationStatePromoted}
	states[broken] = MigrationStateSwapped
	return states
}

func stampUnmirrored(t *testing.T, f *tokenizationChange, half *ShardReindexTaskGeneric) func() {
	rec, ok := half.migrationRecord(f.shard)
	require.True(t, ok)
	stamped, ok := migrationRecordStampedUnmirrored(rec)
	require.True(t, ok)
	require.NoError(t, f.shard.migrationRecords.Put(stamped))
	return func() {}
}

func removeStagedHalf(t *testing.T, f *tokenizationChange, half *ShardReindexTaskGeneric) func() {
	f.removeStaged(t, half)
	return func() {}
}

// blockCanonicalClear makes the load's removal of half's canonical directory
// fail part way: a subdirectory the load cannot empty.
func blockCanonicalClear(t *testing.T, f *tokenizationChange, half *ShardReindexTaskGeneric) func() {
	rec, ok := half.migrationRecord(f.shard)
	require.True(t, ok)
	blocker := filepath.Join(f.shard.pathLSM(), rec.Subject().Props[tokChangeProp].Canonical, "blocker")
	require.NoError(t, os.MkdirAll(blocker, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(blocker, "pinned"), nil, 0o600))
	denyDirectoryWrites(t, blocker)
	return func() { require.NoError(t, os.Chmod(blocker, 0o700)) }
}

func (f *tokenizationChange) removeStaged(t *testing.T, half *ShardReindexTaskGeneric) {
	t.Helper()
	staged := half.ingestBucketName(tokChangeProp)
	require.NoError(t, f.shard.Store().ShutdownBucket(f.ctx, staged))
	require.NoError(t, os.RemoveAll(filepath.Join(f.shard.pathLSM(), staged)))
}

func (f *tokenizationChange) captureLogs() *logrustest.Hook {
	logger, hook := logrustest.NewNullLogger()
	f.idx.logger = logger
	return hook
}

func errorLines(hook *logrustest.Hook) []string {
	var out []string
	for _, entry := range hook.AllEntries() {
		if entry.Level <= logrus.ErrorLevel {
			out = append(out, entry.Message)
		}
	}
	return out
}

// A load that cannot write records leaves the decision half-written; nothing may flip one half alone.
func TestAUnitRunFlipsNothingWhileItsUnitIsHalfDecided(t *testing.T) {
	f := newTokenizationChange(t)
	f.decide(t, f.tasks[0])
	name, records := f.shard.Name(), f.shard.migrationRecords.Dir()
	require.NoError(t, f.shard.Shutdown(f.ctx))
	simulateProcessRestartBucketCleanup(t, f.shard.pathLSM())
	denyDirectoryWrites(t, records)
	f.shard = openShardFromDisk(t, f.ctx, f.idx, f.class, name)
	require.Equal(t, allFound, f.counts(t), "the load itself must leave the old pair")

	f.runUnit(t, f.freshTasks(t))
	assert.Equal(t, tokField, f.tokenization())
	assert.Equal(t, allFound, f.counts(t))

	require.NoError(t, os.Chmod(records, 0o700))
	require.Empty(t, f.swap(f.freshTasks(t)).Errs)
	assert.Equal(t, tokWord, f.tokenization())
	assert.Equal(t, allFound, f.counts(t))
}

// runUnit runs the unit's worker the way the scheduler does after a restart.
func (f *tokenizationChange) runUnit(t *testing.T, tasks []*ShardReindexTaskGeneric) *fakeRecorder {
	t.Helper()
	f.provider.SeedReindexTaskCache(map[distributedtask.TaskDescriptor]map[string][]*ShardReindexTaskGeneric{
		f.desc: {f.unit: tasks},
	})
	raw, err := json.Marshal(f.payload)
	require.NoError(t, err)
	rec := newFakeRecorder()
	f.provider.processOneUnit(f.ctx, &distributedtask.Task{
		Namespace: ReindexNamespace, TaskDescriptor: f.desc,
		Status: distributedtask.TaskStatusStarted, Payload: raw,
	}, f.payload, f.idx, f.unit, rec)
	return rec
}
