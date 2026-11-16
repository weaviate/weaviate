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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

func TestALoadFinishesAHalfWrittenSwapDecision(t *testing.T) {
	const word, field = models.PropertyTokenizationWord, models.PropertyTokenizationField
	promoted := []MigrationState{MigrationStatePromoted, MigrationStatePromoted}
	heldBack := []MigrationState{MigrationStateSwapped, MigrationStateMerged}
	tests := []struct {
		name        string
		decided     int
		breakOther  func(t *testing.T, f *tokenizationChange, other *ShardReindexTaskGeneric)
		wantTok     string
		wantStates  []MigrationState
		wantSwapErr bool
	}{
		{name: "the searchable half is decided", decided: 0, wantTok: word, wantStates: promoted},
		{name: "the filterable half is decided", decided: 1, wantTok: word, wantStates: promoted},
		// A half that cannot follow holds its decided sibling back, so the shard keeps the old pair.
		{
			name: "the undecided half missed writes", decided: 0, wantTok: field, wantStates: heldBack, wantSwapErr: true,
			breakOther: func(t *testing.T, f *tokenizationChange, other *ShardReindexTaskGeneric) {
				rec, ok := other.migrationRecord(f.shard)
				require.True(t, ok)
				stamped, ok := migrationRecordStampedUnmirrored(rec)
				require.True(t, ok)
				require.NoError(t, f.shard.migrationRecords.Put(stamped))
			},
		},
		{
			name: "the undecided half's staged data is gone", decided: 0, wantTok: field, wantStates: heldBack, wantSwapErr: true,
			breakOther: func(t *testing.T, f *tokenizationChange, other *ShardReindexTaskGeneric) {
				staged := other.ingestBucketName(tokChangeProp)
				require.NoError(t, f.shard.Store().ShutdownBucket(f.ctx, staged))
				require.NoError(t, os.RemoveAll(filepath.Join(f.shard.pathLSM(), staged)))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newTokenizationChange(t)
			f.decide(t, f.tasks[tc.decided])
			if tc.breakOther != nil {
				tc.breakOther(t, f, f.tasks[1-tc.decided])
			}

			f.reload(t)
			assert.Equal(t, tc.wantTok, f.tokenization())
			assert.Equal(t, [3]int{3, 3, 3}, f.counts(t), "Equal, Like and BM25 before any swap tick")
			assert.Equal(t, tc.wantStates, f.states(t))

			assert.Equal(t, tc.wantSwapErr, len(f.swap(f.freshTasks(t)).Errs) > 0)
			assert.Equal(t, tc.wantTok, f.tokenization())
			assert.Equal(t, [3]int{3, 3, 3}, f.counts(t))
		})
	}
}

// A load that cannot write records cannot finish a half-written decision, and
// the provider must not flip the decided half alone before the swap phase can.
func TestARehydrateFlipsNothingWhileItsUnitIsHalfDecided(t *testing.T) {
	f := newTokenizationChange(t)
	f.decide(t, f.tasks[0])
	name, records := f.shard.Name(), f.shard.migrationRecords.Dir()
	require.NoError(t, f.shard.Shutdown(f.ctx))
	simulateProcessRestartBucketCleanup(t, f.shard.pathLSM())
	denyDirectoryWrites(t, records)
	f.shard = openShardFromDisk(t, f.ctx, f.idx, f.class, name)
	require.Equal(t, [3]int{3, 3, 3}, f.counts(t), "the load itself must leave the old pair")

	tasks := f.freshTasks(t)
	logger, _ := logrustest.NewNullLogger()
	f.provider.runShardPrepPhase(f.ctx, f.payload, f.unit, f.shard, tasks, true, logger)
	assert.Equal(t, models.PropertyTokenizationField, f.tokenization())
	assert.Equal(t, [3]int{3, 3, 3}, f.counts(t))

	require.NoError(t, os.Chmod(records, 0o700))
	require.Empty(t, f.swap(tasks).Errs)
	assert.Equal(t, models.PropertyTokenizationWord, f.tokenization())
	assert.Equal(t, [3]int{3, 3, 3}, f.counts(t))
}
