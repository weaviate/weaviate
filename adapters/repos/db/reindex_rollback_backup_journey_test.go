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
	"encoding/json"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/schema"
)

// journeyCollection and journeyShard are the one class and shard the backup in
// this file captures.
const (
	journeyCollection = "Movies"
	journeyShard      = "shard1"
)

// rollbackJourney is one node's view of the race: a backup capturing a shard
// while a reindex submission that lost the race withdraws itself.
type rollbackJourney struct {
	db       *DB
	index    *Index
	provider *ReindexProvider
	task     *distributedtask.Task
	payload  *ReindexTaskPayload
}

// newRollbackJourney wires the DB's three reindex lookups the way
// configure_api.go wires them, over a DTM that holds the single task under
// test. units is the task's unit state at the moment the backup asks.
func newRollbackJourney(t *testing.T, status distributedtask.TaskStatus,
	units map[string]*distributedtask.Unit, finishedAt time.Time,
) *rollbackJourney {
	t.Helper()

	logger, _ := logrustest.NewNullLogger()
	payload := &ReindexTaskPayload{
		Collection:    journeyCollection,
		Properties:    []string{"body"},
		MigrationType: ReindexTypeChangeTokenization,
		UnitToShard:   map[string]string{"u1": journeyShard},
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)

	task := &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "task-journey", Version: 1},
		Payload:        raw,
		Status:         status,
		Units:          units,
		FinishedAt:     finishedAt,
	}

	index := &Index{
		Config:     IndexConfig{ClassName: schema.ClassName(journeyCollection)},
		closingCtx: context.Background(),
	}
	database := &DB{
		logger:  logger,
		indices: map[string]*Index{indexID(schema.ClassName(journeyCollection)): index},
	}
	index.db = database

	provider := NewReindexProvider(database, nil, logger, "node1",
		func() int { return 1 }, context.Background())

	list := func(context.Context) (map[string][]*distributedtask.Task, error) {
		return map[string][]*distributedtask.Task{ReindexNamespace: {task}}, nil
	}

	// The per-shard gate: a live task on this shard refuses, mirroring
	// newShardReindexActivityBuilder.
	database.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		live := map[string]bool{}
		for _, tsk := range []*distributedtask.Task{task} {
			if !IsLiveReindexTaskStatus(tsk.Status) {
				continue
			}
			for _, shardName := range payload.UnitToShard {
				live[shardName] = true
			}
		}
		return func(_, shardName string) bool { return live[shardName] }
	})
	database.SetReindexCleanupInProgressLookup(provider.CleanupInProgressLookupBuilder())
	database.SetReindexOverlapLookup(NewReindexOverlapLookup(list, time.Hour))

	return &rollbackJourney{
		db: database, index: index, provider: provider,
		task: task, payload: payload,
	}
}

// captureShard is what a backup does per shard while it walks the collection:
// Shard.HaltForTransfer consults exactly this gate before hardlinking.
func (j *rollbackJourney) captureShard() error {
	return j.index.refuseIfReindexInFlight(journeyShard)
}

// The submit path rolls a committed reindex back precisely so the backup that
// claimed the slot first can finish. The rollback's own cancel must therefore
// not close the backup gate over the shards that backup is capturing, or it
// fails the operation it exists to protect.
//
// Two rounds, both required: the cancel apply lands on every node (the gate the
// cancel handler parks), and the teardown follows on each of them.
func TestRollbackForABackupLeavesThatBackupAbleToComplete(t *testing.T) {
	backupStart := time.Now().Add(-time.Minute)
	logger, _ := logrustest.NewNullLogger()

	// The rollback's shape: cancelled before the scheduler handed any unit out.
	j := newRollbackJourney(t, distributedtask.TaskStatusCancelled,
		map[string]*distributedtask.Unit{"u1": {ID: "u1", Status: distributedtask.UnitStatusPending}},
		time.Now())

	require.NoError(t, j.captureShard(),
		"the backup is mid-capture when the rollback's cancel applies; "+
			"a task no worker ever claimed has nothing to protect")

	j.provider.OnTerminalApplied(j.task)
	require.NoError(t, j.captureShard(),
		"the cancel-apply gate must not refuse the backup the rollback was performed for")

	j.provider.autoCleanupAfterTerminal(j.task, j.payload, logger)
	require.NoError(t, j.captureShard(),
		"the teardown must not refuse it either; there are no sidecars to tear down")

	require.NoError(t,
		j.db.RefuseIfReindexOverlapped(context.Background(), []string{journeyCollection}, backupStart),
		"the commit-time backstop already waives this task; the gate has to agree, "+
			"or the backup is admitted shard by shard and then refused at commit")

	require.True(t, j.provider.AnyCleanupInProgressForCollection(journeyCollection),
		"the confirmation latch is a separate signal and must still fire, "+
			"or the node handling the cancel burns its whole per-owner budget")
}

// The waiver is unit state, not the cancelled status. A cancel that landed on a
// migration already rebuilding buckets leaves half-removed sidecars behind, and
// the backup must still be refused for it.
func TestCancelOfAMigrationThatRanStillRefusesTheBackup(t *testing.T) {
	backupStart := time.Now().Add(-time.Minute)

	j := newRollbackJourney(t, distributedtask.TaskStatusCancelled,
		map[string]*distributedtask.Unit{"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress}},
		time.Now())

	j.provider.OnTerminalApplied(j.task)
	require.Error(t, j.captureShard(),
		"a worker claimed this unit, so the sidecars are still coming down")
	require.Equal(t, ReindexHoldCleanup,
		j.provider.HoldForShard(journeyCollection, journeyShard))

	require.Error(t,
		j.db.RefuseIfReindexOverlapped(context.Background(), []string{journeyCollection}, backupStart),
		"and the backstop refuses it at commit for the same reason")
}
