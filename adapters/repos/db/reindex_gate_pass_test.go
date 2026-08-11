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
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	esync "github.com/weaviate/weaviate/entities/sync"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// capturePassFixture builds an Index whose capture pass reaches the backup gate
// once per shard. Every shard is inactive with an on-disk directory, which is
// the shortest route from [Index.descriptor] to
// [Index.backupInactiveShardWithHardlinks]'s gate check.
func capturePassFixture(t testing.TB, collection string, shards []string) (*Index, *logrustest.Hook) {
	t.Helper()

	logger, hook := logrustest.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	root := t.TempDir()
	db := &DB{logger: logger, localNodeName: "weaviate-0"}

	physical := make(map[string]sharding.Physical, len(shards))
	for _, s := range shards {
		physical[s] = sharding.Physical{Name: s, BelongsToNodes: []string{"weaviate-0"}}
	}
	shardState := &sharding.State{IndexID: collection, Physical: physical}

	reader := schemaUC.NewMockSchemaReader(t)
	reader.On("Read", collection, true, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		fn := args.Get(2).(func(*models.Class, *sharding.State) error)
		require.NoError(t, fn(&models.Class{Class: collection}, shardState))
	}).Maybe()
	getter := schemaUC.NewMockSchemaGetter(t)
	getter.On("NodeName").Return("weaviate-0").Maybe()

	idx := &Index{
		db:               db,
		logger:           logger,
		Config:           IndexConfig{ClassName: schema.ClassName(collection), RootPath: root},
		schemaReader:     reader,
		getSchema:        getter,
		backupLock:       esync.NewKeyRWLocker(),
		shardCreateLocks: esync.NewKeyRWLocker(),
	}
	db.indices = map[string]*Index{indexID(schema.ClassName(collection)): idx}

	for _, s := range shards {
		require.NoError(t, os.MkdirAll(shardPath(idx.path(), s), 0o755))
	}
	return idx, hook
}

// countingActivityBuilder installs an activity lookup that tallies how many
// times it was built. Concurrent because the hardlink capture pass fans its
// shards out over an error group.
func countingActivityBuilder(db *DB, builds *atomic.Int64, live bool) {
	db.SetShardReindexActivityLookup(func(context.Context) ShardReindexActivityLookup {
		builds.Add(1)
		return func(string, string) bool { return live }
	})
}

// Building the activity snapshot is a leader-forwarded RAFT query plus a decode
// of every task the cluster still retains, and the capture pass reaches it once
// per shard. [DB.Backupable] already holds one snapshot for its whole pass; this
// pins the same for the capture, which gets there through
// [ShardLike.HaltForTransfer] and [Index.backupInactiveShardWithHardlinks].
func TestCapturePassBuildsGateSnapshotOncePerPass(t *testing.T) {
	shards := []string{"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"}
	idx, _ := capturePassFixture(t, "CapturePassBuildCountClass", shards)

	var activityBuilds, cleanupBuilds atomic.Int64
	countingActivityBuilder(idx.db, &activityBuilds, true)
	idx.db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		cleanupBuilds.Add(1)
		return func(string, string) ReindexHold { return ReindexHoldNone }
	})

	var desc entitiesbackup.ClassDescriptor
	err := idx.descriptor(context.Background(), "backup-1", &desc, nil)
	require.Error(t, err, "a live task on every shard must refuse the capture")

	assert.Equal(t, int64(1), activityBuilds.Load(),
		"activity snapshot must be built once per capture pass, not once per shard")
	assert.Equal(t, int64(1), cleanupBuilds.Load(),
		"cleanup snapshot must be built once per capture pass, not once per shard")
}

// The snapshot is what the refusal is decided from, so a task live when the
// pass starts has to hold every shard of that pass — not just the ones checked
// before it was seen. Without this, reusing one snapshot could quietly narrow
// the refusal to the first shard.
func TestCapturePassSnapshotRefusesEveryShardOfThePass(t *testing.T) {
	shards := []string{"s1", "s2", "s3", "s4", "s5", "s6"}
	idx, hook := capturePassFixture(t, "CapturePassWideRefusalClass", shards)

	var builds atomic.Int64
	countingActivityBuilder(idx.db, &builds, true)

	var desc entitiesbackup.ClassDescriptor
	err := idx.descriptor(context.Background(), "backup-1", &desc, nil)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)
	assert.Empty(t, desc.Shards, "a refused capture must stage no shard")

	var counted bool
	for _, e := range hook.AllEntries() {
		n, ok := e.Data["blocked_shard_count"]
		if !ok {
			continue
		}
		counted = true
		assert.Equal(t, len(shards), n,
			"the pass snapshot must hold every shard, not only the first one checked")
	}
	assert.True(t, counted, "the refused pass must log its blocked shard count")
}

// A capture pass that clears the gate must not be refused by the reuse itself:
// one snapshot reporting no live task has to admit every shard.
func TestCapturePassSnapshotAdmitsEveryShardWhenNoTaskIsLive(t *testing.T) {
	shards := []string{"s1", "s2", "s3"}
	idx, _ := capturePassFixture(t, "CapturePassAdmitClass", shards)

	var builds atomic.Int64
	countingActivityBuilder(idx.db, &builds, false)

	var desc entitiesbackup.ClassDescriptor
	err := idx.descriptor(context.Background(), "backup-1", &desc, nil)

	// The fixture's shard dirs carry no metadata files, so the capture fails
	// past the gate. What matters is that it is not the gate that refused.
	require.NotErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
		"a snapshot reporting no live task must admit every shard of the pass")
	assert.Equal(t, int64(1), builds.Load(), "still one build for the pass")
}

// Outside a pass the gate keeps building its own snapshot: a single-shard
// caller has no pass to inherit one from, and a stale one carried in from
// somewhere else would answer for a moment it never checked.
func TestSingleShardGateBuildsItsOwnSnapshot(t *testing.T) {
	idx, _ := capturePassFixture(t, "SingleShardGateClass", []string{"s1"})

	var builds atomic.Int64
	countingActivityBuilder(idx.db, &builds, false)

	ctx := context.Background()
	for range 3 {
		require.NoError(t, idx.refuseIfReindexInFlight(ctx, "s1"))
	}
	assert.Equal(t, int64(3), builds.Load(),
		"a caller outside a pass must get a fresh snapshot per check")
}

// benchTaskList builds a DTM task list shaped like the one the production
// builder decodes: liveTasks running tasks plus completedTasks that the
// cluster still retains for its completed-task TTL, each with a real payload.
func benchTaskList(collection string, shards []string, liveTasks, completedTasks int) []*distributedtask.Task {
	newTask := func(i int, status distributedtask.TaskStatus) *distributedtask.Task {
		unitToShard := make(map[string]string, len(shards))
		unitToNode := make(map[string]string, len(shards))
		for u, s := range shards {
			unitToShard[strconv.Itoa(u)] = s
			unitToNode[strconv.Itoa(u)] = "weaviate-0"
		}
		raw, err := json.Marshal(ReindexTaskPayload{
			Collection:         collection,
			Properties:         []string{"title", "body", "author"},
			TargetTokenization: "word",
			UnitToShard:        unitToShard,
			UnitToNode:         unitToNode,
		})
		if err != nil {
			panic(err)
		}
		task := &distributedtask.Task{Namespace: ReindexNamespace, Payload: raw, Status: status}
		task.ID = fmt.Sprintf("task-%d", i)
		return task
	}

	tasks := make([]*distributedtask.Task, 0, liveTasks+completedTasks)
	for i := range liveTasks {
		tasks = append(tasks, newTask(i, distributedtask.TaskStatusStarted))
	}
	for i := range completedTasks {
		tasks = append(tasks, newTask(liveTasks+i, distributedtask.TaskStatusFinished))
	}
	return tasks
}

// benchActivityBuilder mirrors newShardReindexActivityBuilder in
// adapters/handlers/rest: filter by status, decode every live payload, index
// the shards it names. The leader round trip that fetches the list is left out,
// so the numbers here are a floor on the production cost, not an estimate of it.
func benchActivityBuilder(tasks []*distributedtask.Task, builds *atomic.Int64) ShardReindexActivityLookupBuilder {
	type shardKey struct{ collection, shardName string }
	return func(context.Context) ShardReindexActivityLookup {
		builds.Add(1)
		live := make(map[shardKey]bool)
		for _, task := range tasks {
			if !IsLiveReindexTaskStatus(task.Status) {
				continue
			}
			payload, collection, err := DecodeReindexTaskPayload(task.Payload)
			if err != nil {
				continue
			}
			for _, shardName := range payload.UnitToShard {
				live[shardKey{strings.ToLower(collection), shardName}] = true
			}
		}
		return func(collection, shardName string) bool {
			return live[shardKey{strings.ToLower(collection), shardName}]
		}
	}
}

// BenchmarkReindexGateCapturePass measures one capture pass's gate cost as a
// function of shard count and retained task count. The per-shard arm is what
// the pass cost before the snapshot was hoisted; the per-pass arm is what it
// costs after. builds/pass is reported alongside ns/op because it is exact
// where a wall-clock number on a shared machine is not.
//
// The tasks target a different collection than the pass backs up, which is the
// case that matters: the gate rebuilds the whole table, then answers "free" and
// lets the backup through. A task that does hold the shard refuses the pass
// after one build either way.
func BenchmarkReindexGateCapturePass(b *testing.B) {
	const (
		backedUp = "BenchClass"
		migrated = "OtherClass"
	)

	for _, shardCount := range []int{50, 200} {
		shards := make([]string, shardCount)
		for i := range shards {
			shards[i] = fmt.Sprintf("shard-%d", i)
		}
		// Live tasks, plus the completed ones the cluster keeps for its
		// completed-task TTL — five days by default, so they accumulate.
		for _, tasks := range []struct{ live, retained int }{{1, 0}, {5, 0}, {1, 200}} {
			list := benchTaskList(migrated, shards, tasks.live, tasks.retained)
			name := fmt.Sprintf("shards=%d/live=%d/retained=%d", shardCount, tasks.live, tasks.retained)

			run := func(b *testing.B, passCtx func(*DB) context.Context) {
				logger, _ := logrustest.NewNullLogger()
				var builds atomic.Int64
				db := &DB{logger: logger}
				db.SetShardReindexActivityLookup(benchActivityBuilder(list, &builds))
				b.ResetTimer()
				for range b.N {
					ctx := passCtx(db)
					for _, s := range shards {
						db.AnyLiveReindexForShard(ctx, backedUp, s)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(builds.Load())/float64(b.N), "builds/pass")
			}

			b.Run(name+"/per-shard", func(b *testing.B) {
				run(b, func(*DB) context.Context { return context.Background() })
			})
			b.Run(name+"/per-pass", func(b *testing.B) {
				run(b, func(db *DB) context.Context {
					return db.withReindexGateSnapshot(context.Background())
				})
			})
		}
	}
}
