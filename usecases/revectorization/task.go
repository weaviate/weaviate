//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package revectorization

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/boltdb/bolt"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

type executableTask struct {
	ctx      context.Context
	cancelFn context.CancelFunc

	distTask   *distributedtask.Task
	payload    TaskPayload
	localState LocalTaskState

	index *db.Index
	p     *Provider
}

func newTask(p *Provider, distTask *distributedtask.Task) (*executableTask, error) {
	var payload TaskPayload
	if err := json.Unmarshal(distTask.Payload, &payload); err != nil {
		return nil, fmt.Errorf("unmarshalling payload: %w", err)
	}

	index := p.db.GetIndex(schema.ClassName(payload.CollectionName))
	if index == nil {
		return nil, fmt.Errorf("could not find collection %q", payload.CollectionName)
	}

	localState, err := boltView(p.metadataDB, func(b *bolt.Bucket) (LocalTaskState, error) {
		bytes := b.Get([]byte(distTask.TaskDescriptor.String()))
		if bytes == nil {
			return LocalTaskState{
				TaskDescriptor: distTask.TaskDescriptor,
				StartedAt:      p.clock.Now(),
			}, nil
		}

		var task LocalTaskState
		if err := json.Unmarshal(bytes, &task); err != nil {
			return LocalTaskState{}, fmt.Errorf("unmarshalling localState task: %w", err)
		}
		return task, nil
	})
	if err != nil {
		return nil, err
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	return &executableTask{
		ctx: ctx,

		distTask:   distTask,
		payload:    payload,
		localState: localState,

		cancelFn: cancelFn,
		index:    index,
		p:        p,
	}, nil
}

func (t *executableTask) Run() {
	for {
		cont, err := t.processNextShard()
		if err != nil {
			t.recordFailure(err)
			return
		}

		if !cont {
			t.recordCompletion()
			return
		}

		t.p.clock.Sleep(time.Second) // TODO: control with config

		select {
		case <-t.ctx.Done():
		default:
		}
	}
}

func (t *executableTask) processNextShard() (bool, error) {
	shardName, ok := t.selectNextShardName()
	if !ok {
		// no more shards to process
		return false, nil
	}

	shard, release, err := t.index.GetShard(t.ctx, shardName)
	if err != nil {
		return false, err
	}
	defer release()

	var (
		batch                 = make([]*models.Object, 0, 500) // TODO: control with config
		selfID, numOfReplicas = t.resolveReplicationID(shardName)
	)

	cursor := shard.CursorReplace()
	defer cursor.Close()

	var stats persistStats
	for {
		batch = batch[:0]
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			object, err := storobj.FromBinary(v)
			if err != nil {
				return false, err
			}

			if t.payload.TargetVector != "" && len(object.Vectors[t.payload.TargetVector]) > 0 {
				continue
			}

			if object.DocID%uint64(numOfReplicas) != uint64(selfID) {
				// other nodes will process this object
				continue
			}

			batch = append(batch, toModelsObject(object))
			if len(batch) == cap(batch) {
				break
			}
		}

		batchStats, err := t.persistBatch(batch)
		stats = stats.Merge(batchStats)
		if err != nil {
			return false, err
		}

		if len(batch) < cap(batch) {
			break
		}
	}

	t.localState.ProcessedShards = append(t.localState.ProcessedShards, processedShard{
		Name:       shardName,
		FinishedAt: t.p.clock.Now(),
		Stats:      stats,
	})
	t.localState.Stats = t.localState.Stats.Merge(stats)

	if err = t.persistLocalTaskState(); err != nil {
		return false, fmt.Errorf("failed to sync localState data: %w", err)
	}

	return true, nil
}

func (t *executableTask) selectNextShardName() (string, bool) {
	var selectedShardName string
	_ = t.index.ForEachShard(func(name string, shard db.ShardLike) error {
		if selectedShardName != "" {
			// TODO: currently there is no way to terminate iteration of ForEachShard early
			return nil
		}

		if t.excludeShardFilter(name) {
			return nil
		}

		alreadyProcessed := exists(t.localState.ProcessedShards, func(shard processedShard) bool {
			return shard.Name == name
		})
		if alreadyProcessed {
			return nil
		}

		selectedShardName = name
		return nil
	})
	return selectedShardName, selectedShardName != ""
}

func (t *executableTask) excludeShardFilter(shardName string) bool {
	// TODO: add shard filter based on specified tenants
	return true
}

// TODO: come up with a better name
func (t *executableTask) resolveReplicationID(shardName string) (self int, n int) {
	replicas, err := t.p.schemaGetter.ShardReplicas(t.payload.CollectionName, shardName)
	if err != nil {
		panic(err)
	}

	if len(replicas) == 0 {
		return 0, 1
	}

	sort.Strings(replicas)

	index := slices.Index(replicas, t.p.localNode)
	if index == -1 {
		panic("local node is not included into replicas list")
	}
	return index, len(replicas) - 1
}

type persistStats struct {
	Processed uint64 `json:"processed"`
	Failed    uint64 `json:"failed"`
}

func (s persistStats) Merge(other persistStats) persistStats {
	return persistStats{
		Processed: s.Processed + other.Processed,
		Failed:    s.Failed + other.Failed,
	}
}

func (t *executableTask) persistBatch(batch []*models.Object) (persistStats, error) {
	objs, err := t.p.batchManager.AddObjects(t.ctx, nil, batch, nil, &additional.ReplicationProperties{
		ConsistencyLevel: "QUORUM", // TODO: think about this more
	})
	if err != nil {
		return persistStats{}, fmt.Errorf("persisting batch: %w", err)
	}

	var stats persistStats
	for _, obj := range objs {
		stats.Processed++
		if obj.Err == nil {
			return persistStats{}, nil
		}
		stats.Failed++

		t.localState.ObjectErrors = append(t.localState.ObjectErrors, ObjectError{
			ObjectUUID: obj.UUID.String(),
			Err:        obj.Err.Error(),
		})

		if t.payload.MaximumErrorsPerNode < len(t.localState.ObjectErrors) {
			return persistStats{}, newTerminalError(fmt.Errorf("maximum error per node exceeded: %d. Last failure: %s", t.payload.MaximumErrorsPerNode, t.localState.ObjectErrors[len(t.localState.ObjectErrors)-1].Error()))
		}
	}

	return persistStats{}, err
}

func (t *executableTask) Terminate() {
	t.cancelFn()
}

func (t *executableTask) recordCompletion() {
	if err := t.p.completionRecorder.RecordDistributedTaskNodeCompletion(t.ctx, DistributedTasksNamespace, t.localState.ID, t.localState.Version); err != nil {
		panic(err)
	}
}

func (t *executableTask) recordFailure(err error) {
	if err := t.p.recorder.RecordDistributedTaskNodeFailure(t.ctx, DistributedTasksNamespace, t.localState.ID, t.localState.Version, err.Error()); err != nil {
		panic(err)
	}
}

func (t *executableTask) persistLocalTaskState() error {
	bytes, err := json.Marshal(t.localState)
	if err != nil {
		return err
	}

	return t.p.metadataDB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		return bucket.Put([]byte(t.localState.TaskDescriptor.String()), bytes)
	})
}

func toModelsObject(obj *storobj.Object) *models.Object {
	modelObj := &models.Object{}

	// TODO: do the mapping

	return modelObj
}

func exists[T any](slice []T, predicate func(T) bool) bool {
	for _, item := range slice {
		if predicate(item) {
			return true
		}
	}
	return false
}

type terminalErr struct {
	reason error
}

func (e terminalErr) Error() string {
	return e.reason.Error()
}

func newTerminalError(reason error) terminalErr {
	return terminalErr{reason: reason}
}
