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
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/replica"
)

// createAsyncCheckpoint applies a fan-out create. createdAt must be the
// initiator's timestamp (convergence tie-breaker). Returns nil for shards
// not hosted on this node so a class-wide broadcast can no-op safely.
func (i *Index) createAsyncCheckpoint(ctx context.Context, shardName string, cutoffMs int64, createdAt time.Time) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return fmt.Errorf("get shard %q: %w", shardName, err)
	}
	if shard == nil {
		return nil
	}
	defer release()
	return shard.CreateAsyncCheckpoint(ctx, cutoffMs, createdAt)
}

// createAsyncCheckpointShards is best-effort: errors are joined (preserving
// errors.Is) so REST/gRPC mappers can still classify a sentinel result.
func (i *Index) createAsyncCheckpointShards(ctx context.Context, shardNames []string, cutoffMs int64, createdAt time.Time) error {
	var errs []error
	for _, shardName := range shardNames {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := i.createAsyncCheckpoint(ctx, shardName, cutoffMs, createdAt); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (i *Index) deleteAsyncCheckpointShards(ctx context.Context, shardNames []string) error {
	var errs []error
	for _, shardName := range shardNames {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := i.deleteAsyncCheckpoint(ctx, shardName); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// deleteAsyncCheckpoint is idempotent; returns nil when the shard isn't loaded.
func (i *Index) deleteAsyncCheckpoint(ctx context.Context, shardName string) error {
	shard, release, err := i.GetShard(ctx, shardName)
	if err != nil {
		return fmt.Errorf("get shard %q: %w", shardName, err)
	}
	if shard == nil {
		return nil
	}
	defer release()
	return shard.DeleteAsyncCheckpoint(ctx)
}

// getAsyncCheckpointShardStatus omits shards not loaded here (including
// unloaded LazyLoadShards) so the aggregator can distinguish "not on this
// node" from "loaded but inactive" (CutoffMs == 0).
func (i *Index) getAsyncCheckpointShardStatus(ctx context.Context, shardNames []string) (map[string]replica.AsyncCheckpointShardStatus, error) {
	out := make(map[string]replica.AsyncCheckpointShardStatus, len(shardNames))
	for _, shardName := range shardNames {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		shard, release, err := i.GetShard(ctx, shardName)
		if err != nil {
			return nil, fmt.Errorf("get shard %q: %w", shardName, err)
		}
		if shard == nil {
			continue
		}
		if lazy, ok := shard.(*LazyLoadShard); ok && !lazy.IsAsyncCheckpointHostable() {
			release()
			continue
		}
		root, cutoffMs, createdAt, _ := shard.AsyncCheckpointRoot(ctx)
		release()
		out[shardName] = replica.AsyncCheckpointShardStatus{
			Root:      root,
			CutoffMs:  cutoffMs,
			CreatedAt: createdAt,
		}
	}
	return out, nil
}

// resolveShardNames prefers schemaReader.LocalShards over ForEachShard
// because the in-memory map can lag RAFT during a schema-change race.
func (i *Index) resolveShardNames(shards []string) []string {
	if len(shards) > 0 {
		return shards
	}
	if i.schemaReader != nil {
		if locals, err := i.schemaReader.LocalShards(i.Config.ClassName.String()); err == nil {
			return locals
		}
	}
	var all []string
	_ = i.ForEachShard(func(name string, _ ShardLike) error {
		all = append(all, name)
		return nil
	})
	return all
}

// asyncCheckpointBroadcaster is the fan-out seam; tests substitute a stub.
type asyncCheckpointBroadcaster interface {
	LocalNodeName() string
	BroadcastCreateAsyncCheckpoint(ctx context.Context, shardNames []string, cutoffMs int64, createdAt time.Time) (successes, failures int)
	BroadcastDeleteAsyncCheckpoint(ctx context.Context, shardNames []string) (successes, failures int)
	BroadcastGetAsyncCheckpointStatus(ctx context.Context, shardNames []string) (statuses map[string][]replica.AsyncCheckpointNodeStatus, successes, failures int)
}

// CreateAsyncCheckpoints picks one createdAt for the whole fan-out so every
// replica records the same convergence tie-breaker. Best-effort: per-shard
// failures are logged, but the call returns nil and divergence reconciles
// on the next cycle.
func (i *Index) CreateAsyncCheckpoints(ctx context.Context, cutoffMs int64, shards []string) error {
	return i.createAsyncCheckpoints(ctx, cutoffMs, shards, i.replicator)
}

func (i *Index) createAsyncCheckpoints(ctx context.Context, cutoffMs int64, shards []string, broadcaster asyncCheckpointBroadcaster) error {
	if cutoffMs <= 0 {
		return fmt.Errorf("cutoffMs must be > 0, got %d", cutoffMs)
	}
	targets := i.resolveShardNames(shards)
	createdAt := time.Now().UTC()

	var localSuccesses, localFailures int
	for _, shardName := range targets {
		if err := i.createAsyncCheckpoint(ctx, shardName, cutoffMs, createdAt); err != nil {
			localFailures++
			// Debug, not Warn: "shard not loaded here" is expected for fan-out.
			i.logger.WithFields(logrus.Fields{
				"action": "async_checkpoint_local",
				"op":     "create",
				"class":  i.Config.ClassName,
				"shard":  shardName,
			}).WithError(err).Debug("async-checkpoint local create failed")
			continue
		}
		localSuccesses++
	}

	remoteSuccesses, remoteFailures := broadcaster.BroadcastCreateAsyncCheckpoint(ctx, targets, cutoffMs, createdAt)

	i.logger.WithFields(logrus.Fields{
		"action":           "async_checkpoint",
		"op":               "create",
		"class":            i.Config.ClassName,
		"shards":           len(targets),
		"cutoff_ms":        cutoffMs,
		"created_at":       createdAt,
		"local_successes":  localSuccesses,
		"local_failures":   localFailures,
		"remote_successes": remoteSuccesses,
		"remote_failures":  remoteFailures,
	}).Info("async-checkpoint create completed")
	return nil
}

func (i *Index) DeleteAsyncCheckpoints(ctx context.Context, shards []string) error {
	return i.deleteAsyncCheckpoints(ctx, shards, i.replicator)
}

func (i *Index) deleteAsyncCheckpoints(ctx context.Context, shards []string, broadcaster asyncCheckpointBroadcaster) error {
	targets := i.resolveShardNames(shards)
	var localSuccesses, localFailures int
	for _, shardName := range targets {
		if err := i.deleteAsyncCheckpoint(ctx, shardName); err != nil {
			localFailures++
			i.logger.WithFields(logrus.Fields{
				"action": "async_checkpoint_local",
				"op":     "delete",
				"class":  i.Config.ClassName,
				"shard":  shardName,
			}).WithError(err).Debug("local async-checkpoint delete failed")
			continue
		}
		localSuccesses++
	}
	remoteSuccesses, remoteFailures := broadcaster.BroadcastDeleteAsyncCheckpoint(ctx, targets)
	i.logger.WithFields(logrus.Fields{
		"action":           "async_checkpoint",
		"op":               "delete",
		"class":            i.Config.ClassName,
		"shards":           len(targets),
		"local_successes":  localSuccesses,
		"local_failures":   localFailures,
		"remote_successes": remoteSuccesses,
		"remote_failures":  remoteFailures,
	}).Info("async-checkpoint delete completed")
	return nil
}

func (i *Index) GetAsyncCheckpointStatus(ctx context.Context, shards []string) (map[string][]replica.AsyncCheckpointNodeStatus, error) {
	return i.getAsyncCheckpointStatus(ctx, shards, i.replicator)
}

func (i *Index) getAsyncCheckpointStatus(ctx context.Context, shards []string, broadcaster asyncCheckpointBroadcaster) (map[string][]replica.AsyncCheckpointNodeStatus, error) {
	targets := i.resolveShardNames(shards)

	out, remoteSuccesses, remoteFailures := broadcaster.BroadcastGetAsyncCheckpointStatus(ctx, targets)

	localStatuses, err := i.getAsyncCheckpointShardStatus(ctx, targets)
	if err != nil {
		return nil, fmt.Errorf("get local checkpoint status: %w", err)
	}
	localNode := broadcaster.LocalNodeName()
	for shardName, s := range localStatuses {
		out[shardName] = append(out[shardName], replica.AsyncCheckpointNodeStatus{
			Node:      localNode,
			CutoffMs:  s.CutoffMs,
			CreatedAt: s.CreatedAt,
			Root:      s.Root,
		})
	}

	i.logger.WithFields(logrus.Fields{
		"action":           "async_checkpoint",
		"op":               "status",
		"class":            i.Config.ClassName,
		"shards":           len(targets),
		"local_present":    len(localStatuses),
		"remote_successes": remoteSuccesses,
		"remote_failures":  remoteFailures,
	}).Info("async-checkpoint status completed")
	return out, nil
}
