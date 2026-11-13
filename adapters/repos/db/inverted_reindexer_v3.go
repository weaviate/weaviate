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

import "github.com/weaviate/weaviate/usecases/logrusext"

// ShardReindexerV3 recovers a loading shard's migrations. RecoverMigrations returns the tasks to re-arm.
// AbandonUnarmed stamps Unmirrored those that failed, and its error holds the shard READONLY with the tasks it returns.
// ClaimUnits keeps a teardown off each claimed task's unit until release runs, and returns in skipped the tasks a teardown holds.
// UnbuildableFaults samples the lines about records no task can be built from; nil means unsampled.
type ShardReindexerV3 interface {
	RecoverMigrations(shard *Shard) []*ShardReindexTaskGeneric
	AbandonUnarmed(shard *Shard, unarmed []*ShardReindexTaskGeneric, cause error) ([]*ShardReindexTaskGeneric, error)
	ClaimUnits(tasks []*ShardReindexTaskGeneric) (claimed, skipped []*ShardReindexTaskGeneric, release func())
	UnbuildableFaults() *logrusext.Sampler
}

// -----------------------------------------------------------------------------

func NewShardReindexerV3Noop() *shardReindexerV3Noop {
	return &shardReindexerV3Noop{}
}

type shardReindexerV3Noop struct{}

func (r *shardReindexerV3Noop) RecoverMigrations(*Shard) []*ShardReindexTaskGeneric {
	return nil
}

func (r *shardReindexerV3Noop) AbandonUnarmed(*Shard, []*ShardReindexTaskGeneric, error) ([]*ShardReindexTaskGeneric, error) {
	return nil, nil
}

func (r *shardReindexerV3Noop) ClaimUnits(tasks []*ShardReindexTaskGeneric,
) ([]*ShardReindexTaskGeneric, []*ShardReindexTaskGeneric, func()) {
	return tasks, nil, func() {}
}

func (r *shardReindexerV3Noop) UnbuildableFaults() *logrusext.Sampler {
	return nil
}
