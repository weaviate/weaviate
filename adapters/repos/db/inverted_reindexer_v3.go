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

// ShardReindexerV3 returns the tasks a loading shard must re-arm, and is told which failed to.
// An error from AbandonUnarmed holds the shard read-only.
type ShardReindexerV3 interface {
	RecoverMigrations(shard *Shard) []*ShardReindexTaskGeneric
	AbandonUnarmed(shard *Shard, unarmed []*ShardReindexTaskGeneric, cause error) error
	// ClaimUnits returns the tasks whose unit it claimed, the ones a teardown holds, and a release.
	ClaimUnits(tasks []*ShardReindexTaskGeneric) (claimed, skipped []*ShardReindexTaskGeneric, release func())
}

// -----------------------------------------------------------------------------

func NewShardReindexerV3Noop() *shardReindexerV3Noop {
	return &shardReindexerV3Noop{}
}

type shardReindexerV3Noop struct{}

func (r *shardReindexerV3Noop) RecoverMigrations(*Shard) []*ShardReindexTaskGeneric {
	return nil
}

func (r *shardReindexerV3Noop) AbandonUnarmed(*Shard, []*ShardReindexTaskGeneric, error) error {
	return nil
}

func (r *shardReindexerV3Noop) ClaimUnits(tasks []*ShardReindexTaskGeneric,
) ([]*ShardReindexTaskGeneric, []*ShardReindexTaskGeneric, func()) {
	return tasks, nil, func() {}
}
