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
)

type ShardReindexerV3 interface {
	RunBeforeLsmInit(ctx context.Context, shard *Shard) error
	RunAfterLsmInit(ctx context.Context, shard *Shard) error
	RunAfterLsmInitAsync(ctx context.Context, shard *Shard) error
	Stop(shard *Shard, cause error)
}

// -----------------------------------------------------------------------------

func NewShardReindexerV3Noop() *shardReindexerV3Noop {
	return &shardReindexerV3Noop{}
}

type shardReindexerV3Noop struct{}

func (r *shardReindexerV3Noop) RunBeforeLsmInit(ctx context.Context, shard *Shard) error {
	return nil
}

func (r *shardReindexerV3Noop) RunAfterLsmInit(ctx context.Context, shard *Shard) error {
	return nil
}

func (r *shardReindexerV3Noop) RunAfterLsmInitAsync(ctx context.Context, shard *Shard) error {
	return nil
}

func (r *shardReindexerV3Noop) Stop(shard *Shard, cause error) {}
