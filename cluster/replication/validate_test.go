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

package replication

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
)

type stubSchemaReader struct {
	classExists bool
	replicas    []string
	shardErr    error
}

func (s stubSchemaReader) ClassInfo(class string) schema.ClassInfo {
	return schema.ClassInfo{Exists: s.classExists}
}

func (s stubSchemaReader) ShardReplicas(class, shard string) ([]string, error) {
	return s.replicas, s.shardErr
}

func TestValidateReplicationReplicateShard(t *testing.T) {
	const (
		coll = "C"
		sh   = "S"
		src  = "node-A"
		tgt  = "node-B"
	)

	t.Run("copy_rejects_existing_target", func(t *testing.T) {
		err := ValidateReplicationReplicateShard(stubSchemaReader{
			classExists: true,
			replicas:    []string{src, tgt},
		}, &api.ReplicationReplicateShardRequest{
			Uuid:             "00000000-0000-0000-0000-000000000001",
			SourceNode:       src,
			TargetNode:       tgt,
			SourceCollection: coll,
			SourceShard:      sh,
			TransferType:     api.COPY.String(),
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrAlreadyExists), "expected ErrAlreadyExists, got %v", err)
	})

	t.Run("self_recovery_accepts_existing_target", func(t *testing.T) {
		err := ValidateReplicationReplicateShard(stubSchemaReader{
			classExists: true,
			replicas:    []string{src, tgt},
		}, &api.ReplicationReplicateShardRequest{
			Uuid:             "00000000-0000-0000-0000-000000000002",
			SourceNode:       src,
			TargetNode:       tgt,
			SourceCollection: coll,
			SourceShard:      sh,
			TransferType:     api.SELF_RECOVERY.String(),
		})
		require.NoError(t, err)
	})

	t.Run("self_recovery_rejects_non_replica_target", func(t *testing.T) {
		err := ValidateReplicationReplicateShard(stubSchemaReader{
			classExists: true,
			replicas:    []string{src},
		}, &api.ReplicationReplicateShardRequest{
			Uuid:             "00000000-0000-0000-0000-000000000003",
			SourceNode:       src,
			TargetNode:       tgt,
			SourceCollection: coll,
			SourceShard:      sh,
			TransferType:     api.SELF_RECOVERY.String(),
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrNodeNotFound), "expected ErrNodeNotFound, got %v", err)
	})

	t.Run("self_recovery_rejects_missing_source", func(t *testing.T) {
		err := ValidateReplicationReplicateShard(stubSchemaReader{
			classExists: true,
			replicas:    []string{tgt},
		}, &api.ReplicationReplicateShardRequest{
			Uuid:             "00000000-0000-0000-0000-000000000004",
			SourceNode:       src,
			TargetNode:       tgt,
			SourceCollection: coll,
			SourceShard:      sh,
			TransferType:     api.SELF_RECOVERY.String(),
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrNodeNotFound), "expected ErrNodeNotFound, got %v", err)
	})

	t.Run("self_recovery_rejects_same_source_target", func(t *testing.T) {
		err := ValidateReplicationReplicateShard(stubSchemaReader{
			classExists: true,
			replicas:    []string{src},
		}, &api.ReplicationReplicateShardRequest{
			Uuid:             "00000000-0000-0000-0000-000000000005",
			SourceNode:       src,
			TargetNode:       src,
			SourceCollection: coll,
			SourceShard:      sh,
			TransferType:     api.SELF_RECOVERY.String(),
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrBadRequest), "expected ErrBadRequest, got %v", err)
	})

	t.Run("self_recovery_rejects_missing_uuid", func(t *testing.T) {
		err := ValidateReplicationReplicateShard(stubSchemaReader{
			classExists: true,
			replicas:    []string{src, tgt},
		}, &api.ReplicationReplicateShardRequest{
			Uuid:             "",
			SourceNode:       src,
			TargetNode:       tgt,
			SourceCollection: coll,
			SourceShard:      sh,
			TransferType:     api.SELF_RECOVERY.String(),
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrBadRequest), "expected ErrBadRequest, got %v", err)
	})

	t.Run("rejects_unknown_class", func(t *testing.T) {
		err := ValidateReplicationReplicateShard(stubSchemaReader{
			classExists: false,
		}, &api.ReplicationReplicateShardRequest{
			Uuid:             "00000000-0000-0000-0000-000000000006",
			SourceNode:       src,
			TargetNode:       tgt,
			SourceCollection: coll,
			SourceShard:      sh,
			TransferType:     api.SELF_RECOVERY.String(),
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrClassNotFound), "expected ErrClassNotFound, got %v", err)
	})
}
