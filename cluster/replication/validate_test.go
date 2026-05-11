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

// stubSchemaReader is a minimal in-test implementation of the
// validateSchemaReader interface used to exercise
// ValidateReplicationReplicateShard without standing up a full schema.
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

	// Case 1: COPY rejected when target is already in the replica set.
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

	// Case 2: SELF_RECOVERY accepted when target is already a replica
	// (this is the whole point — the on-disk data was lost, the schema
	// still lists this node as a replica, and we want to re-hydrate).
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

	// Case 3: SELF_RECOVERY rejects when target is NOT a replica per
	// schema (logic error somewhere upstream — recovering a node that
	// does not own the shard makes no sense).
	t.Run("self_recovery_rejects_non_replica_target", func(t *testing.T) {
		err := ValidateReplicationReplicateShard(stubSchemaReader{
			classExists: true,
			replicas:    []string{src}, // tgt absent
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

	// Case 4: SELF_RECOVERY rejects when source is missing — even though
	// target-existence is fine, we cannot copy from a peer that does
	// not have the shard.
	t.Run("self_recovery_rejects_missing_source", func(t *testing.T) {
		err := ValidateReplicationReplicateShard(stubSchemaReader{
			classExists: true,
			replicas:    []string{tgt}, // src absent
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

	// Case 5: shared rejections still apply for SELF_RECOVERY (uuid,
	// same source/target).
	t.Run("self_recovery_rejects_same_source_target", func(t *testing.T) {
		err := ValidateReplicationReplicateShard(stubSchemaReader{
			classExists: true,
			replicas:    []string{src},
		}, &api.ReplicationReplicateShardRequest{
			Uuid:             "00000000-0000-0000-0000-000000000005",
			SourceNode:       src,
			TargetNode:       src, // same as source
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
