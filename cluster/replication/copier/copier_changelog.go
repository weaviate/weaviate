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

package copier

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/replication/copier/internal/changelogdrain"
	"github.com/weaviate/weaviate/entities/schema"
)

// StartChangeCapture must be called before the source takes its file snapshot.
func (c *Copier) StartChangeCapture(ctx context.Context, srcNodeId, indexName, shardName, opID string, schemaVersion uint64) error {
	client, err := c.dialSource(ctx, srcNodeId)
	if err != nil {
		return err
	}
	_, err = client.StartChangeCapture(ctx, &protocol.StartChangeCaptureRequest{
		IndexName:     indexName,
		ShardName:     shardName,
		OpId:          opID,
		SchemaVersion: schemaVersion,
	})
	if err != nil {
		return fmt.Errorf("start change capture on %s: %w", srcNodeId, err)
	}
	return nil
}

// TailAndApply streams the source log and replays each entry on the local
// shard. untilLSN is the inclusive upper bound on emitted LSNs; the stream
// closes when lastApplied reaches untilLSN or the source seals the log,
// whichever fires first. Pass a Snapshot LSN to drain a phase boundary
// (the log keeps accepting writes), or finalLSN after FinalizeChangeLog to
// drain the sealed tail. untilLSN=0 is a no-op for a quiet shard. Returns
// on io.EOF, ctx cancel, or fatal recv/apply error.
func (c *Copier) TailAndApply(ctx context.Context, srcNodeId, indexName, shardName, opID string, untilLSN uint64) (lastAppliedLSN uint64, err error) {
	client, err := c.dialSource(ctx, srcNodeId)
	if err != nil {
		return 0, err
	}

	index := c.dbWrapper.GetIndex(schema.ClassName(indexName))
	if index == nil {
		return 0, fmt.Errorf("local index %q not found", indexName)
	}

	stream, err := client.GetChangeLog(ctx, &protocol.GetChangeLogRequest{
		IndexName: indexName,
		ShardName: shardName,
		OpId:      opID,
		UntilLsn:  untilLSN,
	})
	if err != nil {
		return 0, fmt.Errorf("open change-log stream on %s: %w", srcNodeId, err)
	}

	apply := func(ctx context.Context, batch []db.ChangeLogReplayEntry) error {
		return index.OverwriteObjectsFromChangeLog(ctx, shardName, batch)
	}
	return changelogdrain.Drain(ctx, stream, apply)
}

// SnapshotChangeLogLSN returns the source's current change-log LSN without
// sealing it. The log stays writable; pair with a capped TailAndApply to
// drain a phase boundary without sealing.
func (c *Copier) SnapshotChangeLogLSN(ctx context.Context, srcNodeId, indexName, shardName, opID string) (uint64, error) {
	client, err := c.dialSource(ctx, srcNodeId)
	if err != nil {
		return 0, err
	}
	resp, err := client.SnapshotChangeLogLSN(ctx, &protocol.SnapshotChangeLogLSNRequest{
		IndexName: indexName,
		ShardName: shardName,
		OpId:      opID,
	})
	if err != nil {
		return 0, fmt.Errorf("snapshot change-log LSN on %s: %w", srcNodeId, err)
	}
	return resp.Lsn, nil
}

// FinalizeChangeLog seals the source's change-capture log and returns its
// final LSN. The caller need not compare to lastAppliedLSN — the server
// closes the stream with io.EOF once its tailer drains through finalLSN.
func (c *Copier) FinalizeChangeLog(ctx context.Context, srcNodeId, indexName, shardName, opID string) (uint64, error) {
	client, err := c.dialSource(ctx, srcNodeId)
	if err != nil {
		return 0, err
	}
	resp, err := client.FinalizeChangeLog(ctx, &protocol.FinalizeChangeLogRequest{
		IndexName: indexName,
		ShardName: shardName,
		OpId:      opID,
	})
	if err != nil {
		return 0, fmt.Errorf("finalize change log on %s: %w", srcNodeId, err)
	}
	return resp.FinalLsn, nil
}

// StopChangeCapture deactivates the source log and removes its file. Safe to
// call on an unknown opID; the server treats it as a no-op.
func (c *Copier) StopChangeCapture(ctx context.Context, srcNodeId, indexName, shardName, opID string) error {
	client, err := c.dialSource(ctx, srcNodeId)
	if err != nil {
		return err
	}
	_, err = client.StopChangeCapture(ctx, &protocol.StopChangeCaptureRequest{
		IndexName: indexName,
		ShardName: shardName,
		OpId:      opID,
	})
	if err != nil {
		return fmt.Errorf("stop change capture on %s: %w", srcNodeId, err)
	}
	return nil
}

// ReleaseReplicaSnapshot covers the case where CopyReplicaFiles' defer
// never registered because the Create response was lost in transit. Source
// is idempotent on unknown opIDs.
func (c *Copier) ReleaseReplicaSnapshot(ctx context.Context, srcNodeId, indexName, opID string) error {
	client, err := c.dialSource(ctx, srcNodeId)
	if err != nil {
		return err
	}
	_, err = client.ReleaseReplicaSnapshot(ctx, &protocol.ReleaseReplicaSnapshotRequest{
		IndexName: indexName,
		OpId:      opID,
	})
	if err != nil {
		return fmt.Errorf("release replica snapshot on %s: %w", srcNodeId, err)
	}
	return nil
}

func (c *Copier) dialSource(ctx context.Context, srcNodeId string) (FileReplicationServiceClient, error) {
	addr := c.nodeSelector.NodeAddress(srcNodeId)
	port, err := c.nodeSelector.NodeGRPCPort(srcNodeId)
	if err != nil {
		return nil, fmt.Errorf("failed to get gRPC port for source node %s: %w", srcNodeId, err)
	}
	return c.clientFactory(ctx, fmt.Sprintf("%s:%d", addr, port))
}
