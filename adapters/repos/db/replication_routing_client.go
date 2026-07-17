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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// routingReplicationClient serves replica calls targeting the local node
// in-process via *DB, avoiding a loopback HTTP/gRPC round-trip, and delegates
// every other host to the wrapped network client.
type routingReplicationClient struct {
	remote        replica.Client
	local         *DB
	resolver      cluster.NodeResolver
	localNodeName string
	cachedHost    atomic.Pointer[string]
}

var _ replica.Client = (*routingReplicationClient)(nil)

// newRoutingReplicationClient serves local-node targets in-process, delegating
// everything else to remote.
func newRoutingReplicationClient(remote replica.Client, local *DB,
	resolver cluster.NodeResolver, localNodeName string,
) *routingReplicationClient {
	return &routingReplicationClient{
		remote:        remote,
		local:         local,
		resolver:      resolver,
		localNodeName: localNodeName,
	}
}

// localHost returns the local node's address, or "" if not yet resolvable
// (callers then fall back to the network). Memoised on first success.
func (c *routingReplicationClient) localHost() string {
	if p := c.cachedHost.Load(); p != nil {
		return *p
	}
	host, ok := c.resolver.NodeHostname(c.localNodeName)
	if !ok || host == "" {
		return "" // not resolvable yet; don't cache, retry next call
	}
	c.cachedHost.Store(&host)
	return host
}

// isLocal reports whether host is the local node. The comparison is exact:
// host and localHost both come from NodeResolver.NodeHostname. An empty or
// unresolved address yields false, safely falling back to the network.
func (c *routingReplicationClient) isLocal(host string) bool {
	if host == "" {
		return false
	}
	return host == c.localHost()
}

// ── Read operations (RClient) ────────────────────────────────────────────────

func (c *routingReplicationClient) FetchObject(ctx context.Context, host, index, shard string,
	id strfmt.UUID, props search.SelectProperties, addl additional.Properties, numRetries int,
) (replica.Replica, error) {
	if c.isLocal(host) {
		return c.local.FetchObject(ctx, index, shard, id)
	}
	return c.remote.FetchObject(ctx, host, index, shard, id, props, addl, numRetries)
}

func (c *routingReplicationClient) FetchObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID,
) ([]replica.Replica, error) {
	if c.isLocal(host) {
		return c.local.FetchObjects(ctx, index, shard, ids)
	}
	return c.remote.FetchObjects(ctx, host, index, shard, ids)
}

func (c *routingReplicationClient) OverwriteObjects(ctx context.Context, host, index, shard string,
	vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	if c.isLocal(host) {
		return c.local.OverwriteObjects(ctx, index, shard, vobjects)
	}
	return c.remote.OverwriteObjects(ctx, host, index, shard, vobjects)
}

func (c *routingReplicationClient) DigestObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID, numRetries int,
) ([]types.RepairResponse, error) {
	if c.isLocal(host) {
		return c.local.DigestObjects(ctx, index, shard, ids)
	}
	return c.remote.DigestObjects(ctx, host, index, shard, ids, numRetries)
}

func (c *routingReplicationClient) DigestObjectsInRange(ctx context.Context, host, index, shard string,
	initialUUID, finalUUID strfmt.UUID, limit int,
) ([]types.RepairResponse, error) {
	if c.isLocal(host) {
		return c.local.DigestObjectsInRange(ctx, index, shard, initialUUID, finalUUID, limit)
	}
	return c.remote.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
}

func (c *routingReplicationClient) CompareDigests(ctx context.Context, host, index, shard string,
	digests []types.RepairResponse,
) ([]types.RepairResponse, error) {
	if c.isLocal(host) {
		return c.local.CompareDigests(ctx, index, shard, digests)
	}
	return c.remote.CompareDigests(ctx, host, index, shard, digests)
}

func (c *routingReplicationClient) HashTreeLevel(ctx context.Context, host, index, shard string,
	level int, discriminant *hashtree.Bitset,
) ([]hashtree.Digest, error) {
	if c.isLocal(host) {
		return c.local.HashTreeLevel(ctx, index, shard, level, discriminant)
	}
	return c.remote.HashTreeLevel(ctx, host, index, shard, level, discriminant)
}

func (c *routingReplicationClient) FindUUIDs(ctx context.Context, host, index, shard string,
	filter *filters.LocalFilter, limit int,
) ([]strfmt.UUID, error) {
	if c.isLocal(host) {
		return c.local.FindUUIDs(ctx, index, shard, filter, limit)
	}
	return c.remote.FindUUIDs(ctx, host, index, shard, filter, limit)
}

func (c *routingReplicationClient) CountObjects(ctx context.Context, host, index, shard string) (int, error) {
	if c.isLocal(host) {
		return c.local.CountObjects(ctx, index, shard)
	}
	return c.remote.CountObjects(ctx, host, index, shard)
}

func (c *routingReplicationClient) GetAsyncCheckpointStatus(ctx context.Context, host, index string,
	shardNames []string,
) (map[string]replica.AsyncCheckpointShardStatus, error) {
	if c.isLocal(host) {
		return c.local.GetAsyncCheckpointStatus(ctx, index, shardNames)
	}
	return c.remote.GetAsyncCheckpointStatus(ctx, host, index, shardNames)
}

func (c *routingReplicationClient) CreateAsyncCheckpoint(ctx context.Context, host, index string,
	shardNames []string, cutoffMs int64, createdAt time.Time,
) error {
	if c.isLocal(host) {
		return c.local.CreateAsyncCheckpoint(ctx, index, shardNames, cutoffMs, createdAt)
	}
	return c.remote.CreateAsyncCheckpoint(ctx, host, index, shardNames, cutoffMs, createdAt)
}

func (c *routingReplicationClient) DeleteAsyncCheckpoint(ctx context.Context, host, index string,
	shardNames []string,
) error {
	if c.isLocal(host) {
		return c.local.DeleteAsyncCheckpoint(ctx, index, shardNames)
	}
	return c.remote.DeleteAsyncCheckpoint(ctx, host, index, shardNames)
}

// ── Write operations (WClient) ───────────────────────────────────────────────

func (c *routingReplicationClient) PutObject(ctx context.Context, host, index, shard, requestID string,
	obj *storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateObject(ctx, index, shard, requestID, obj, schemaVersion), nil
	}
	return c.remote.PutObject(ctx, host, index, shard, requestID, obj, schemaVersion)
}

func (c *routingReplicationClient) PutObjects(ctx context.Context, host, index, shard, requestID string,
	objs []*storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateObjects(ctx, index, shard, requestID, objs, schemaVersion), nil
	}
	return c.remote.PutObjects(ctx, host, index, shard, requestID, objs, schemaVersion)
}

func (c *routingReplicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	doc *objects.MergeDocument, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateUpdate(ctx, index, shard, requestID, doc, schemaVersion), nil
	}
	return c.remote.MergeObject(ctx, host, index, shard, requestID, doc, schemaVersion)
}

func (c *routingReplicationClient) DeleteObject(ctx context.Context, host, index, shard, requestID string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateDeletion(ctx, index, shard, requestID, id, deletionTime, schemaVersion), nil
	}
	return c.remote.DeleteObject(ctx, host, index, shard, requestID, id, deletionTime, schemaVersion)
}

func (c *routingReplicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateDeletions(ctx, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion), nil
	}
	return c.remote.DeleteObjects(ctx, host, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
}

func (c *routingReplicationClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs []objects.BatchReference, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateReferences(ctx, index, shard, requestID, refs, schemaVersion), nil
	}
	return c.remote.AddReferences(ctx, host, index, shard, requestID, refs, schemaVersion)
}

func (c *routingReplicationClient) Commit(ctx context.Context, host, index, shard, requestID string, resp any) error {
	if c.isLocal(host) {
		return assignCommitResponse(c.local.CommitReplication(ctx, index, shard, requestID), resp, requestID)
	}
	return c.remote.Commit(ctx, host, index, shard, requestID, resp)
}

func (c *routingReplicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return toSimpleResponse(c.local.AbortReplication(ctx, index, shard, requestID))
	}
	return c.remote.Abort(ctx, host, index, shard, requestID)
}

func (c *routingReplicationClient) CompareHashTreeRoots(ctx context.Context, host, index string,
	roots map[string]hashtree.Digest,
) (divergingShards []string, err error) {
	if c.isLocal(host) {
		return c.local.CompareHashTreeRoots(ctx, index, roots)
	}
	return c.remote.CompareHashTreeRoots(ctx, host, index, roots)
}

// assignCommitResponse copies a local CommitReplication result into the caller's
// container (*replica.SimpleResponse for most writes, *replica.DeleteBatchResponse
// for DeleteObjects). A nil result means the request id was not found.
func assignCommitResponse(result any, resp any, requestID string) error {
	if result == nil {
		return fmt.Errorf("commit: request %q not found", requestID)
	}
	switch target := resp.(type) {
	case *replica.SimpleResponse:
		sr, err := toSimpleResponse(result)
		if err != nil {
			return err
		}
		*target = sr
	case *replica.DeleteBatchResponse:
		// The delete path returns a DeleteBatchResponse on success, but a
		// SimpleResponse when a startup/class/shard precheck fails.
		switch v := result.(type) {
		case replica.DeleteBatchResponse:
			*target = v
		case replica.SimpleResponse:
			if err := v.FirstError(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("commit: unexpected local result %T for delete batch", result)
		}
	default:
		return fmt.Errorf("commit: unexpected response container type %T", resp)
	}
	return nil
}

// toSimpleResponse narrows the any returned by Abort/CommitReplication; anything
// other than a SimpleResponse is a bug.
func toSimpleResponse(result any) (replica.SimpleResponse, error) {
	switch v := result.(type) {
	case replica.SimpleResponse:
		return v, nil
	case *replica.SimpleResponse:
		return *v, nil
	default:
		return replica.SimpleResponse{}, fmt.Errorf("unexpected local replication result type %T", result)
	}
}
