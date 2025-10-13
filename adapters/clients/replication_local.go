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

package clients

import (
	"context"
	"net/http"
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// compositeReplicationClient routes to local (in-process) paths when host matches localHostAddr,
// otherwise forwards to the HTTP-based replication client.
type compositeReplicationClient struct {
	httpClient replica.Client
	local      *replica.RemoteReplicaIncoming
	localHost  string
}

// LocalConfigurableReplicationClient exposes a way to wire the local fast-path after construction.
// Callers can type-assert to this interface to configure the local handler safely.
type LocalConfigurableReplicationClient interface {
	replica.Client
	SetLocal(local *replica.RemoteReplicaIncoming, localHostAddr string)
}

func NewCompositeReplicationClient(httpClient *http.Client) LocalConfigurableReplicationClient {
	return &compositeReplicationClient{
		httpClient: NewReplicationClient(httpClient),
	}
}

func (c *compositeReplicationClient) isLocal(host string) bool {
	return c.local != nil && c.localHost != "" && host == c.localHost
}

// SetLocal wires the local fast-path handler and local host address.
// Safe to call once the repo and RemoteReplicaIncoming are initialized.
func (c *compositeReplicationClient) SetLocal(local *replica.RemoteReplicaIncoming, localHostAddr string) {
	c.local = local
	c.localHost = localHostAddr
}

// RClient implementations
func (c *compositeReplicationClient) FetchObject(ctx context.Context, host, index, shard string,
	id strfmt.UUID, selectProps search.SelectProperties, additionalProps additional.Properties, numRetries int,
) (replica.Replica, error) {
	if c.isLocal(host) {
		return c.local.FetchObject(ctx, index, shard, id)
	}
	return c.httpClient.FetchObject(ctx, host, index, shard, id, selectProps, additionalProps, numRetries)
}

func (c *compositeReplicationClient) DigestObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID, numRetries int,
) ([]types.RepairResponse, error) {
	if c.isLocal(host) {
		return c.local.DigestObjects(ctx, index, shard, ids)
	}
	return c.httpClient.DigestObjects(ctx, host, index, shard, ids, numRetries)
}

func (c *compositeReplicationClient) DigestObjectsInRange(ctx context.Context, host, index, shard string,
	initialUUID, finalUUID strfmt.UUID, limit int,
) ([]types.RepairResponse, error) {
	if c.isLocal(host) {
		return c.local.DigestObjectsInRange(ctx, index, shard, initialUUID, finalUUID, limit)
	}
	return c.httpClient.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
}

func (c *compositeReplicationClient) HashTreeLevel(ctx context.Context, host, index, shard string,
	level int, discriminant *hashtree.Bitset,
) (digests []hashtree.Digest, err error) {
	if c.isLocal(host) {
		return c.local.HashTreeLevel(ctx, index, shard, level, discriminant)
	}
	return c.httpClient.HashTreeLevel(ctx, host, index, shard, level, discriminant)
}

func (c *compositeReplicationClient) FetchObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID,
) ([]replica.Replica, error) {
	if c.isLocal(host) {
		return c.local.FetchObjects(ctx, index, shard, ids)
	}
	return c.httpClient.FetchObjects(ctx, host, index, shard, ids)
}

func (c *compositeReplicationClient) OverwriteObjects(ctx context.Context, host, index, shard string,
	vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	if c.isLocal(host) {
		return c.local.OverwriteObjects(ctx, index, shard, vobjects)
	}
	return c.httpClient.OverwriteObjects(ctx, host, index, shard, vobjects)
}

func (c *compositeReplicationClient) FindUUIDs(ctx context.Context, host, index, shard string,
	f *filters.LocalFilter,
) ([]strfmt.UUID, error) {
	// Reads finding UUIDs are only exposed via REST; reuse HTTP even if local.
	return c.httpClient.FindUUIDs(ctx, host, index, shard, f)
}

// WClient implementations
func (c *compositeReplicationClient) PutObject(ctx context.Context, host, index, shard, requestID string,
	obj *storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateObject(ctx, index, shard, requestID, obj, schemaVersion), nil
	}
	return c.httpClient.PutObject(ctx, host, index, shard, requestID, obj, schemaVersion)
}

func (c *compositeReplicationClient) DeleteObject(ctx context.Context, host, index, shard, requestID string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateDeletion(ctx, index, shard, requestID, id, deletionTime, schemaVersion), nil
	}
	return c.httpClient.DeleteObject(ctx, host, index, shard, requestID, id, deletionTime, schemaVersion)
}

func (c *compositeReplicationClient) PutObjects(ctx context.Context, host, index, shard, requestID string,
	objs []*storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateObjects(ctx, index, shard, requestID, objs, schemaVersion), nil
	}
	return c.httpClient.PutObjects(ctx, host, index, shard, requestID, objs, schemaVersion)
}

func (c *compositeReplicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	mergeDoc *objects.MergeDocument, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateUpdate(ctx, index, shard, requestID, mergeDoc, schemaVersion), nil
	}
	return c.httpClient.MergeObject(ctx, host, index, shard, requestID, mergeDoc, schemaVersion)
}

func (c *compositeReplicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateDeletions(ctx, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion), nil
	}
	return c.httpClient.DeleteObjects(ctx, host, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
}

func (c *compositeReplicationClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs []objects.BatchReference, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		return c.local.ReplicateReferences(ctx, index, shard, requestID, refs, schemaVersion), nil
	}
	return c.httpClient.AddReferences(ctx, host, index, shard, requestID, refs, schemaVersion)
}

func (c *compositeReplicationClient) Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error {
	if c.isLocal(host) {
		r := c.local.CommitReplication(index, shard, requestID)
		switch x := resp.(type) {
		case *replica.SimpleResponse:
			if v, ok := r.(replica.SimpleResponse); ok {
				*x = v
			}
		case *replica.DeleteBatchResponse:
			if v, ok := r.(replica.DeleteBatchResponse); ok {
				*x = v
			}
		default:
			// ignore; commit may not have a response body for some ops
		}
		return nil
	}
	return c.httpClient.Commit(ctx, host, index, shard, requestID, resp)
}

func (c *compositeReplicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (replica.SimpleResponse, error) {
	if c.isLocal(host) {
		r := c.local.AbortReplication(index, shard, requestID)
		if v, ok := r.(replica.SimpleResponse); ok {
			return v, nil
		}
		return replica.SimpleResponse{}, nil
	}
	return c.httpClient.Abort(ctx, host, index, shard, requestID)
}
