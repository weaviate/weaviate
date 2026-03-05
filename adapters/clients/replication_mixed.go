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

package clients

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// hybridReplicationClient tries gRPC first; if the target node returns
// codes.Unimplemented (old node without the replication service), it
// transparently falls back to REST.
type hybridReplicationClient struct {
	grpcClient *grpcReplicationClient
	restClient *replicationClient
}

// NewHybridReplicationClient creates a client that always attempts gRPC first
// and falls back to REST on Unimplemented errors.
func NewHybridReplicationClient(
	grpcClient *grpcReplicationClient,
	restClient *replicationClient,
) *hybridReplicationClient {
	return &hybridReplicationClient{
		grpcClient: grpcClient,
		restClient: restClient,
	}
}

// isUnimplemented returns true when the error represents a gRPC
// codes.Unimplemented status, indicating the remote node does not
// support the replication gRPC service.
func isUnimplemented(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unimplemented
}

// ── Write operations (WClient) ───────────────────────────────────────────────

func (h *hybridReplicationClient) PutObject(ctx context.Context, host, index, shard, requestID string,
	obj *storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	resp, err := h.grpcClient.PutObject(ctx, host, index, shard, requestID, obj, schemaVersion)
	if isUnimplemented(err) {
		return h.restClient.PutObject(ctx, host, index, shard, requestID, obj, schemaVersion)
	}
	return resp, err
}

func (h *hybridReplicationClient) PutObjects(ctx context.Context, host, index, shard, requestID string,
	objs []*storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	resp, err := h.grpcClient.PutObjects(ctx, host, index, shard, requestID, objs, schemaVersion)
	if isUnimplemented(err) {
		return h.restClient.PutObjects(ctx, host, index, shard, requestID, objs, schemaVersion)
	}
	return resp, err
}

func (h *hybridReplicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	doc *objects.MergeDocument, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	resp, err := h.grpcClient.MergeObject(ctx, host, index, shard, requestID, doc, schemaVersion)
	if isUnimplemented(err) {
		return h.restClient.MergeObject(ctx, host, index, shard, requestID, doc, schemaVersion)
	}
	return resp, err
}

func (h *hybridReplicationClient) DeleteObject(ctx context.Context, host, index, shard, requestID string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	resp, err := h.grpcClient.DeleteObject(ctx, host, index, shard, requestID, id, deletionTime, schemaVersion)
	if isUnimplemented(err) {
		return h.restClient.DeleteObject(ctx, host, index, shard, requestID, id, deletionTime, schemaVersion)
	}
	return resp, err
}

func (h *hybridReplicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	resp, err := h.grpcClient.DeleteObjects(ctx, host, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
	if isUnimplemented(err) {
		return h.restClient.DeleteObjects(ctx, host, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
	}
	return resp, err
}

func (h *hybridReplicationClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs []objects.BatchReference, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	resp, err := h.grpcClient.AddReferences(ctx, host, index, shard, requestID, refs, schemaVersion)
	if isUnimplemented(err) {
		return h.restClient.AddReferences(ctx, host, index, shard, requestID, refs, schemaVersion)
	}
	return resp, err
}

func (h *hybridReplicationClient) Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error {
	err := h.grpcClient.Commit(ctx, host, index, shard, requestID, resp)
	if isUnimplemented(err) {
		return h.restClient.Commit(ctx, host, index, shard, requestID, resp)
	}
	return err
}

func (h *hybridReplicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (replica.SimpleResponse, error) {
	resp, err := h.grpcClient.Abort(ctx, host, index, shard, requestID)
	if isUnimplemented(err) {
		return h.restClient.Abort(ctx, host, index, shard, requestID)
	}
	return resp, err
}

// ── Read operations (RClient) ────────────────────────────────────────────────

func (h *hybridReplicationClient) FetchObject(ctx context.Context, host, index, shard string,
	id strfmt.UUID, props search.SelectProperties, additional additional.Properties, numRetries int,
) (replica.Replica, error) {
	resp, err := h.grpcClient.FetchObject(ctx, host, index, shard, id, props, additional, numRetries)
	if isUnimplemented(err) {
		return h.restClient.FetchObject(ctx, host, index, shard, id, props, additional, numRetries)
	}
	return resp, err
}

func (h *hybridReplicationClient) FetchObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID,
) ([]replica.Replica, error) {
	resp, err := h.grpcClient.FetchObjects(ctx, host, index, shard, ids)
	if isUnimplemented(err) {
		return h.restClient.FetchObjects(ctx, host, index, shard, ids)
	}
	return resp, err
}

func (h *hybridReplicationClient) DigestObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID, numRetries int,
) ([]types.RepairResponse, error) {
	resp, err := h.grpcClient.DigestObjects(ctx, host, index, shard, ids, numRetries)
	if isUnimplemented(err) {
		return h.restClient.DigestObjects(ctx, host, index, shard, ids, numRetries)
	}
	return resp, err
}

func (h *hybridReplicationClient) DigestObjectsInRange(ctx context.Context, host, index, shard string,
	initialUUID, finalUUID strfmt.UUID, limit int,
) ([]types.RepairResponse, error) {
	resp, err := h.grpcClient.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
	if isUnimplemented(err) {
		return h.restClient.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
	}
	return resp, err
}

func (h *hybridReplicationClient) OverwriteObjects(ctx context.Context, host, index, shard string,
	vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	resp, err := h.grpcClient.OverwriteObjects(ctx, host, index, shard, vobjects)
	if isUnimplemented(err) {
		return h.restClient.OverwriteObjects(ctx, host, index, shard, vobjects)
	}
	return resp, err
}

func (h *hybridReplicationClient) FindUUIDs(ctx context.Context, host, index, shard string,
	filter *filters.LocalFilter, limit int,
) ([]strfmt.UUID, error) {
	resp, err := h.grpcClient.FindUUIDs(ctx, host, index, shard, filter, limit)
	if isUnimplemented(err) {
		return h.restClient.FindUUIDs(ctx, host, index, shard, filter, limit)
	}
	return resp, err
}

func (h *hybridReplicationClient) HashTreeLevel(ctx context.Context, host, index, shard string,
	level int, discriminant *hashtree.Bitset,
) ([]hashtree.Digest, error) {
	resp, err := h.grpcClient.HashTreeLevel(ctx, host, index, shard, level, discriminant)
	if isUnimplemented(err) {
		return h.restClient.HashTreeLevel(ctx, host, index, shard, level, discriminant)
	}
	return resp, err
}
