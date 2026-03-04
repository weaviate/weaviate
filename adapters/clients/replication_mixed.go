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

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// switchableReplicationClient delegates to either a gRPC or REST client
// based on a runtime config check. No fallback, no caching — the useGRPC
// closure reads a DynamicValue[bool] per call, enabling runtime switching.
type switchableReplicationClient struct {
	grpcClient *grpcReplicationClient
	restClient *replicationClient
	useGRPC    func() bool
}

// NewSwitchableReplicationClient creates a client that delegates to gRPC
// when useGRPC() returns true, otherwise to REST.
func NewSwitchableReplicationClient(
	grpcClient *grpcReplicationClient,
	restClient *replicationClient,
	useGRPC func() bool,
) *switchableReplicationClient {
	return &switchableReplicationClient{
		grpcClient: grpcClient,
		restClient: restClient,
		useGRPC:    useGRPC,
	}
}

// ── Write operations (WClient) ───────────────────────────────────────────────

func (s *switchableReplicationClient) PutObject(ctx context.Context, host, index, shard, requestID string,
	obj *storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.PutObject(ctx, host, index, shard, requestID, obj, schemaVersion)
	}
	return s.restClient.PutObject(ctx, host, index, shard, requestID, obj, schemaVersion)
}

func (s *switchableReplicationClient) PutObjects(ctx context.Context, host, index, shard, requestID string,
	objs []*storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.PutObjects(ctx, host, index, shard, requestID, objs, schemaVersion)
	}
	return s.restClient.PutObjects(ctx, host, index, shard, requestID, objs, schemaVersion)
}

func (s *switchableReplicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	doc *objects.MergeDocument, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.MergeObject(ctx, host, index, shard, requestID, doc, schemaVersion)
	}
	return s.restClient.MergeObject(ctx, host, index, shard, requestID, doc, schemaVersion)
}

func (s *switchableReplicationClient) DeleteObject(ctx context.Context, host, index, shard, requestID string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.DeleteObject(ctx, host, index, shard, requestID, id, deletionTime, schemaVersion)
	}
	return s.restClient.DeleteObject(ctx, host, index, shard, requestID, id, deletionTime, schemaVersion)
}

func (s *switchableReplicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.DeleteObjects(ctx, host, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
	}
	return s.restClient.DeleteObjects(ctx, host, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
}

func (s *switchableReplicationClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs []objects.BatchReference, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.AddReferences(ctx, host, index, shard, requestID, refs, schemaVersion)
	}
	return s.restClient.AddReferences(ctx, host, index, shard, requestID, refs, schemaVersion)
}

func (s *switchableReplicationClient) Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error {
	if s.useGRPC() {
		return s.grpcClient.Commit(ctx, host, index, shard, requestID, resp)
	}
	return s.restClient.Commit(ctx, host, index, shard, requestID, resp)
}

func (s *switchableReplicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.Abort(ctx, host, index, shard, requestID)
	}
	return s.restClient.Abort(ctx, host, index, shard, requestID)
}

// ── Read operations (RClient) ────────────────────────────────────────────────

func (s *switchableReplicationClient) FetchObject(ctx context.Context, host, index, shard string,
	id strfmt.UUID, props search.SelectProperties, additional additional.Properties, numRetries int,
) (replica.Replica, error) {
	if s.useGRPC() {
		return s.grpcClient.FetchObject(ctx, host, index, shard, id, props, additional, numRetries)
	}
	return s.restClient.FetchObject(ctx, host, index, shard, id, props, additional, numRetries)
}

func (s *switchableReplicationClient) FetchObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID,
) ([]replica.Replica, error) {
	if s.useGRPC() {
		return s.grpcClient.FetchObjects(ctx, host, index, shard, ids)
	}
	return s.restClient.FetchObjects(ctx, host, index, shard, ids)
}

func (s *switchableReplicationClient) DigestObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID, numRetries int,
) ([]types.RepairResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.DigestObjects(ctx, host, index, shard, ids, numRetries)
	}
	return s.restClient.DigestObjects(ctx, host, index, shard, ids, numRetries)
}

func (s *switchableReplicationClient) DigestObjectsInRange(ctx context.Context, host, index, shard string,
	initialUUID, finalUUID strfmt.UUID, limit int,
) ([]types.RepairResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
	}
	return s.restClient.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
}

func (s *switchableReplicationClient) OverwriteObjects(ctx context.Context, host, index, shard string,
	vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.OverwriteObjects(ctx, host, index, shard, vobjects)
	}
	return s.restClient.OverwriteObjects(ctx, host, index, shard, vobjects)
}

func (s *switchableReplicationClient) FindUUIDs(ctx context.Context, host, index, shard string,
	filter *filters.LocalFilter, limit int,
) ([]strfmt.UUID, error) {
	if s.useGRPC() {
		return s.grpcClient.FindUUIDs(ctx, host, index, shard, filter, limit)
	}
	return s.restClient.FindUUIDs(ctx, host, index, shard, filter, limit)
}

func (s *switchableReplicationClient) HashTreeLevel(ctx context.Context, host, index, shard string,
	level int, discriminant *hashtree.Bitset,
) ([]hashtree.Digest, error) {
	if s.useGRPC() {
		return s.grpcClient.HashTreeLevel(ctx, host, index, shard, level, discriminant)
	}
	return s.restClient.HashTreeLevel(ctx, host, index, shard, level, discriminant)
}
