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

// switchReplicationClient routes all replication calls to either gRPC or REST
// based on the useGRPC function, controlled by the replication_grpc_enabled runtime config.
type switchReplicationClient struct {
	grpcClient *grpcReplicationClient
	restClient *replicationClient
	useGRPC    func() bool
}

var _ (replica.Client) = (*switchReplicationClient)(nil)

// NewSwitchReplicationClient creates a client that routes all calls to gRPC
// when useGRPC returns true, or REST when false.
func NewSwitchReplicationClient(
	grpcClient *grpcReplicationClient,
	restClient *replicationClient,
	useGRPC func() bool,
) *switchReplicationClient {
	return &switchReplicationClient{
		grpcClient: grpcClient,
		restClient: restClient,
		useGRPC:    useGRPC,
	}
}

// ── Write operations (WClient) ───────────────────────────────────────────────

func (s *switchReplicationClient) PutObject(ctx context.Context, host, index, shard, requestID string,
	obj *storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.PutObject(ctx, host, index, shard, requestID, obj, schemaVersion)
	}
	return s.restClient.PutObject(ctx, host, index, shard, requestID, obj, schemaVersion)
}

func (s *switchReplicationClient) PutObjects(ctx context.Context, host, index, shard, requestID string,
	objs []*storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.PutObjects(ctx, host, index, shard, requestID, objs, schemaVersion)
	}
	return s.restClient.PutObjects(ctx, host, index, shard, requestID, objs, schemaVersion)
}

func (s *switchReplicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	doc *objects.MergeDocument, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.MergeObject(ctx, host, index, shard, requestID, doc, schemaVersion)
	}
	return s.restClient.MergeObject(ctx, host, index, shard, requestID, doc, schemaVersion)
}

func (s *switchReplicationClient) DeleteObject(ctx context.Context, host, index, shard, requestID string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.DeleteObject(ctx, host, index, shard, requestID, id, deletionTime, schemaVersion)
	}
	return s.restClient.DeleteObject(ctx, host, index, shard, requestID, id, deletionTime, schemaVersion)
}

func (s *switchReplicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.DeleteObjects(ctx, host, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
	}
	return s.restClient.DeleteObjects(ctx, host, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
}

func (s *switchReplicationClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs []objects.BatchReference, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.AddReferences(ctx, host, index, shard, requestID, refs, schemaVersion)
	}
	return s.restClient.AddReferences(ctx, host, index, shard, requestID, refs, schemaVersion)
}

func (s *switchReplicationClient) Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error {
	if s.useGRPC() {
		return s.grpcClient.Commit(ctx, host, index, shard, requestID, resp)
	}
	return s.restClient.Commit(ctx, host, index, shard, requestID, resp)
}

func (s *switchReplicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (replica.SimpleResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.Abort(ctx, host, index, shard, requestID)
	}
	return s.restClient.Abort(ctx, host, index, shard, requestID)
}

// ── Read operations (RClient) ────────────────────────────────────────────────

func (s *switchReplicationClient) FetchObject(ctx context.Context, host, index, shard string,
	id strfmt.UUID, props search.SelectProperties, additional additional.Properties, numRetries int,
) (replica.Replica, error) {
	if s.useGRPC() {
		return s.grpcClient.FetchObject(ctx, host, index, shard, id, props, additional, numRetries)
	}
	return s.restClient.FetchObject(ctx, host, index, shard, id, props, additional, numRetries)
}

func (s *switchReplicationClient) FetchObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID,
) ([]replica.Replica, error) {
	if s.useGRPC() {
		return s.grpcClient.FetchObjects(ctx, host, index, shard, ids)
	}
	return s.restClient.FetchObjects(ctx, host, index, shard, ids)
}

func (s *switchReplicationClient) DigestObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID, numRetries int,
) ([]types.RepairResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.DigestObjects(ctx, host, index, shard, ids, numRetries)
	}
	return s.restClient.DigestObjects(ctx, host, index, shard, ids, numRetries)
}

func (s *switchReplicationClient) DigestObjectsInRange(ctx context.Context, host, index, shard string,
	initialUUID, finalUUID strfmt.UUID, limit int,
) ([]types.RepairResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
	}
	return s.restClient.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
}

func (s *switchReplicationClient) OverwriteObjects(ctx context.Context, host, index, shard string,
	vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	if s.useGRPC() {
		return s.grpcClient.OverwriteObjects(ctx, host, index, shard, vobjects)
	}
	return s.restClient.OverwriteObjects(ctx, host, index, shard, vobjects)
}

func (s *switchReplicationClient) FindUUIDs(ctx context.Context, host, index, shard string,
	filter *filters.LocalFilter, limit int,
) ([]strfmt.UUID, error) {
	if s.useGRPC() {
		return s.grpcClient.FindUUIDs(ctx, host, index, shard, filter, limit)
	}
	return s.restClient.FindUUIDs(ctx, host, index, shard, filter, limit)
}

func (s *switchReplicationClient) HashTreeLevel(ctx context.Context, host, index, shard string,
	level int, discriminant *hashtree.Bitset,
) ([]hashtree.Digest, error) {
	if s.useGRPC() {
		return s.grpcClient.HashTreeLevel(ctx, host, index, shard, level, discriminant)
	}
	return s.restClient.HashTreeLevel(ctx, host, index, shard, level, discriminant)
}

func (s *switchReplicationClient) CountObjects(ctx context.Context, host string, index string, shard string) (int, error) {
	if s.useGRPC() {
		return s.grpcClient.CountObjects(ctx, host, index, shard)
	}
	return s.restClient.CountObjects(ctx, host, index, shard)
}
