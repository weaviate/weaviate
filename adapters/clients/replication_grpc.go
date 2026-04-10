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
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	clusterapi "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	grpcconn "github.com/weaviate/weaviate/grpc/conn"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// grpcReplicationClient implements replica.Client using gRPC.
type grpcReplicationClient struct {
	connManager *grpcconn.ConnManager
}

// NewGRPCReplicationClient creates a new gRPC-based replication client.
func NewGRPCReplicationClient(connManager *grpcconn.ConnManager) *grpcReplicationClient {
	return &grpcReplicationClient{connManager: connManager}
}

func (c *grpcReplicationClient) getClient(host string) (protocol.ReplicationServiceClient, error) {
	conn, err := c.connManager.GetConn(host)
	if err != nil {
		return nil, fmt.Errorf("get gRPC connection to %s: %w", host, err)
	}
	return protocol.NewReplicationServiceClient(conn), nil
}

// ── Write operations (WClient) ───────────────────────────────────────────────

func (c *grpcReplicationClient) PutObject(ctx context.Context, host, index, shard, requestID string,
	obj *storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	client, err := c.getClient(host)
	if err != nil {
		return replica.SimpleResponse{}, err
	}

	objData, err := obj.MarshalBinary()
	if err != nil {
		return replica.SimpleResponse{}, fmt.Errorf("marshal object: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, COMMIT_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.PutObject(ctx, &protocol.PutObjectRequest{
		Index:         index,
		Shard:         shard,
		RequestId:     requestID,
		SchemaVersion: schemaVersion,
		ObjectData:    objData,
	})
	if err != nil {
		return replica.SimpleResponse{}, fmt.Errorf("gRPC PutObject: %w", err)
	}

	return protoToSimpleResponse(resp.GetResponse()), nil
}

func (c *grpcReplicationClient) PutObjects(ctx context.Context, host, index, shard, requestID string,
	objs []*storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	client, err := c.getClient(host)
	if err != nil {
		return replica.SimpleResponse{}, err
	}

	objsData, err := clusterapi.IndicesPayloads.ObjectList.Marshal(objs, clusterapi.MethodPut)
	if err != nil {
		return replica.SimpleResponse{}, fmt.Errorf("marshal objects: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, COMMIT_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.PutObjects(ctx, &protocol.PutObjectsRequest{
		Index:         index,
		Shard:         shard,
		RequestId:     requestID,
		SchemaVersion: schemaVersion,
		ObjectsData:   objsData,
	})
	if err != nil {
		return replica.SimpleResponse{}, fmt.Errorf("gRPC PutObjects: %w", err)
	}

	return protoToSimpleResponse(resp.GetResponse()), nil
}

func (c *grpcReplicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	doc *objects.MergeDocument, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	client, err := c.getClient(host)
	if err != nil {
		return replica.SimpleResponse{}, err
	}

	mergeData, err := json.Marshal(doc)
	if err != nil {
		return replica.SimpleResponse{}, fmt.Errorf("marshal merge document: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, COMMIT_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.MergeObject(ctx, &protocol.MergeObjectRequest{
		Index:         index,
		Shard:         shard,
		RequestId:     requestID,
		SchemaVersion: schemaVersion,
		MergeDocument: mergeData,
	})
	if err != nil {
		return replica.SimpleResponse{}, fmt.Errorf("gRPC MergeObject: %w", err)
	}

	return protoToSimpleResponse(resp.GetResponse()), nil
}

func (c *grpcReplicationClient) DeleteObject(ctx context.Context, host, index, shard, requestID string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	client, err := c.getClient(host)
	if err != nil {
		return replica.SimpleResponse{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, COMMIT_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.DeleteObject(ctx, &protocol.DeleteObjectRequest{
		Index:                 index,
		Shard:                 shard,
		RequestId:             requestID,
		SchemaVersion:         schemaVersion,
		Uuid:                  id.String(),
		DeletionTimeUnixMilli: deletionTime.UnixMilli(),
	})
	if err != nil {
		return replica.SimpleResponse{}, fmt.Errorf("gRPC DeleteObject: %w", err)
	}

	return protoToSimpleResponse(resp.GetResponse()), nil
}

func (c *grpcReplicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	client, err := c.getClient(host)
	if err != nil {
		return replica.SimpleResponse{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, COMMIT_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.DeleteObjects(ctx, &protocol.DeleteObjectsRequest{
		Index:                 index,
		Shard:                 shard,
		RequestId:             requestID,
		SchemaVersion:         schemaVersion,
		Uuids:                 clusterapi.UUIDsToStrings(uuids),
		DeletionTimeUnixMilli: deletionTime.UnixMilli(),
		DryRun:                dryRun,
	})
	if err != nil {
		return replica.SimpleResponse{}, fmt.Errorf("gRPC DeleteObjects: %w", err)
	}

	return protoToSimpleResponse(resp.GetResponse()), nil
}

func (c *grpcReplicationClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs []objects.BatchReference, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	client, err := c.getClient(host)
	if err != nil {
		return replica.SimpleResponse{}, err
	}

	refsData, err := json.Marshal(refs)
	if err != nil {
		return replica.SimpleResponse{}, fmt.Errorf("marshal references: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, COMMIT_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.AddReferences(ctx, &protocol.AddReferencesRequest{
		Index:         index,
		Shard:         shard,
		RequestId:     requestID,
		SchemaVersion: schemaVersion,
		References:    refsData,
	})
	if err != nil {
		return replica.SimpleResponse{}, fmt.Errorf("gRPC AddReferences: %w", err)
	}

	return protoToSimpleResponse(resp.GetResponse()), nil
}

func (c *grpcReplicationClient) Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error {
	client, err := c.getClient(host)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, COMMIT_TIMEOUT_VALUE*time.Second)
	defer cancel()

	commitResp, err := client.Commit(ctx, &protocol.CommitRequest{
		Index:     index,
		Shard:     shard,
		RequestId: requestID,
	})
	if err != nil {
		return fmt.Errorf("gRPC Commit: %w", err)
	}

	if len(commitResp.GetPayload()) > 0 {
		if err := json.Unmarshal(commitResp.GetPayload(), resp); err != nil {
			return fmt.Errorf("unmarshal commit response: %w", err)
		}
	}
	return nil
}

func (c *grpcReplicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (replica.SimpleResponse, error) {
	client, err := c.getClient(host)
	if err != nil {
		return replica.SimpleResponse{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, ABORT_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.Abort(ctx, &protocol.AbortRequest{
		Index:     index,
		Shard:     shard,
		RequestId: requestID,
	})
	if err != nil {
		return replica.SimpleResponse{}, fmt.Errorf("gRPC Abort: %w", err)
	}

	return protoToSimpleResponse(resp.GetResponse()), nil
}

// ── Read operations (RClient) ────────────────────────────────────────────────

func (c *grpcReplicationClient) FetchObject(ctx context.Context, host, index, shard string,
	id strfmt.UUID, _ search.SelectProperties, _ additional.Properties, numRetries int,
) (replica.Replica, error) {
	client, err := c.getClient(host)
	if err != nil {
		return replica.Replica{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, QUERY_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.FetchObject(ctx, &protocol.FetchObjectRequest{
		Index: index,
		Shard: shard,
		Uuid:  id.String(),
	}, grpc_retry.WithMax(uint(numRetries)))
	if err != nil {
		return replica.Replica{}, fmt.Errorf("gRPC FetchObject: %w", err)
	}

	var r replica.Replica
	if err := r.UnmarshalBinary(resp.GetReplicaData()); err != nil {
		return replica.Replica{}, fmt.Errorf("unmarshal replica: %w", err)
	}
	return r, nil
}

func (c *grpcReplicationClient) FetchObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID,
) ([]replica.Replica, error) {
	client, err := c.getClient(host)
	if err != nil {
		return nil, err
	}

	// Use COMMIT_TIMEOUT_VALUE (90s) to match the REST transport, since FetchObjects
	// is used for consistency repair and may need more time under load.
	ctx, cancel := context.WithTimeout(ctx, COMMIT_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.FetchObjects(ctx, &protocol.FetchObjectsRequest{
		Index: index,
		Shard: shard,
		Uuids: clusterapi.UUIDsToStrings(ids),
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC FetchObjects: %w", err)
	}

	var replicas replica.Replicas
	if err := replicas.UnmarshalBinary(resp.GetReplicasData()); err != nil {
		return nil, fmt.Errorf("unmarshal replicas: %w", err)
	}
	return replicas, nil
}

func (c *grpcReplicationClient) DigestObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID, numRetries int,
) ([]types.RepairResponse, error) {
	client, err := c.getClient(host)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, QUERY_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.DigestObjects(ctx, &protocol.DigestObjectsRequest{
		Index: index,
		Shard: shard,
		Ids:   clusterapi.UUIDsToStrings(ids),
	}, grpc_retry.WithMax(uint(numRetries)))
	if err != nil {
		return nil, fmt.Errorf("gRPC DigestObjects: %w", err)
	}

	return protoToRepairResponses(resp.GetDigests()), nil
}

func (c *grpcReplicationClient) DigestObjectsInRange(ctx context.Context, host, index, shard string,
	initialUUID, finalUUID strfmt.UUID, limit int,
) ([]types.RepairResponse, error) {
	client, err := c.getClient(host)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, QUERY_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.DigestObjectsInRange(ctx, &protocol.DigestObjectsInRangeRequest{
		Index:       index,
		Shard:       shard,
		InitialUuid: initialUUID.String(),
		FinalUuid:   finalUUID.String(),
		Limit:       int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC DigestObjectsInRange: %w", err)
	}

	return protoToRepairResponses(resp.GetDigests()), nil
}

func (c *grpcReplicationClient) OverwriteObjects(ctx context.Context, host, index, shard string,
	vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	client, err := c.getClient(host)
	if err != nil {
		return nil, err
	}

	vData, err := clusterapi.IndicesPayloads.VersionedObjectList.Marshal(vobjects)
	if err != nil {
		return nil, fmt.Errorf("marshal vobjects: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, COMMIT_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.OverwriteObjects(ctx, &protocol.OverwriteObjectsRequest{
		Index:        index,
		Shard:        shard,
		VobjectsData: vData,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC OverwriteObjects: %w", err)
	}

	return protoToRepairResponses(resp.GetResults()), nil
}

func (c *grpcReplicationClient) FindUUIDs(ctx context.Context, host, index, shard string,
	filter *filters.LocalFilter, limit int,
) ([]strfmt.UUID, error) {
	client, err := c.getClient(host)
	if err != nil {
		return nil, err
	}

	var filterJSON []byte
	if filter != nil {
		filterJSON, err = json.Marshal(filter)
		if err != nil {
			return nil, fmt.Errorf("marshal filter: %w", err)
		}
	}

	// No explicit timeout — relies on caller's context deadline, matching REST behavior.
	// Disable retries to match REST behavior, which had no retries for FindUUIDs.
	resp, err := client.FindUUIDs(ctx, &protocol.FindUUIDsRequest{
		Index:      index,
		Shard:      shard,
		FilterJson: filterJSON,
		Limit:      int32(limit),
	}, grpc_retry.Disable())
	if err != nil {
		return nil, fmt.Errorf("gRPC FindUUIDs: %w", err)
	}

	return clusterapi.StringsToUUIDs(resp.GetUuids()), nil
}

func (c *grpcReplicationClient) HashTreeLevel(ctx context.Context, host, index, shard string,
	level int, discriminant *hashtree.Bitset,
) ([]hashtree.Digest, error) {
	client, err := c.getClient(host)
	if err != nil {
		return nil, err
	}

	discData, err := discriminant.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal discriminant: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, QUERY_TIMEOUT_VALUE*time.Second)
	defer cancel()

	resp, err := client.HashTreeLevel(ctx, &protocol.HashTreeLevelRequest{
		Index:        index,
		Shard:        shard,
		Level:        int32(level),
		Discriminant: discData,
	})
	if err != nil {
		return nil, fmt.Errorf("gRPC HashTreeLevel: %w", err)
	}

	var digests []hashtree.Digest
	if err := json.Unmarshal(resp.GetDigestsData(), &digests); err != nil {
		return nil, fmt.Errorf("unmarshal digests: %w", err)
	}
	return digests, nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func protoToSimpleResponse(r *protocol.SimpleReplicaResponse) replica.SimpleResponse {
	if r == nil {
		return replica.SimpleResponse{}
	}
	errs := make([]replica.Error, len(r.GetErrors()))
	for i, e := range r.GetErrors() {
		errs[i] = replica.Error{
			Code: replica.StatusCode(e.GetCode()),
			Msg:  e.GetMsg(),
		}
	}
	return replica.SimpleResponse{Errors: errs}
}

func protoToRepairResponses(results []*protocol.RepairResponse) []types.RepairResponse {
	out := make([]types.RepairResponse, len(results))
	for i, r := range results {
		out[i] = types.RepairResponse{
			ID:         r.GetId(),
			Version:    r.GetVersion(),
			UpdateTime: r.GetUpdateTime(),
			Err:        r.GetErr(),
			Deleted:    r.GetDeleted(),
		}
	}
	return out
}
