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

package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-openapi/strfmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/cluster/router/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
)

// ReplicationService implements the gRPC ReplicationServiceServer.
type ReplicationService struct {
	pb.UnimplementedReplicationServiceServer

	server replicaTypes.Replicator
}

// NewReplicationService creates a new ReplicationService.
func NewReplicationService(server replicaTypes.Replicator) *ReplicationService {
	return &ReplicationService{server: server}
}

// ── Write operations ─────────────────────────────────────────────────────────

func (s *ReplicationService) PutObject(ctx context.Context, req *pb.PutObjectRequest) (*pb.PutObjectResponse, error) {
	obj, err := storobj.FromBinary(req.GetObjectData())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal object: %v", err)
	}

	resp := s.server.ReplicateObject(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(), obj, req.GetSchemaVersion())
	if shared.LocalIndexNotReady(resp) {
		return nil, status.Errorf(codes.Unavailable, "local index not ready: %v", resp.FirstError())
	}
	return &pb.PutObjectResponse{Response: simpleResponseToProto(&resp)}, nil
}

func (s *ReplicationService) PutObjects(ctx context.Context, req *pb.PutObjectsRequest) (*pb.PutObjectsResponse, error) {
	objs, err := shared.IndicesPayloads.ObjectList.Unmarshal(req.GetObjectsData(), shared.MethodPut)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal objects: %v", err)
	}

	resp := s.server.ReplicateObjects(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(), objs, req.GetSchemaVersion())
	if shared.LocalIndexNotReady(resp) {
		return nil, status.Errorf(codes.Unavailable, "local index not ready: %v", resp.FirstError())
	}
	return &pb.PutObjectsResponse{Response: simpleResponseToProto(&resp)}, nil
}

func (s *ReplicationService) MergeObject(ctx context.Context, req *pb.MergeObjectRequest) (*pb.MergeObjectResponse, error) {
	var mergeDoc objects.MergeDocument
	if err := json.Unmarshal(req.GetMergeDocument(), &mergeDoc); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal merge document: %v", err)
	}

	resp := s.server.ReplicateUpdate(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(), &mergeDoc, req.GetSchemaVersion())
	if shared.LocalIndexNotReady(resp) {
		return nil, status.Errorf(codes.Unavailable, "local index not ready: %v", resp.FirstError())
	}
	return &pb.MergeObjectResponse{Response: simpleResponseToProto(&resp)}, nil
}

func (s *ReplicationService) DeleteObject(ctx context.Context, req *pb.DeleteObjectRequest) (*pb.DeleteObjectResponse, error) {
	deletionTime := time.UnixMilli(req.GetDeletionTimeUnixMilli())

	resp := s.server.ReplicateDeletion(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(),
		strfmt.UUID(req.GetUuid()), deletionTime, req.GetSchemaVersion())
	if shared.LocalIndexNotReady(resp) {
		return nil, status.Errorf(codes.Unavailable, "local index not ready: %v", resp.FirstError())
	}
	return &pb.DeleteObjectResponse{Response: simpleResponseToProto(&resp)}, nil
}

func (s *ReplicationService) DeleteObjects(ctx context.Context, req *pb.DeleteObjectsRequest) (*pb.DeleteObjectsResponse, error) {
	uuids := shared.StringsToUUIDs(req.GetUuids())
	deletionTime := time.UnixMilli(req.GetDeletionTimeUnixMilli())

	resp := s.server.ReplicateDeletions(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(),
		uuids, deletionTime, req.GetDryRun(), req.GetSchemaVersion())
	if shared.LocalIndexNotReady(resp) {
		return nil, status.Errorf(codes.Unavailable, "local index not ready: %v", resp.FirstError())
	}
	return &pb.DeleteObjectsResponse{Response: simpleResponseToProto(&resp)}, nil
}

func (s *ReplicationService) AddReferences(ctx context.Context, req *pb.AddReferencesRequest) (*pb.AddReferencesResponse, error) {
	var refs objects.BatchReferences
	if err := json.Unmarshal(req.GetReferences(), &refs); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal references: %v", err)
	}

	resp := s.server.ReplicateReferences(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(), refs, req.GetSchemaVersion())
	if shared.LocalIndexNotReady(resp) {
		return nil, status.Errorf(codes.Unavailable, "local index not ready: %v", resp.FirstError())
	}
	return &pb.AddReferencesResponse{Response: simpleResponseToProto(&resp)}, nil
}

func (s *ReplicationService) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	result := s.server.CommitReplication(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId())
	if result == nil {
		return nil, status.Error(codes.NotFound, "request not found")
	}

	payload, err := json.Marshal(result)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal commit result: %v", err)
	}
	return &pb.CommitResponse{Payload: payload}, nil
}

func (s *ReplicationService) Abort(ctx context.Context, req *pb.AbortRequest) (*pb.AbortResponse, error) {
	result := s.server.AbortReplication(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId())
	if result == nil {
		return nil, status.Error(codes.NotFound, "request not found")
	}

	if resp, ok := result.(replica.SimpleResponse); ok {
		return &pb.AbortResponse{Response: simpleResponseToProto(&resp)}, nil
	}

	return &pb.AbortResponse{Response: &pb.SimpleReplicaResponse{}}, nil
}

// ── Read operations ──────────────────────────────────────────────────────────

func (s *ReplicationService) FetchObject(ctx context.Context, req *pb.FetchObjectRequest) (*pb.FetchObjectResponse, error) {
	resp, err := s.server.FetchObject(ctx, req.GetIndex(), req.GetShard(), strfmt.UUID(req.GetUuid()))
	if err != nil {
		return nil, replicationErrorToGRPC(err)
	}

	data, err := resp.MarshalBinary()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal replica: %v", err)
	}
	return &pb.FetchObjectResponse{ReplicaData: data}, nil
}

func (s *ReplicationService) FetchObjects(ctx context.Context, req *pb.FetchObjectsRequest) (*pb.FetchObjectsResponse, error) {
	uuids := shared.StringsToUUIDs(req.GetUuids())

	resp, err := s.server.FetchObjects(ctx, req.GetIndex(), req.GetShard(), uuids)
	if err != nil {
		return nil, replicationErrorToGRPC(err)
	}

	data, err := replica.Replicas(resp).MarshalBinary()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal replicas: %v", err)
	}
	return &pb.FetchObjectsResponse{ReplicasData: data}, nil
}

func (s *ReplicationService) DigestObjects(ctx context.Context, req *pb.DigestObjectsRequest) (*pb.DigestObjectsResponse, error) {
	ids := shared.StringsToUUIDs(req.GetIds())

	results, err := s.server.DigestObjects(ctx, req.GetIndex(), req.GetShard(), ids)
	if err != nil {
		return nil, replicationErrorToGRPC(err)
	}

	return &pb.DigestObjectsResponse{Digests: repairResponsesToProto(results)}, nil
}

func (s *ReplicationService) DigestObjectsInRange(ctx context.Context, req *pb.DigestObjectsInRangeRequest) (*pb.DigestObjectsInRangeResponse, error) {
	results, err := s.server.DigestObjectsInRange(ctx, req.GetIndex(), req.GetShard(),
		strfmt.UUID(req.GetInitialUuid()), strfmt.UUID(req.GetFinalUuid()), int(req.GetLimit()))
	if err != nil {
		return nil, replicationErrorToGRPC(err)
	}

	return &pb.DigestObjectsInRangeResponse{Digests: repairResponsesToProto(results)}, nil
}

func (s *ReplicationService) OverwriteObjects(ctx context.Context, req *pb.OverwriteObjectsRequest) (*pb.OverwriteObjectsResponse, error) {
	vobjs, err := shared.IndicesPayloads.VersionedObjectList.Unmarshal(req.GetVobjectsData())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal vobjects: %v", err)
	}

	results, err := s.server.OverwriteObjects(ctx, req.GetIndex(), req.GetShard(), vobjs)
	if err != nil {
		return nil, replicationErrorToGRPC(err)
	}

	return &pb.OverwriteObjectsResponse{Results: repairResponsesToProto(results)}, nil
}

func (s *ReplicationService) FindUUIDs(ctx context.Context, req *pb.FindUUIDsRequest) (*pb.FindUUIDsResponse, error) {
	var filter *filters.LocalFilter
	if len(req.GetFilterJson()) > 0 {
		filter = &filters.LocalFilter{}
		if err := json.Unmarshal(req.GetFilterJson(), filter); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "unmarshal filter: %v", err)
		}
	}

	uuids, err := s.server.FindUUIDs(ctx, req.GetIndex(), req.GetShard(), filter, int(req.GetLimit()))
	if err != nil {
		return nil, replicationErrorToGRPC(err)
	}

	return &pb.FindUUIDsResponse{Uuids: shared.UUIDsToStrings(uuids)}, nil
}

func (s *ReplicationService) HashTreeLevel(ctx context.Context, req *pb.HashTreeLevelRequest) (*pb.HashTreeLevelResponse, error) {
	var discriminant hashtree.Bitset
	if err := discriminant.Unmarshal(req.GetDiscriminant()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal discriminant: %v", err)
	}

	digests, err := s.server.HashTreeLevel(ctx, req.GetIndex(), req.GetShard(), int(req.GetLevel()), &discriminant)
	if err != nil {
		return nil, replicationErrorToGRPC(err)
	}

	data, err := json.Marshal(digests)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal digests: %v", err)
	}
	return &pb.HashTreeLevelResponse{DigestsData: data}, nil
}

func simpleResponseToProto(r *replica.SimpleResponse) *pb.SimpleReplicaResponse {
	if r == nil {
		return &pb.SimpleReplicaResponse{}
	}
	errs := make([]*pb.ReplicaError, len(r.Errors))
	for i, e := range r.Errors {
		errs[i] = &pb.ReplicaError{
			Code: int32(e.Code),
			Msg:  e.Msg,
		}
	}
	return &pb.SimpleReplicaResponse{Errors: errs}
}

func repairResponsesToProto(results []types.RepairResponse) []*pb.RepairResponse {
	out := make([]*pb.RepairResponse, len(results))
	for i, r := range results {
		out[i] = &pb.RepairResponse{
			Id:         r.ID,
			Version:    r.Version,
			UpdateTime: r.UpdateTime,
			Err:        r.Err,
			Deleted:    r.Deleted,
		}
	}
	return out
}

func replicationErrorToGRPC(err error) error {
	if err == nil {
		return nil
	}
	if errors.As(err, &enterrors.ErrUnprocessable{}) {
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return status.Errorf(codes.Internal, "%v", err)
}
