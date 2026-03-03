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
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-openapi/strfmt"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	replicaTypes "github.com/weaviate/weaviate/usecases/replica/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	le               = binary.LittleEndian
	errTruncatedData = errors.New("truncated binary data")
)

// ReplicationServer is the interface that the gRPC replication service requires.
// It combines the Replicator interface with FindUUIDs which is served via a different
// REST path but is part of the RClient contract.
type ReplicationServer interface {
	replicaTypes.Replicator
	FindUUIDs(ctx context.Context, indexName, shardName string,
		filters *filters.LocalFilter, limit int) ([]strfmt.UUID, error)
}

// ReplicationService implements the gRPC ReplicationServiceServer.
type ReplicationService struct {
	pb.UnimplementedReplicationServiceServer

	server ReplicationServer
}

// NewReplicationService creates a new ReplicationService.
func NewReplicationService(server ReplicationServer) *ReplicationService {
	return &ReplicationService{server: server}
}

// ── Write operations ─────────────────────────────────────────────────────────

func (s *ReplicationService) PutObject(ctx context.Context, req *pb.PutObjectRequest) (*pb.PutObjectResponse, error) {
	obj, err := storobj.FromBinary(req.GetObjectData())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal object: %v", err)
	}

	resp := s.server.ReplicateObject(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(), obj, req.GetSchemaVersion())
	return &pb.PutObjectResponse{Response: simpleResponseToProto(&resp)}, nil
}

func (s *ReplicationService) PutObjects(ctx context.Context, req *pb.PutObjectsRequest) (*pb.PutObjectsResponse, error) {
	objs, err := unmarshalObjectList(req.GetObjectsData())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal objects: %v", err)
	}

	resp := s.server.ReplicateObjects(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(), objs, req.GetSchemaVersion())
	return &pb.PutObjectsResponse{Response: simpleResponseToProto(&resp)}, nil
}

func (s *ReplicationService) MergeObject(ctx context.Context, req *pb.MergeObjectRequest) (*pb.MergeObjectResponse, error) {
	var mergeDoc objects.MergeDocument
	if err := json.Unmarshal(req.GetMergeDocument(), &mergeDoc); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal merge document: %v", err)
	}

	resp := s.server.ReplicateUpdate(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(), &mergeDoc, req.GetSchemaVersion())
	return &pb.MergeObjectResponse{Response: simpleResponseToProto(&resp)}, nil
}

func (s *ReplicationService) DeleteObject(ctx context.Context, req *pb.DeleteObjectRequest) (*pb.DeleteObjectResponse, error) {
	deletionTime := time.UnixMilli(req.GetDeletionTimeUnixMilli())

	resp := s.server.ReplicateDeletion(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(),
		strfmt.UUID(req.GetUuid()), deletionTime, req.GetSchemaVersion())
	return &pb.DeleteObjectResponse{Response: simpleResponseToProto(&resp)}, nil
}

func (s *ReplicationService) DeleteObjects(ctx context.Context, req *pb.DeleteObjectsRequest) (*pb.DeleteObjectsResponse, error) {
	uuids := stringsToUUIDs(req.GetUuids())
	deletionTime := time.UnixMilli(req.GetDeletionTimeUnixMilli())

	resp := s.server.ReplicateDeletions(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(),
		uuids, deletionTime, req.GetDryRun(), req.GetSchemaVersion())
	return &pb.DeleteObjectsResponse{Response: simpleResponseToProto(&resp)}, nil
}

func (s *ReplicationService) AddReferences(ctx context.Context, req *pb.AddReferencesRequest) (*pb.AddReferencesResponse, error) {
	var refs objects.BatchReferences
	if err := json.Unmarshal(req.GetReferences(), &refs); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unmarshal references: %v", err)
	}

	resp := s.server.ReplicateReferences(ctx, req.GetIndex(), req.GetShard(), req.GetRequestId(), refs, req.GetSchemaVersion())
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
	uuids := stringsToUUIDs(req.GetUuids())

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
	ids := stringsToUUIDs(req.GetIds())

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
	vobjs, err := unmarshalVObjectList(req.GetVobjectsData())
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

	return &pb.FindUUIDsResponse{Uuids: uuidsToStrings(uuids)}, nil
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
	return status.Errorf(codes.Internal, "%v", err)
}

func stringsToUUIDs(ss []string) []strfmt.UUID {
	uuids := make([]strfmt.UUID, len(ss))
	for i, s := range ss {
		uuids[i] = strfmt.UUID(s)
	}
	return uuids
}

func uuidsToStrings(uuids []strfmt.UUID) []string {
	ss := make([]string, len(uuids))
	for i, u := range uuids {
		ss[i] = u.String()
	}
	return ss
}

// unmarshalObjectList parses the binary object list format (length-prefixed storobj entries).
// This mirrors the logic in clusterapi.objectListPayload.Unmarshal without importing clusterapi.
func unmarshalObjectList(data []byte) ([]*storobj.Object, error) {
	var out []*storobj.Object
	offset := 0
	for offset < len(data) {
		if offset+8 > len(data) {
			return nil, errTruncatedData
		}
		length := int(le.Uint64(data[offset : offset+8]))
		offset += 8
		if offset+length > len(data) {
			return nil, errTruncatedData
		}
		obj, err := storobj.FromBinary(data[offset : offset+length])
		if err != nil {
			return nil, err
		}
		out = append(out, obj)
		offset += length
	}
	return out, nil
}

// unmarshalVObjectList parses the binary versioned object list format.
// This mirrors clusterapi.versionedObjectListPayload.Unmarshal without importing clusterapi.
func unmarshalVObjectList(data []byte) ([]*objects.VObject, error) {
	var out []*objects.VObject
	offset := 0
	for offset < len(data) {
		if offset+8 > len(data) {
			return nil, errTruncatedData
		}
		length := int(le.Uint64(data[offset : offset+8]))
		offset += 8
		if offset+length > len(data) {
			return nil, errTruncatedData
		}
		var vobj objects.VObject
		if err := vobj.UnmarshalBinary(data[offset : offset+length]); err != nil {
			return nil, err
		}
		out = append(out, &vobj)
		offset += length
	}
	return out, nil
}
