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
	"fmt"
	"io"

	"github.com/pkg/errors"
	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FileReplicationService struct {
	pb.UnimplementedFileReplicationServiceServer

	repo   sharding.RemoteIncomingRepo
	schema sharding.RemoteIncomingSchema

	fileChunkSize int
}

func NewFileReplicationService(repo sharding.RemoteIncomingRepo, schema sharding.RemoteIncomingSchema, fileChunkSize int) *FileReplicationService {
	return &FileReplicationService{
		repo:          repo,
		schema:        schema,
		fileChunkSize: fileChunkSize,
	}
}

func (fps *FileReplicationService) CreateReplicaSnapshot(ctx context.Context, req *pb.CreateReplicaSnapshotRequest) (*pb.CreateReplicaSnapshotResponse, error) {
	indexName := req.GetIndexName()
	shardName := req.GetShardName()
	opID := req.GetOpId()
	schemaVersion := req.GetSchemaVersion()

	index, err := fps.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "local index %q not found: %v", indexName, err)
	}

	files, err := index.IncomingCreateReplicaSnapshot(ctx, shardName, opID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create replica snapshot for index %q, shard %q, op %q: %v", indexName, shardName, opID, err)
	}

	return &pb.CreateReplicaSnapshotResponse{
		IndexName: indexName,
		ShardName: shardName,
		FileNames: files,
	}, nil
}

func (fps *FileReplicationService) ReleaseReplicaSnapshot(ctx context.Context, req *pb.ReleaseReplicaSnapshotRequest) (*pb.ReleaseReplicaSnapshotResponse, error) {
	indexName := req.GetIndexName()
	opID := req.GetOpId()

	index := fps.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, status.Errorf(codes.Internal, "local index %q not found", indexName)
	}

	if err := index.IncomingReleaseReplicaSnapshot(ctx, opID); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to release replica snapshot for index %q, op %q: %v", indexName, opID, err)
	}

	return &pb.ReleaseReplicaSnapshotResponse{
		IndexName: indexName,
	}, nil
}

func (fps *FileReplicationService) GetReplicaSnapshotFileMetadata(ctx context.Context, req *pb.GetReplicaSnapshotFileMetadataRequest) (*pb.FileMetadata, error) {
	indexName := req.GetIndexName()
	opID := req.GetOpId()
	fileName := req.GetFileName()

	index := fps.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, status.Errorf(codes.Internal, "local index %q not found", indexName)
	}

	md, err := index.IncomingGetReplicaSnapshotFileMetadata(ctx, opID, fileName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get file metadata for %q in op %q: %v", fileName, opID, err)
	}

	return &pb.FileMetadata{
		IndexName: indexName,
		FileName:  fileName,
		Size:      md.Size,
		Crc32:     md.CRC32,
	}, nil
}

func (fps *FileReplicationService) GetReplicaSnapshotFile(req *pb.GetReplicaSnapshotFileRequest, stream pb.FileReplicationService_GetReplicaSnapshotFileServer) error {
	if req.GetCompression() != pb.CompressionType_COMPRESSION_TYPE_UNSPECIFIED {
		return status.Errorf(codes.Unimplemented, "compression type %q is not supported", req.GetCompression())
	}

	indexName := req.GetIndexName()
	opID := req.GetOpId()
	fileName := req.GetFileName()

	index := fps.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return status.Errorf(codes.Internal, "local index %q not found", indexName)
	}

	reader, err := index.IncomingGetReplicaSnapshotFile(stream.Context(), opID, fileName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get file %q: %v", fileName, err)
	}
	defer reader.Close()

	buf := make([]byte, fps.fileChunkSize)
	offset := 0

	for {
		n, err := reader.Read(buf)
		eof := errors.Is(err, io.EOF)

		if err != nil && !eof {
			return status.Errorf(codes.Internal, "failed to read file %q: %v", fileName, err)
		}

		if n == 0 && !eof {
			return status.Errorf(codes.Internal,
				"unexpected zero-byte read without EOF for file %q in op %q", fileName, opID)
		}

		if err := stream.Send(&pb.FileChunk{
			Offset: int64(offset),
			Data:   buf[:n],
			Eof:    eof,
		}); err != nil {
			return err
		}
		offset += n

		if eof {
			return nil
		}
	}
}

func (fps *FileReplicationService) indexForIncomingWrite(ctx context.Context, indexName string,
	schemaVersion uint64,
) (sharding.RemoteIndexIncomingRepo, error) {
	// wait for schema and store to reach version >= schemaVersion
	if _, err := fps.schema.ReadOnlyClassWithVersion(ctx, indexName, schemaVersion); err != nil {
		return nil, fmt.Errorf("local index %q not found: %w", indexName, err)
	}
	index := fps.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, fmt.Errorf("local index %q not found", indexName)
	}

	return index, nil
}
