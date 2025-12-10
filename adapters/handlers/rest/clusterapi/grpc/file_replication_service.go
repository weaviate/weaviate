//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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

func (fps *FileReplicationService) PauseFileActivity(ctx context.Context, req *pb.PauseFileActivityRequest) (*pb.PauseFileActivityResponse, error) {
	indexName := req.GetIndexName()
	shardName := req.GetShardName()
	schemaVersion := req.GetSchemaVersion()

	index, err := fps.indexForIncomingWrite(ctx, indexName, schemaVersion)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "local index %q not found: %v", indexName, err)
	}

	err = index.IncomingPauseFileActivity(ctx, shardName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to pause file activity for index %q, shard %q: %v", indexName, shardName, err)
	}

	return &pb.PauseFileActivityResponse{
		IndexName: indexName,
		ShardName: shardName,
	}, nil
}

func (fps *FileReplicationService) ResumeFileActivity(ctx context.Context, req *pb.ResumeFileActivityRequest) (*pb.ResumeFileActivityResponse, error) {
	indexName := req.GetIndexName()
	shardName := req.GetShardName()

	index := fps.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, status.Errorf(codes.Internal, "local index %q not found", indexName)
	}

	err := index.IncomingResumeFileActivity(ctx, shardName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to resume file activity for index %q, shard %q: %v", indexName, shardName, err)
	}

	return &pb.ResumeFileActivityResponse{
		IndexName: indexName,
		ShardName: shardName,
	}, nil
}

func (fps *FileReplicationService) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	indexName := req.GetIndexName()
	shardName := req.GetShardName()

	index := fps.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, status.Errorf(codes.Internal, "local index %q not found", indexName)
	}

	files, err := index.IncomingListFiles(ctx, shardName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list files for index %q, shard %q: %v", indexName, shardName, err)
	}

	return &pb.ListFilesResponse{
		IndexName: indexName,
		ShardName: shardName,
		FileNames: files,
	}, nil
}

func (fps *FileReplicationService) GetFileMetadata(ctx context.Context, req *pb.GetFileMetadataRequest) (*pb.FileMetadata, error) {
	indexName := req.GetIndexName()
	shardName := req.GetShardName()
	fileName := req.GetFileName()

	index := fps.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
	if index == nil {
		return nil, status.Errorf(codes.Internal, "local index %q not found", indexName)
	}

	md, err := index.IncomingGetFileMetadata(ctx, shardName, fileName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get file metadata for %q in shard %q: %v", fileName, shardName, err)
	}

	return &pb.FileMetadata{
		IndexName: indexName,
		ShardName: shardName,
		FileName:  fileName,
		Size:      md.Size,
		Crc32:     md.CRC32,
	}, nil
}

func (fps *FileReplicationService) GetFile(req *pb.GetFileRequest, stream pb.FileReplicationService_GetFileServer) error {
	if req.GetCompression() != pb.CompressionType_COMPRESSION_TYPE_UNSPECIFIED {
		return status.Errorf(codes.Unimplemented, "compression type %q is not supported", req.GetCompression())
	}

	index := fps.repo.GetIndexForIncomingSharding(schema.ClassName(req.IndexName))
	if index == nil {
		return status.Errorf(codes.Internal, "local index %q not found", req.IndexName)
	}

	reader, err := index.IncomingGetFile(stream.Context(), req.ShardName, req.FileName)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get file %q: %v", req.FileName, err)
	}
	defer reader.Close()

	buf := make([]byte, fps.fileChunkSize)
	offset := 0

	for {
		n, err := reader.Read(buf)
		eof := errors.Is(err, io.EOF)

		if err != nil && !eof {
			return status.Errorf(codes.Internal, "failed to read file %q: %v", req.FileName, err)
		}

		if n == 0 && !eof {
			return status.Errorf(codes.Internal,
				"unexpected zero-byte read without EOF for file %q in shard %q", req.FileName, req.ShardName)
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
