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

package v1

import (
	"context"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/sharding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fileChunkSize defines the size of each file chunk sent over gRPC.
// Currently set to 64 KB, which is a reasonable size for network transmission.
// It can be made configurable in the future if needed.
const fileChunkSize = 64 * 1024 // 64 KB

type FileReplicationService struct {
	pb.UnimplementedFileReplicationServiceServer

	repo   sharding.RemoteIncomingRepo
	schema sharding.RemoteIncomingSchema
}

func NewFileReplicationService(repo sharding.RemoteIncomingRepo, schema sharding.RemoteIncomingSchema) *FileReplicationService {
	return &FileReplicationService{
		repo:   repo,
		schema: schema,
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

func (fps *FileReplicationService) GetFileMetadata(stream grpc.BidiStreamingServer[pb.GetFileMetadataRequest, pb.FileMetadata]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return status.Errorf(codes.Internal, "failed to receive request: %v", err)
		}

		indexName := req.GetIndexName()
		shardName := req.GetShardName()
		fileName := req.GetFileName()

		index := fps.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
		if index == nil {
			return status.Errorf(codes.Internal, "local index %q not found", indexName)
		}

		md, err := index.IncomingGetFileMetadata(stream.Context(), shardName, fileName)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to get file metadata for %q in shard %q: %v", fileName, shardName, err)
		}

		if err := stream.Send(&pb.FileMetadata{
			IndexName: indexName,
			ShardName: shardName,
			FileName:  fileName,
			Size:      md.Size,
			Crc32:     md.CRC32,
		}); err != nil {
			return status.Errorf(codes.Internal, "failed to send file metadata response: %v", err)
		}
	}
}

func (fps *FileReplicationService) GetFile(stream grpc.BidiStreamingServer[pb.GetFileRequest, pb.FileChunk]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return status.Errorf(codes.Internal, "failed to receive request: %v", err)
		}

		if req.GetCompression() != pb.CompressionType_COMPRESSION_TYPE_UNSPECIFIED {
			return status.Errorf(codes.Unimplemented, "compression type %q is not supported", req.GetCompression())
		}

		indexName := req.GetIndexName()
		shardName := req.GetShardName()
		fileName := req.GetFileName()

		index := fps.repo.GetIndexForIncomingSharding(schema.ClassName(indexName))
		if index == nil {
			return status.Errorf(codes.Internal, "local index %q not found", indexName)
		}

		fileReader, err := index.IncomingGetFile(stream.Context(), shardName, fileName)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to get file %q in shard %q: %v", fileName, shardName, err)
		}
		defer fileReader.Close()

		buf := make([]byte, fileChunkSize)

		offset := 0

		for {
			n, err := fileReader.Read(buf)
			eof := err != nil && errors.Is(err, io.EOF)

			if err := stream.Send(&pb.FileChunk{
				Offset: int64(offset),
				Data:   buf[:n],
				Eof:    eof,
			}); err != nil {
				return status.Errorf(codes.Internal, "failed to send file chunk: %v", err)
			}

			if eof {
				break
			}

			offset += n
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
