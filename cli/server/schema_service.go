package cli

import (
	"context"

	pb "github.com/weaviate/weaviate/cli/proto/cli"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type schemaService struct{}

func (*schemaService) GetSchema(ctx context.Context, req *pb.GetSchemaRequest) (*pb.GetSchemaResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (*schemaService) GetRaftStatus(ctx context.Context, req *pb.GetRaftStatusRequest) (*pb.GetRaftStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (*schemaService) RaftTakeSnapshot(ctx context.Context, req *pb.RaftTakeSnapshotRequest) (*pb.RaftTakeSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
