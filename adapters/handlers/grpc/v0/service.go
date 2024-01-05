//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package weaviategrpc

import (
	"context"

	"github.com/pkg/errors"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v0"
)

type Service struct {
	pb.UnimplementedWeaviateServer
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
	return nil, errors.New(
		"The V0 gRPC API is deprecated and will be removed in the next major release. Please use the V1 API instead by upgrading your client.",
	)
}

func (s *Service) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	return nil, errors.New(
		"The V0 gRPC API is deprecated and will be removed in the next major release. Please use the V1 API instead by upgrading your client.",
	)
}
