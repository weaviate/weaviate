//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package v0

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/usecases/objects"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v0"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	schemaManager "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/traverser"
)

type Server struct {
	pb.UnimplementedWeaviateServer
	traverser            *traverser.Traverser
	authComposer         composer.TokenFunc
	allowAnonymousAccess bool
	schemaManager        *schemaManager.Manager
	batchManager         *objects.BatchManager
}

func NewServer(traverser *traverser.Traverser, authComposer composer.TokenFunc,
	allowAnonymousAccess bool, schemaManager *schemaManager.Manager,
	batchManager *objects.BatchManager) *Server {
	return &Server{
		traverser:            traverser,
		authComposer:         authComposer,
		allowAnonymousAccess: allowAnonymousAccess,
		schemaManager:        schemaManager,
		batchManager:         batchManager,
	}
}

func (s *Server) BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
	before := time.Now()
	principal, err := s.principalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}
	scheme := s.schemaManager.GetSchemaSkipAuth()

	objs, objOriginalIndex, objectParsingErrors := batchFromProto(req, scheme)

	replicationProperties := extractReplicationPropertiesV0(req.ConsistencyLevel)

	all := "ALL"
	response, err := s.batchManager.AddObjects(ctx, principal, objs, []*string{&all}, replicationProperties)
	if err != nil {
		return nil, err
	}
	var objErrors []*pb.BatchObjectsReply_BatchError

	for i, obj := range response {
		if obj.Err != nil {
			objErrors = append(objErrors, &pb.BatchObjectsReply_BatchError{Index: int32(objOriginalIndex[i]), Error: obj.Err.Error()})
		}
	}

	for i, err := range objectParsingErrors {
		objErrors = append(objErrors, &pb.BatchObjectsReply_BatchError{Index: int32(i), Error: err.Error()})
	}

	result := &pb.BatchObjectsReply{
		Took:   float32(time.Since(before).Seconds()),
		Errors: objErrors,
	}
	return result, nil
}

func (s *Server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	return nil, errors.New(
		"The V0 gRPC API is deprecated and will be removed in the next major release. Please use the V1 API instead by upgrading your client.",
	)
}

func extractReplicationPropertiesV0(level *pb.ConsistencyLevel) *additional.ReplicationProperties {
	if level == nil {
		return nil
	}

	switch *level {
	case pb.ConsistencyLevel_CONSISTENCY_LEVEL_ONE:
		return &additional.ReplicationProperties{ConsistencyLevel: "ONE"}
	case pb.ConsistencyLevel_CONSISTENCY_LEVEL_QUORUM:
		return &additional.ReplicationProperties{ConsistencyLevel: "QUORUM"}
	case pb.ConsistencyLevel_CONSISTENCY_LEVEL_ALL:
		return &additional.ReplicationProperties{ConsistencyLevel: "ALL"}
	default:
		return nil
	}
}
