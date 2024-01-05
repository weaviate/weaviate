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

package v1

import (
	"context"
	"fmt"
	"time"

	"github.com/weaviate/weaviate/usecases/objects"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	schemaManager "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/traverser"
)

type Service struct {
	pb.UnimplementedWeaviateServer
	traverser            *traverser.Traverser
	authComposer         composer.TokenFunc
	allowAnonymousAccess bool
	schemaManager        *schemaManager.Manager
	batchManager         *objects.BatchManager
}

func NewService(traverser *traverser.Traverser, authComposer composer.TokenFunc,
	allowAnonymousAccess bool, schemaManager *schemaManager.Manager,
	batchManager *objects.BatchManager,
) *Service {
	return &Service{
		traverser:            traverser,
		authComposer:         authComposer,
		allowAnonymousAccess: allowAnonymousAccess,
		schemaManager:        schemaManager,
		batchManager:         batchManager,
	}
}

func (s *Service) BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
	before := time.Now()
	principal, err := s.principalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}
	scheme := s.schemaManager.GetSchemaSkipAuth()

	objs, objOriginalIndex, objectParsingErrors := batchFromProto(req, scheme)

	replicationProperties := extractReplicationProperties(req.ConsistencyLevel)

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

func (s *Service) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	before := time.Now()

	principal, err := s.principalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}

	scheme := s.schemaManager.GetSchemaSkipAuth()

	type reply struct {
		Result *pb.SearchReply
		Error  error
	}

	c := make(chan reply, 1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				c <- reply{
					Result: nil,
					Error:  fmt.Errorf("panic occurred: %v", err),
				}
			}
		}()

		searchParams, err := searchParamsFromProto(req, scheme)
		if err != nil {
			c <- reply{
				Result: nil,
				Error:  fmt.Errorf("extract params: %w", err),
			}
		}

		if err := s.validateClassAndProperty(searchParams); err != nil {
			c <- reply{
				Result: nil,
				Error:  err,
			}
		}

		res, err := s.traverser.GetClass(ctx, principal, searchParams)
		if err != nil {
			c <- reply{
				Result: nil,
				Error:  err,
			}
		}

		proto, err := searchResultsToProto(res, before, searchParams, scheme, req.Uses_123Api)
		c <- reply{
			Result: proto,
			Error:  err,
		}
	}()
	res := <-c
	return res.Result, res.Error
}

func (s *Service) validateClassAndProperty(searchParams dto.GetParams) error {
	scheme := s.schemaManager.GetSchemaSkipAuth()
	class, err := schema.GetClassByName(scheme.Objects, searchParams.ClassName)
	if err != nil {
		return err
	}

	for _, prop := range searchParams.Properties {
		_, err := schema.GetPropertyByName(class, prop.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

func extractReplicationProperties(level *pb.ConsistencyLevel) *additional.ReplicationProperties {
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
