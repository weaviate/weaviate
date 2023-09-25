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

package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/additional"

	"github.com/weaviate/weaviate/usecases/objects"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	schemaManager "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/traverser"
	"google.golang.org/grpc"
)

const maxMsgSize = 104858000 // 10mb, needs to be synchronized with clients

func CreateGRPCServer(state *state.State) *GRPCServer {
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)
	pb.RegisterWeaviateServer(s, &Server{
		traverser: state.Traverser,
		authComposer: composer.New(
			state.ServerConfig.Config.Authentication,
			state.APIKey, state.OIDC),
		allowAnonymousAccess: state.ServerConfig.Config.Authentication.AnonymousAccess.Enabled,
		schemaManager:        state.SchemaManager,
		batchManager:         state.BatchManager,
	})

	return &GRPCServer{s}
}

func StartAndListen(s *GRPCServer, state *state.State) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d",
		state.ServerConfig.Config.GRPC.Port))
	if err != nil {
		return err
	}
	state.Logger.WithField("action", "grpc_startup").
		Infof("grpc server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}

	return nil
}

type GRPCServer struct {
	*grpc.Server
}

type Server struct {
	pb.UnimplementedWeaviateServer
	traverser            *traverser.Traverser
	authComposer         composer.TokenFunc
	allowAnonymousAccess bool
	schemaManager        *schemaManager.Manager
	batchManager         *objects.BatchManager
}

func (s *Server) BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
	before := time.Now()
	principal, err := s.principalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}
	scheme := s.schemaManager.GetSchemaSkipAuth()

	objs, err := batchFromProto(req, scheme)
	if err != nil {
		return nil, err
	}

	replicationProperties := extractReplicationProperties(req.ConsistencyLevel)

	all := "ALL"
	response, err := s.batchManager.AddObjects(ctx, principal, objs, []*string{&all}, replicationProperties)
	if err != nil {
		return nil, err
	}
	var objErrors []*pb.BatchObjectsReply_BatchResults

	for i, obj := range response {
		if obj.Err != nil {
			objErrors = append(objErrors, &pb.BatchObjectsReply_BatchResults{Index: int32(i), Error: obj.Err.Error()})
		}
	}

	result := &pb.BatchObjectsReply{
		Took:    float32(time.Since(before).Seconds()),
		Results: objErrors,
	}
	return result, nil
}

func (s *Server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	return nil, errors.New(
		"search endpoint not supported anymore. Please update your client to use the new SearchV1 endpoint",
	)
}

func (s *Server) SearchV1(ctx context.Context, req *pb.SearchRequestV1) (*pb.SearchReplyV1, error) {
	before := time.Now()

	principal, err := s.principalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}

	scheme := s.schemaManager.GetSchemaSkipAuth()

	type reply struct {
		Result *pb.SearchReplyV1
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

		proto, err := searchResultsToProto(res, before, searchParams, scheme)
		c <- reply{
			Result: proto,
			Error:  err,
		}
	}()
	res := <-c
	return res.Result, res.Error
}

func (s *Server) validateClassAndProperty(searchParams dto.GetParams) error {
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
