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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	enterrors "github.com/weaviate/weaviate/entities/errors"

	"github.com/weaviate/weaviate/usecases/config"

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
	traverser     *traverser.Traverser
	schemaManager *schemaManager.Manager
	batchManager  *objects.BatchManager
	config        *config.Config
	authorizer    authorization.Authorizer
	logger        logrus.FieldLogger

	authenticator       *authHandler
	batchObjectsHandler *batch.ObjectsHandler
	batchStreamHandler  *batch.StreamHandler
}

func NewService(traverser *traverser.Traverser, authComposer composer.TokenFunc,
	allowAnonymousAccess bool, schemaManager *schemaManager.Manager,
	batchManager *objects.BatchManager, config *config.Config, authorization authorization.Authorizer,
	logger logrus.FieldLogger,
) *Service {
	authenticator := NewAuthHandler(allowAnonymousAccess, authComposer)
	batchObjectsHandler := batch.NewObjectsHandler(authorization, batchManager, logger, authenticator, schemaManager)
	return &Service{
		traverser:           traverser,
		schemaManager:       schemaManager,
		batchManager:        batchManager,
		config:              config,
		logger:              logger,
		authorizer:          authorization,
		authenticator:       authenticator,
		batchObjectsHandler: batchObjectsHandler,
		batchStreamHandler:  batch.NewStreamHandler(batchObjectsHandler, logger),
	}
}

func (s *Service) Aggregate(ctx context.Context, req *pb.AggregateRequest) (*pb.AggregateReply, error) {
	var result *pb.AggregateReply
	var errInner error

	if err := enterrors.GoWrapperWithBlock(func() {
		result, errInner = s.aggregate(ctx, req)
	}, s.logger); err != nil {
		return nil, err
	}

	return result, errInner
}

func (s *Service) aggregate(ctx context.Context, req *pb.AggregateRequest) (*pb.AggregateReply, error) {
	before := time.Now()

	principal, err := s.authenticator.PrincipalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}

	parser := NewAggregateParser(
		s.classGetterWithAuthzFunc(principal),
	)

	params, err := parser.Aggregate(req)
	if err != nil {
		return nil, fmt.Errorf("parse params: %w", err)
	}

	res, err := s.traverser.Aggregate(restCtx.AddPrincipalToContext(ctx, principal), principal, params)
	if err != nil {
		return nil, fmt.Errorf("aggregate: %w", err)
	}

	replier := NewAggregateReplier(
		s.classGetterWithAuthzFunc(principal),
		params,
	)
	reply, err := replier.Aggregate(res, params.GroupBy != nil)
	if err != nil {
		return nil, fmt.Errorf("prepare reply: %w", err)
	}

	reply.Took = float32(time.Since(before).Seconds())
	return reply, nil
}

func (s *Service) TenantsGet(ctx context.Context, req *pb.TenantsGetRequest) (*pb.TenantsGetReply, error) {
	before := time.Now()

	principal, err := s.authenticator.PrincipalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}

	retTenants, err := s.tenantsGet(ctx, principal, req)
	if err != nil {
		return nil, fmt.Errorf("get tenants: %w", err)
	}

	result := &pb.TenantsGetReply{
		Took:    float32(time.Since(before).Seconds()),
		Tenants: retTenants,
	}
	return result, nil
}

func (s *Service) BatchDelete(ctx context.Context, req *pb.BatchDeleteRequest) (*pb.BatchDeleteReply, error) {
	var result *pb.BatchDeleteReply
	var errInner error

	if err := enterrors.GoWrapperWithBlock(func() {
		result, errInner = s.batchDelete(ctx, req)
	}, s.logger); err != nil {
		return nil, err
	}

	return result, errInner
}

func (s *Service) batchDelete(ctx context.Context, req *pb.BatchDeleteRequest) (*pb.BatchDeleteReply, error) {
	before := time.Now()
	principal, err := s.authenticator.PrincipalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}
	replicationProperties := extractReplicationProperties(req.ConsistencyLevel)

	tenant := ""
	if req.Tenant != nil {
		tenant = *req.Tenant
	}

	if err := s.authorizer.Authorize(principal, authorization.DELETE, authorization.ShardsData(req.Collection, tenant)...); err != nil {
		return nil, err
	}

	params, err := batchDeleteParamsFromProto(req, s.classGetterWithAuthzFunc(principal))
	if err != nil {
		return nil, fmt.Errorf("batch delete params: %w", err)
	}

	response, err := s.batchManager.DeleteObjectsFromGRPCAfterAuth(ctx, principal, params, replicationProperties, tenant)
	if err != nil {
		return nil, fmt.Errorf("batch delete: %w", err)
	}

	result, err := batchDeleteReplyFromObjects(response, req.Verbose)
	if err != nil {
		return nil, fmt.Errorf("batch delete reply: %w", err)
	}
	result.Took = float32(time.Since(before).Seconds())

	return result, nil
}

func (s *Service) BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
	var result *pb.BatchObjectsReply
	var errInner error

	if err := enterrors.GoWrapperWithBlock(func() {
		result, errInner = s.batchObjectsHandler.BatchObjects(ctx, req)
	}, s.logger); err != nil {
		return nil, err
	}

	return result, errInner
}

func (s *Service) Batch(stream pb.Weaviate_BatchServer) error {
	var errInner error

	if err := enterrors.GoWrapperWithBlock(func() {
		errInner = s.batchStreamHandler.Stream(stream)
	}, s.logger); err != nil {
		return err
	}

	return errInner
}

func (s *Service) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	var result *pb.SearchReply
	var errInner error

	if err := enterrors.GoWrapperWithBlock(func() {
		result, errInner = s.search(ctx, req)
	}, s.logger); err != nil {
		return nil, err
	}

	return result, errInner
}

func (s *Service) search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	before := time.Now()

	principal, err := s.authenticator.PrincipalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}

	parser := NewParser(
		req.Uses_127Api,
		s.classGetterWithAuthzFunc(principal),
	)
	replier := NewReplier(
		req.Uses_123Api || req.Uses_125Api || req.Uses_127Api,
		req.Uses_125Api || req.Uses_127Api,
		req.Uses_127Api,
		parser.generative,
		s.logger,
	)

	searchParams, err := parser.Search(req, s.config)
	if err != nil {
		return nil, err
	}

	if err := s.validateClassAndProperty(searchParams); err != nil {
		return nil, err
	}

	res, err := s.traverser.GetClass(restCtx.AddPrincipalToContext(ctx, principal), principal, searchParams)
	if err != nil {
		return nil, err
	}

	scheme := s.schemaManager.GetSchemaSkipAuth()
	return replier.Search(res, before, searchParams, scheme)
}

func (s *Service) validateClassAndProperty(searchParams dto.GetParams) error {
	class := s.schemaManager.ReadOnlyClass(searchParams.ClassName)
	if class == nil {
		return fmt.Errorf("could not find class %s in schema", searchParams.ClassName)
	}

	for _, prop := range searchParams.Properties {
		_, err := schema.GetPropertyByName(class, prop.Name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) classGetterWithAuthzFunc(principal *models.Principal) func(string) (*models.Class, error) {
	authorizedCollections := map[string]*models.Class{}
	return func(name string) (*models.Class, error) {
		class, ok := authorizedCollections[name]
		if !ok {
			if err := s.authorizer.Authorize(principal, authorization.READ, authorization.Collections(name)...); err != nil {
				return nil, err
			}
			class = s.schemaManager.ReadOnlyClass(name)
			authorizedCollections[name] = class
		}
		if class == nil {
			return nil, fmt.Errorf("could not find class %s in schema", name)
		}
		return class, nil
	}
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
