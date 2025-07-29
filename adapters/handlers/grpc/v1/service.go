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
	"time"

	"github.com/google/uuid"
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
	traverser            *traverser.Traverser
	authComposer         composer.TokenFunc
	allowAnonymousAccess bool
	schemaManager        *schemaManager.Manager
	batchManager         *objects.BatchManager
	config               *config.Config
	authorizer           authorization.Authorizer
	logger               logrus.FieldLogger

	authenticator       *authHandler
	batchObjectsHandler *batch.ObjectsHandler
	batchHandler        *batch.Handler
	// batchWorkers        []*batch.Worker
}

func NewService(traverser *traverser.Traverser, authComposer composer.TokenFunc,
	allowAnonymousAccess bool, schemaManager *schemaManager.Manager,
	batchManager *objects.BatchManager, config *config.Config, authorization authorization.Authorizer,
	logger logrus.FieldLogger, grpcShutdownCtx context.Context,
) *Service {
	authenticator := NewAuthHandler(allowAnonymousAccess, authComposer)
	batchWriteQueue := batch.NewBatchWriteQueue()
	batchReadQueues := batch.NewBatchReadQueues()
	batchObjectsHandler := batch.NewObjectsHandler(authorization, batchManager, logger, authenticator, schemaManager)
	batch.StartBatchWorkers(grpcShutdownCtx, 1, batchWriteQueue, batchReadQueues, batchObjectsHandler, logger)

	return &Service{
		traverser:            traverser,
		authComposer:         authComposer,
		allowAnonymousAccess: allowAnonymousAccess,
		schemaManager:        schemaManager,
		batchManager:         batchManager,
		config:               config,
		logger:               logger,
		authorizer:           authorization,
		authenticator:        authenticator,
		batchObjectsHandler:  batchObjectsHandler,
		batchHandler:         batch.NewHandler(grpcShutdownCtx, batchWriteQueue, batchReadQueues, logger),
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
	ctx = restCtx.AddPrincipalToContext(ctx, principal)

	parser := NewAggregateParser(
		s.classGetterWithAuthzFunc(ctx, principal, req.Tenant),
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
		s.classGetterWithAuthzFunc(ctx, principal, req.Tenant),
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
	ctx = restCtx.AddPrincipalToContext(ctx, principal)

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
	ctx = restCtx.AddPrincipalToContext(ctx, principal)

	replicationProperties := extractReplicationProperties(req.ConsistencyLevel)

	tenant := ""
	if req.Tenant != nil {
		tenant = *req.Tenant
	}

	if err := s.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.ShardsData(req.Collection, tenant)...); err != nil {
		return nil, err
	}

	params, err := batchDeleteParamsFromProto(req, s.classGetterWithAuthzFunc(ctx, principal, tenant))
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

func (s *Service) BatchSend(ctx context.Context, req *pb.BatchSendRequest) (*pb.BatchSendReply, error) {
	var result *pb.BatchSendReply
	var errInner error

	if err := enterrors.GoWrapperWithBlock(func() {
		next := s.batchHandler.Send(ctx, req)
		result = &pb.BatchSendReply{Next: next}
	}, s.logger); err != nil {
		return nil, err
	}

	return result, errInner
}

func (s *Service) BatchStream(req *pb.BatchStreamRequest, stream pb.Weaviate_BatchStreamServer) error {
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	streamId := id.String()
	s.batchHandler.Setup(streamId)
	defer s.batchHandler.Teardown(streamId)
	return s.batchHandler.Stream(stream.Context(), streamId, stream)
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
	ctx = restCtx.AddPrincipalToContext(ctx, principal)

	parser := NewParser(
		req.Uses_127Api,
		s.classGetterWithAuthzFunc(ctx, principal, req.Tenant),
		s.aliasGetter(),
	)
	replier := NewReplier(
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

type classGetterWithAuthzFunc func(string) (*models.Class, error)

func (s *Service) classGetterWithAuthzFunc(ctx context.Context, principal *models.Principal, tenant string) classGetterWithAuthzFunc {
	authorizedCollections := map[string]*models.Class{}

	return func(name string) (*models.Class, error) {
		classTenantName := name + "#" + tenant
		class, ok := authorizedCollections[classTenantName]
		if !ok {
			resources := authorization.CollectionsData(name)
			if tenant != "" {
				resources = authorization.ShardsData(name, tenant)
			}
			// having data access is enough for querying as we dont leak any info from the collection config that you cannot get via data access anyways
			if err := s.authorizer.Authorize(ctx, principal, authorization.READ, resources...); err != nil {
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

type aliasGetter func(string) string

func (s *Service) aliasGetter() aliasGetter {
	return func(name string) string {
		if cls := s.schemaManager.ResolveAlias(name); cls != "" {
			return name // name is an alias
		}
		return ""
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
