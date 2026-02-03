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

package v1

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/auth"
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

var NUMCPU = runtime.GOMAXPROCS(0)

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

	authenticator      *auth.Handler
	batchHandler       *batch.Handler
	batchStreamHandler *batch.StreamHandler
}

func NewService(traverser *traverser.Traverser, authComposer composer.TokenFunc,
	allowAnonymousAccess bool, schemaManager *schemaManager.Manager,
	batchManager *objects.BatchManager, config *config.Config, authorization authorization.Authorizer,
	logger logrus.FieldLogger,
) (*Service, batch.Drain) {
	authenticator := auth.NewHandler(allowAnonymousAccess, authComposer)
	batchHandler := batch.NewHandler(authorization, batchManager, logger, authenticator, schemaManager)
	batchStreamHandler, batchDrain := batch.Start(authenticator, authorization, batchHandler, prometheus.DefaultRegisterer, 2*NUMCPU, logger)
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
		batchHandler:         batchHandler,
		batchStreamHandler:   batchStreamHandler,
	}, batchDrain
}

func (s *Service) Aggregate(ctx context.Context, req *pb.AggregateRequest) (*pb.AggregateReply, error) {
	var result *pb.AggregateReply
	var errInner error

	if class := s.schemaManager.ResolveAlias(req.Collection); class != "" {
		req.Collection = class
	}

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

	if class := s.schemaManager.ResolveAlias(req.Collection); class != "" {
		req.Collection = class
	}

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

	if class := s.schemaManager.ResolveAlias(req.Collection); class != "" {
		req.Collection = class
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

// BatchObjects handles end-to-end batch object creation. It accepts N objects in the request and forwards them to the internal
// batch objects logic. It blocks until a response is retrieved from the internal APIs whereupon it returns the response to the client.
//
// It is intended to be used in isolation and therefore is not dependent on BatchSend/BatchStream.
func (s *Service) BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
	var result *pb.BatchObjectsReply
	var errInner error

	if err := enterrors.GoWrapperWithBlock(func() {
		result, errInner = s.batchHandler.BatchObjects(ctx, req)
	}, s.logger); err != nil {
		return nil, err
	}

	return result, errInner
}

// BatchObjects handles end-to-end batch reference creation. It accepts N references in the request and forwards them to the internal
// batch references logic. It blocks until a response is retrieved from the internal APIs whereupon it returns the response to the client.
//
// It is intended to be used in isolation and therefore is not dependent on BatchSend/BatchStream.
func (s *Service) BatchReferences(ctx context.Context, req *pb.BatchReferencesRequest) (*pb.BatchReferencesReply, error) {
	var result *pb.BatchReferencesReply
	var errInner error

	if err := enterrors.GoWrapperWithBlock(func() {
		result, errInner = s.batchHandler.BatchReferences(ctx, req)
	}, s.logger); err != nil {
		return nil, err
	}

	return result, errInner
}

// BatchStream defines a StreamStream gRPC method whereby the server streams messages back to the client in order to
// asynchronously report on any errors that have occurred during the automatic batching process.
//
// The initial request contains the consistency level that is desired when batch inserting in this processing context.
//
// The first message send to the client contains the stream ID for the overall stream. All subsequent messages, besides the final one,
// correspond to errors emitted by the internal batching APIs, e.g. validation errors of the objects/references. The final
// message sent to the client is a confirmation that the batch processing has completed successfully and that the client can hangup.
//
// In addition, there is also the shutdown logic that is sent via the stream from the server to the client. In the event that
// the node handling the batch processing must be shutdown, e.g. there's a rolling restart occurring on the cluster, then the
// stream will notify the client that it is shutting down allowing for all the internal queues to be drained and waited on. Once the final
// shutdown message is sent and received by the client, the client can then safely hangup and reconnect to the cluster in an effort to
// reconnect to a different available node. At that point, the batching process resumes on the other node as if nothing happened.
//
// It should be used as part of the automatic batching process provided in clients.
func (s *Service) BatchStream(stream pb.Weaviate_BatchStreamServer) error {
	return s.batchStreamHandler.Handle(stream)
}

func (s *Service) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchReply, error) {
	var result *pb.SearchReply
	var errInner error

	if class := s.schemaManager.ResolveAlias(req.Collection); class != "" {
		req.Collection = class
	}

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

	if searchParams.Cursor != nil {
		// Preserve existing ShardCursors if they were parsed from the request
		existingShardCursors := make(map[string]string)
		if searchParams.IteratorState != nil && len(searchParams.IteratorState.ShardCursors) > 0 {
			existingShardCursors = searchParams.IteratorState.ShardCursors
		}
		searchParams.IteratorState = &dto.IteratorState{
			ShardCursors: existingShardCursors,
		}
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
