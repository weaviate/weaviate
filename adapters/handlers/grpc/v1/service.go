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

	"github.com/liutizhong/weaviate/entities/models"
	"github.com/liutizhong/weaviate/usecases/auth/authorization"

	"github.com/sirupsen/logrus"
	enterrors "github.com/liutizhong/weaviate/entities/errors"

	"github.com/liutizhong/weaviate/usecases/config"

	"github.com/liutizhong/weaviate/usecases/objects"

	"github.com/liutizhong/weaviate/entities/additional"
	"github.com/liutizhong/weaviate/entities/dto"
	"github.com/liutizhong/weaviate/entities/schema"
	pb "github.com/liutizhong/weaviate/grpc/generated/protocol/v1"
	"github.com/liutizhong/weaviate/usecases/auth/authentication/composer"
	schemaManager "github.com/liutizhong/weaviate/usecases/schema"
	"github.com/liutizhong/weaviate/usecases/traverser"
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
}

func NewService(traverser *traverser.Traverser, authComposer composer.TokenFunc,
	allowAnonymousAccess bool, schemaManager *schemaManager.Manager,
	batchManager *objects.BatchManager, config *config.Config, authorization authorization.Authorizer,
	logger logrus.FieldLogger,
) *Service {
	return &Service{
		traverser:            traverser,
		authComposer:         authComposer,
		allowAnonymousAccess: allowAnonymousAccess,
		schemaManager:        schemaManager,
		batchManager:         batchManager,
		config:               config,
		logger:               logger,
		authorizer:           authorization,
	}
}

func (s *Service) TenantsGet(ctx context.Context, req *pb.TenantsGetRequest) (*pb.TenantsGetReply, error) {
	before := time.Now()

	principal, err := s.principalFromContext(ctx)
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
	principal, err := s.principalFromContext(ctx)
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
		result, errInner = s.batchObjects(ctx, req)
	}, s.logger); err != nil {
		return nil, err
	}

	return result, errInner
}

func (s *Service) batchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
	before := time.Now()
	principal, err := s.principalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}

	knownClasses := map[string]*models.Class{}
	classGetter := func(classname, shard string) (*models.Class, error) {
		// use a letter that cannot be in class/shard name to not allow different combinations leading to the same combined name
		name := classname + "#" + shard
		class, ok := knownClasses[name]
		if ok {
			return class, nil
		}

		if err := s.authorizer.Authorize(principal, authorization.READ, authorization.ShardsMetadata(classname, shard)...); err != nil {
			return nil, err
		}

		// batch is upsert
		if err := s.authorizer.Authorize(principal, authorization.UPDATE, authorization.ShardsData(classname, shard)...); err != nil {
			return nil, err
		}

		if err := s.authorizer.Authorize(principal, authorization.CREATE, authorization.ShardsData(classname, shard)...); err != nil {
			return nil, err
		}
		class = s.schemaManager.ReadOnlyClass(classname)

		knownClasses[name] = class
		return class, nil
	}
	objs, objOriginalIndex, objectParsingErrors := BatchFromProto(req, classGetter)

	var objErrors []*pb.BatchObjectsReply_BatchError
	for i, err := range objectParsingErrors {
		objErrors = append(objErrors, &pb.BatchObjectsReply_BatchError{Index: int32(i), Error: err.Error()})
	}

	// If every object failed to parse, return early with the errors
	if len(objs) == 0 {
		result := &pb.BatchObjectsReply{
			Took:   float32(time.Since(before).Seconds()),
			Errors: objErrors,
		}
		return result, nil
	}

	replicationProperties := extractReplicationProperties(req.ConsistencyLevel)

	all := "ALL"
	response, err := s.batchManager.AddObjects(ctx, principal, objs, []*string{&all}, replicationProperties)
	if err != nil {
		return nil, err
	}

	for i, obj := range response {
		if obj.Err != nil {
			objErrors = append(objErrors, &pb.BatchObjectsReply_BatchError{Index: int32(objOriginalIndex[i]), Error: obj.Err.Error()})
		}
	}

	result := &pb.BatchObjectsReply{
		Took:   float32(time.Since(before).Seconds()),
		Errors: objErrors,
	}
	return result, nil
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

	principal, err := s.principalFromContext(ctx)
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

	res, err := s.traverser.GetClass(ctx, principal, searchParams)
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
	return func(name string) (*models.Class, error) {
		if err := s.authorizer.Authorize(principal, authorization.READ, authorization.Collections(name)...); err != nil {
			return nil, err
		}
		class := s.schemaManager.ReadOnlyClass(name)
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
