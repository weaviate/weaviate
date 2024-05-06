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

	"github.com/sirupsen/logrus"
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
	logger               logrus.FieldLogger
}

func NewService(traverser *traverser.Traverser, authComposer composer.TokenFunc,
	allowAnonymousAccess bool, schemaManager *schemaManager.Manager,
	batchManager *objects.BatchManager, config *config.Config, logger logrus.FieldLogger,
) *Service {
	return &Service{
		traverser:            traverser,
		authComposer:         authComposer,
		allowAnonymousAccess: allowAnonymousAccess,
		schemaManager:        schemaManager,
		batchManager:         batchManager,
		config:               config,
		logger:               logger,
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

	params, err := batchDeleteParamsFromProto(req, s.schemaManager.ReadOnlyClass)
	if err != nil {
		return nil, fmt.Errorf("batch delete params: %w", err)
	}

	tenant := ""
	if req.Tenant != nil {
		tenant = *req.Tenant
	}

	response, err := s.batchManager.DeleteObjectsFromGRPC(ctx, principal, params, replicationProperties, tenant)
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
	objs, objOriginalIndex, objectParsingErrors := batchFromProto(req, s.schemaManager.ReadOnlyClass)

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

	scheme := s.schemaManager.GetSchemaSkipAuth()
	replier := NewReplier(req.Uses_123Api, req.Uses_125Api)

	searchParams, err := searchParamsFromProto(req, s.schemaManager.ReadOnlyClass, s.config)
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
