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

//	_       _
//
// __      _____  __ ___   ___  __ _| |_ ___
//
//	\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//	 \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//	  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//	 Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//	 CONTACT: hello@weaviate.io
package batch

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaManager "github.com/weaviate/weaviate/usecases/schema"
)

type ObjectsHandler struct {
	authorizer    authorization.Authorizer
	authenticator authenticator
	batchManager  *objects.BatchManager
	logger        logrus.FieldLogger
	schemaManager *schemaManager.Manager
}

type authenticator interface {
	PrincipalFromContext(ctx context.Context) (*models.Principal, error)
}

func NewObjectsHandler(authorizer authorization.Authorizer, batchManager *objects.BatchManager, logger logrus.FieldLogger, authenticator authenticator, schemaManager *schemaManager.Manager) *ObjectsHandler {
	return &ObjectsHandler{
		authorizer:    authorizer,
		authenticator: authenticator,
		batchManager:  batchManager,
		logger:        logger,
		schemaManager: schemaManager,
	}
}

func (h *ObjectsHandler) BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
	before := time.Now()
	principal, err := h.authenticator.PrincipalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}
	ctx = classcache.ContextWithClassCache(ctx)

	// we need to save the class two times:
	// - to check if we already authorized the class+shard combination and if yes skip the auth, this is indexed by
	//   a combination of class+shard
	// - to pass down the stack to reuse, index by classname so it can be found easily
	knownClasses := map[string]versioned.Class{}
	knownClassesAuthCheck := map[string]*models.Class{}
	classGetter := func(classname, shard string) (*models.Class, error) {
		// use a letter that cannot be in class/shard name to not allow different combinations leading to the same combined name
		classTenantName := classname + "#" + shard
		class, ok := knownClassesAuthCheck[classTenantName]
		if ok {
			return class, nil
		}

		// batch is upsert
		if err := h.authorizer.Authorize(principal, authorization.UPDATE, authorization.ShardsData(classname, shard)...); err != nil {
			return nil, err
		}

		if err := h.authorizer.Authorize(principal, authorization.CREATE, authorization.ShardsData(classname, shard)...); err != nil {
			return nil, err
		}
		vClass, err := h.schemaManager.GetCachedClass(ctx, principal, classname)
		if err != nil {
			return nil, err
		}
		knownClasses[classname] = vClass[classname]
		knownClassesAuthCheck[classTenantName] = vClass[classname].Class
		return vClass[classname].Class, nil
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
	response, err := h.batchManager.AddObjectsGRPCAfterAuth(ctx, principal, objs, []*string{&all}, replicationProperties, knownClasses)
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
