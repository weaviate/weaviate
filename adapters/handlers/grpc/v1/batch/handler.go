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

package batch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/auth"
	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/versioned"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

type Handler struct {
	authorizer        authorization.Authorizer
	authenticator     *auth.Handler
	batchManager      *objects.BatchManager
	logger            logrus.FieldLogger
	schemaManager     *schema.Manager
	namespacesEnabled bool
}

func NewHandler(authorizer authorization.Authorizer, batchManager *objects.BatchManager, logger logrus.FieldLogger, authenticator *auth.Handler, schemaManager *schema.Manager, namespacesEnabled bool) *Handler {
	return &Handler{
		authorizer:        authorizer,
		authenticator:     authenticator,
		batchManager:      batchManager,
		logger:            logger,
		schemaManager:     schemaManager,
		namespacesEnabled: namespacesEnabled,
	}
}

func (h *Handler) BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (reply *pb.BatchObjectsReply, retErr error) {
	before := time.Now()
	principal, err := h.authenticator.PrincipalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}
	defer func() { retErr = namespacing.StripErrForPrincipal(principal, retErr) }()
	ctx = restCtx.AddPrincipalToContext(ctx, principal)
	ctx = classcache.ContextWithClassCache(ctx)

	// we need to save the class two times:
	// - to check if we already authorized the class+shard combination and if yes skip the auth, this is indexed by
	//   a combination of class+shard
	// - to pass down the stack to reuse, index by classname so it can be found easily
	knownClasses := map[string]versioned.Class{}
	knownClassesAuthCheck := map[string]*models.Class{}
	classGetter := func(classname, shard string) (*models.Class, error) {
		resolved, _, err := namespacing.Resolve(principal, h.schemaManager, h.namespacesEnabled, classname)
		if err != nil {
			return nil, err
		}
		classname = resolved
		// use a letter that cannot be in class/shard name to not allow different combinations leading to the same combined name
		classTenantName := classname + "#" + shard
		class, ok := knownClassesAuthCheck[classTenantName]
		if ok {
			return class, nil
		}

		// batch is upsert
		if err := h.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.ShardsData(classname, shard)...); err != nil {
			return nil, err
		}

		if err := h.authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.ShardsData(classname, shard)...); err != nil {
			return nil, err
		}

		// we don't leak any info that someone who inserts data does not have anyway
		vClass, err := h.schemaManager.GetCachedClassNoAuth(ctx, classname)
		if err != nil {
			return nil, err
		}
		knownClasses[classname] = vClass[classname]
		knownClassesAuthCheck[classTenantName] = vClass[classname].Class
		return vClass[classname].Class, nil
	}
	objs, objOriginalIndex, objectParsingErrors, conditionals := BatchObjectsFromProto(req, classGetter)

	var objErrors []*pb.BatchObjectsReply_BatchError
	for i, err := range objectParsingErrors {
		objErrors = append(objErrors, &pb.BatchObjectsReply_BatchError{Index: int32(i), Error: namespacing.StripErrorMessage(principal, err.Error())})
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

	response, err := h.batchManager.AddObjectsGRPCAfterAuth(ctx, principal, objs, replicationProperties, knownClasses, conditionals)
	if err != nil {
		return nil, err
	}

	var objectResults []*pb.BatchObjectsReply_ObjectResult
	for i, obj := range response {
		if obj.Err != nil {
			var pf *objects.ErrPreconditionFailed
			if errors.As(obj.Err, &pf) {
				// Conditional precondition not met: surface as a per-object
				// ConditionalWriteOutcome rather than a top-level batch error,
				// so clients can inspect individual outcomes without treating the
				// whole batch as failed.
				outcome := conditionalOutcomeFromErr(pf, conditionals, i)
				msg := pf.Reason
				objectResults = append(objectResults, &pb.BatchObjectsReply_ObjectResult{
					Uuid: obj.UUID.String(),
					ConditionalResult: &pb.ConditionalWriteReply{
						Outcome:      outcome,
						ErrorMessage: &msg,
					},
				})
			} else {
				objErrors = append(objErrors, &pb.BatchObjectsReply_BatchError{
					Index: int32(objOriginalIndex[i]),
					Error: namespacing.StripErrorMessage(principal, obj.Err.Error()),
				})
			}
		} else if i < len(conditionals) && !conditionals[i].IsZero() {
			// Conditional write that succeeded: surface the INSERTED outcome.
			objectResults = append(objectResults, &pb.BatchObjectsReply_ObjectResult{
				Uuid: obj.UUID.String(),
				ConditionalResult: &pb.ConditionalWriteReply{
					Outcome: pb.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_INSERTED,
				},
			})
		}
	}

	result := &pb.BatchObjectsReply{
		Took:          float32(time.Since(before).Seconds()),
		Errors:        objErrors,
		ObjectResults: objectResults,
	}
	return result, nil
}

// conditionalOutcomeFromErr maps an ErrPreconditionFailed to the proto
// ConditionalWriteOutcome enum. It inspects the Reason string set by the shard
// to determine whether the insert_if_not_exists condition fired (SKIPPED) or
// another condition failed (CONDITION_FAILED).
func conditionalOutcomeFromErr(pf *objects.ErrPreconditionFailed, conditionals []storobj.Conditional, idx int) pb.ConditionalWriteOutcome {
	if idx < len(conditionals) && conditionals[idx].OnlyIfNotExists {
		// insert_if_not_exists: object already existed — idempotent skip.
		return pb.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_SKIPPED
	}
	return pb.ConditionalWriteOutcome_CONDITIONAL_WRITE_OUTCOME_CONDITION_FAILED
}

func (h *Handler) BatchReferences(ctx context.Context, req *pb.BatchReferencesRequest) (reply *pb.BatchReferencesReply, retErr error) {
	before := time.Now()
	principal, err := h.authenticator.PrincipalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("extract auth: %w", err)
	}
	defer func() { retErr = namespacing.StripErrForPrincipal(principal, retErr) }()
	ctx = restCtx.AddPrincipalToContext(ctx, principal)
	replProps := extractReplicationProperties(req.ConsistencyLevel)

	response, err := h.batchManager.AddReferences(ctx, principal, BatchReferencesFromProto(req), replProps)
	if err != nil {
		return nil, err
	}

	var refErrors []*pb.BatchReferencesReply_BatchError
	for i, ref := range response {
		if ref.Err != nil {
			refErrors = append(refErrors, &pb.BatchReferencesReply_BatchError{Index: int32(i), Error: namespacing.StripErrorMessage(principal, ref.Err.Error())})
		}
	}

	result := &pb.BatchReferencesReply{
		Took:   float32(time.Since(before).Seconds()),
		Errors: refErrors,
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
