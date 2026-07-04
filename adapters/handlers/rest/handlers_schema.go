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

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"

	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	uco "github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/restrictions"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// reindexInFlightChecker is the narrow interface schema handlers use
// to fail fast on property mutations that would conflict with an
// in-flight reindex migration. Implemented by the RAFT-backed
// distributed-task state (today: cluster/raft.Raft).
//
// This is the REST-handler UX layer of the mutation guard: the
// handler returns 409 Conflict before issuing the RAFT command, so
// operators see a clean error instead of a downstream "apply rejected"
// surprise. The cluster-wide safety net lives at the schema FSM's
// UpdateProperty apply path via [cluster/schema.SchemaManager]'s
// [MutationGuard] — that is the load-bearing check; this one is a
// pre-flight optimization.
type reindexInFlightChecker interface {
	ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error)
}

// reindexSubmitLockProvider returns the per-(collection, property)
// mutex shared with the reindex-submit REST handler. This is the
// SAME lock acquired by indexesHandlers.submitLock — the sharing is
// load-bearing; see [state.ReindexSubmitLocks] godoc for the race
// the lock closes.
//
// We accept the interface form (rather than the concrete
// *state.ReindexSubmitLocks) so the schema handlers stay testable
// without dragging in the full appState graph.
type reindexSubmitLockProvider interface {
	SubmitLockFor(collection, property string) *sync.Mutex
}

type schemaHandlers struct {
	manager             *schemaUC.Manager
	metricRequestsTotal restApiRequestsTotal
	reindexTaskLister   reindexInFlightChecker
	reindexSubmitLocks  reindexSubmitLockProvider
	namespacesEnabled   bool
}

func (s *schemaHandlers) addClass(params schema.SchemaObjectsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	_, _, err := s.manager.AddClass(ctx, principal, params.ObjectClass)
	if err != nil {
		s.metricRequestsTotal.logError(params.ObjectClass.Class, err)
		if le, ok := usagelimits.AsLimitExceeded(err); ok {
			return schema.NewSchemaObjectsCreateTooManyRequests().
				WithPayload(newUsageLimitPayload(le))
		}
		if v, ok := restrictions.AsViolation(err); ok {
			return schema.NewSchemaObjectsCreateUnprocessableEntity().
				WithPayload(newRestrictionViolationPayload(v))
		}
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewSchemaObjectsCreateUnprocessableEntity().
				WithPayload(restrictionViolationFromErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.ObjectClass.Class)
	return schema.NewSchemaObjectsCreateOK().WithPayload(namespacing.StripClassResponse(principal, params.ObjectClass))
}

func (s *schemaHandlers) updateClass(params schema.SchemaObjectsUpdateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	err := s.manager.UpdateClass(ctx, principal, params.ClassName,
		params.ObjectClass)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		if errors.Is(err, schemaUC.ErrNotFound) {
			return schema.NewSchemaObjectsUpdateNotFound()
		}
		if v, ok := restrictions.AsViolation(err); ok {
			return schema.NewSchemaObjectsUpdateUnprocessableEntity().
				WithPayload(newRestrictionViolationPayload(v))
		}

		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewSchemaObjectsUpdateUnprocessableEntity().
				WithPayload(restrictionViolationFromErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsUpdateOK().WithPayload(namespacing.StripClassResponse(principal, params.ObjectClass))
}

func (s *schemaHandlers) getClass(params schema.SchemaObjectsGetParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	class, _, err := s.manager.GetConsistentClass(ctx, principal, params.ClassName, *params.Consistency)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsGetForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		case errors.Is(err, schemaUC.ErrValidation):
			return schema.NewSchemaObjectsGetUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewSchemaObjectsGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	if class == nil {
		s.metricRequestsTotal.logUserError(params.ClassName)
		return schema.NewSchemaObjectsGetNotFound()
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsGetOK().WithPayload(namespacing.StripClassResponse(principal, class))
}

func (s *schemaHandlers) deleteClass(params schema.SchemaObjectsDeleteParams, principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteClass(params.HTTPRequest.Context(), principal, params.ClassName)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewSchemaObjectsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsDeleteOK()
}

func (s *schemaHandlers) addClassProperty(params schema.SchemaObjectsPropertiesAddParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	_, _, err := s.manager.AddClassProperty(ctx, principal, params.ClassName, false, params.Body)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		if v, ok := restrictions.AsViolation(err); ok {
			return schema.NewSchemaObjectsPropertiesAddUnprocessableEntity().
				WithPayload(newRestrictionViolationPayload(v))
		}
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsPropertiesAddForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewSchemaObjectsPropertiesAddUnprocessableEntity().
				WithPayload(restrictionViolationFromErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsPropertiesAddOK().WithPayload(namespacing.StripPropertyResponse(principal, params.Body))
}

func (s *schemaHandlers) deleteClassPropertyIndex(params schema.SchemaObjectsPropertiesDeleteParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	// Conflict check and submit lock key on the qualified class (the reindex-task
	// key); the manager delete call qualifies internally, so it gets the raw name.
	qualifiedClass, qErr := namespacing.QualifyClass(principal, s.namespacesEnabled, params.ClassName)
	if qErr != nil {
		s.metricRequestsTotal.logError(params.ClassName, qErr)
		return schema.NewSchemaObjectsPropertiesDeleteUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(principal, qErr))
	}

	// Serialize with the reindex-submit REST handler on the same
	// (collection, property) tuple. Without this lock, a parallel
	// PUT /v1/schema/{class}/indexes/{prop} (which submits a reindex
	// task) and this DELETE (which drops the canonical bucket) race
	// at the RAFT serializer: if DELETE's UpdateProperty commits
	// before the reindex's DistributedTaskAdd, the apply-time
	// MutationGuard cannot reject DELETE because no task is in-flight
	// yet, the bucket is dropped, and the reindex worker then fails
	// trying to swap into a missing canonical bucket — leaving a
	// torn filterable bucket on the shard. The
	// TestParallelConflictMatrix/change_tokenization_both__delete_searchable_parallel
	// matrix sub-test exercises exactly this race. See
	// state.ReindexSubmitLocks godoc for the cross-handler contract.
	//
	// nil-safe: reindexSubmitLocks is wired in production but may be
	// nil in unit tests that construct schemaHandlers directly.
	if s.reindexSubmitLocks != nil {
		lock := s.reindexSubmitLocks.SubmitLockFor(qualifiedClass, params.PropertyName)
		lock.Lock()
		defer lock.Unlock()
	}

	// REST-handler pre-flight: fail fast at the REST boundary if a
	// reindex on this (class, property) is in flight, so operators get
	// a clean 4xx instead of a downstream RAFT-apply rejection minutes
	// later. The cluster-wide safety net at the schema FSM's
	// UpdateProperty apply ([MutationGuard]) still closes the
	// multi-node race that this per-node check cannot — they are
	// complementary, not redundant.
	if conflict := s.checkReindexConflictForPropertyMutation(ctx, qualifiedClass, params.PropertyName); conflict != "" {
		s.metricRequestsTotal.logError(params.ClassName, fmt.Errorf("reindex conflict: %s", conflict))
		return schema.NewSchemaObjectsPropertiesDeleteUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(principal, fmt.Errorf("%s", conflict)))
	}

	err := s.manager.DeleteClassPropertyIndex(ctx, principal, params.ClassName, params.PropertyName, params.IndexName)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsPropertiesDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewSchemaObjectsPropertiesDeleteUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsPropertiesDeleteOK()
}

// checkReindexConflictForPropertyMutation is the REST-handler
// pre-flight for the mutation guard. Returns a non-empty conflict
// reason iff a reindex migration on (className, propertyName) is in
// any non-terminal state (STARTED, PREPARING, or SWAPPING) — same
// epistemics as the schema FSM's MutationGuard at apply time, just
// earlier in the request lifecycle for operator UX.
//
// Per-node, in-memory: two REST handlers on different nodes can both
// observe "no conflict" and both forward to RAFT — that's expected,
// the apply-time [MutationGuard] is what closes that multi-node
// race. This check exists purely to short-circuit the common
// single-node case with a clean 4xx instead of an apply-time
// rejection.
//
// Degrades gracefully: a TaskLister error returns "" (no conflict
// detected) so the request proceeds to RAFT and the apply-time guard
// handles correctness. We never spuriously reject due to a transient
// local error.
func (s *schemaHandlers) checkReindexConflictForPropertyMutation(ctx context.Context, className, propertyName string) string {
	if s.reindexTaskLister == nil {
		return ""
	}
	tasksByNamespace, err := s.reindexTaskLister.ListDistributedTasks(ctx)
	if err != nil {
		return ""
	}
	for _, task := range tasksByNamespace[db.ReindexNamespace] {
		// PREPARING and SWAPPING count as in-flight (via
		// [distributedtask.TaskStatus.IsActive]) — see the godoc on
		// [checkReindexConflict] for the full reasoning. Mutating the
		// property during either phase would race the in-flight per-
		// shard bucket-pointer flip.
		if !task.Status.IsActive() {
			continue
		}
		var payload db.ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			// Unparseable payload in flight is a hard reject reason on
			// the apply side; mirror that here so the REST caller
			// doesn't get a spurious "ok-then-FAILED" two-step.
			return fmt.Sprintf(
				"in-flight reindex task %q has unparseable payload; "+
					"refusing property mutation on %s.%s until the task "+
					"is inspected", task.ID, className, propertyName)
		}
		if !strings.EqualFold(payload.Collection, className) {
			continue
		}
		// Empty Properties means "all properties" (whole-collection
		// rebuild, reserved); treat as a match.
		matches := len(payload.Properties) == 0
		for _, p := range payload.Properties {
			if p == propertyName {
				matches = true
				break
			}
		}
		if !matches {
			continue
		}
		return fmt.Sprintf(
			"reindex task %q (%s) is in flight on %s.%s (status=%s); "+
				"schema mutations on this property are blocked until "+
				"the reindex completes or is cancelled — wait for the "+
				"task to reach a terminal state, or cancel it via the "+
				"reindex REST API before retrying",
			task.ID, payload.MigrationType, payload.Collection,
			propertyName, task.Status)
	}
	return ""
}

func (s *schemaHandlers) deleteClassVectorIndex(params schema.SchemaObjectsVectorsDeleteParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	err := s.manager.DeleteClassVectorIndex(ctx, principal, params.ClassName, params.VectorIndexName)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsVectorsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		case errors.Is(err, schemaUC.ErrNotFound):
			return schema.NewSchemaObjectsVectorsDeleteUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		case errors.Is(err, schemaUC.ErrValidation):
			return schema.NewSchemaObjectsVectorsDeleteUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewSchemaObjectsVectorsDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsVectorsDeleteOK()
}

func (s *schemaHandlers) getSchema(params schema.SchemaDumpParams, principal *models.Principal) middleware.Responder {
	dbSchema, err := s.manager.GetConsistentSchema(params.HTTPRequest.Context(), principal, *params.Consistency)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaDumpForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewSchemaDumpInternalServerError().WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	payload := dbSchema.Objects
	if principal != nil && principal.Namespace != "" && payload != nil && len(payload.Classes) > 0 {
		stripped := make([]*models.Class, len(payload.Classes))
		for i, c := range payload.Classes {
			stripped[i] = namespacing.StripClassResponse(principal, c)
		}
		copyPayload := *payload
		copyPayload.Classes = stripped
		payload = &copyPayload
	}

	s.metricRequestsTotal.logOk("")
	return schema.NewSchemaDumpOK().WithPayload(payload)
}

func (s *schemaHandlers) getShardsStatus(params schema.SchemaObjectsShardsGetParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	var tenant string
	if params.Tenant == nil {
		tenant = ""
	} else {
		tenant = *params.Tenant
	}

	status, err := s.manager.ShardsStatus(ctx, principal, params.ClassName, tenant)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsShardsGetForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		case errors.Is(err, schemaUC.ErrNotFound):
			return schema.NewSchemaObjectsShardsGetNotFound().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewSchemaObjectsShardsGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	payload := status

	s.metricRequestsTotal.logOk("")
	return schema.NewSchemaObjectsShardsGetOK().WithPayload(payload)
}

func (s *schemaHandlers) updateShardStatus(params schema.SchemaObjectsShardsUpdateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	_, err := s.manager.UpdateShardStatus(
		ctx, principal, params.ClassName, params.ShardName, params.Body.Status,
	)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsShardsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		case errors.Is(err, schemaUC.ErrNotFound):
			return schema.NewSchemaObjectsShardsUpdateNotFound().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewSchemaObjectsShardsUpdateInternalServerError().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	payload := params.Body

	s.metricRequestsTotal.logOk("")
	return schema.NewSchemaObjectsShardsUpdateOK().WithPayload(payload)
}

func (s *schemaHandlers) createTenants(params schema.TenantsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	_, err := s.manager.AddTenants(
		ctx, principal, params.ClassName, params.Body,
	)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		if le, ok := usagelimits.AsLimitExceeded(err); ok {
			return schema.NewTenantsCreateTooManyRequests().
				WithPayload(newUsageLimitPayload(le))
		}
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewTenantsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsCreateOK().WithPayload(params.Body)
}

func (s *schemaHandlers) updateTenants(params schema.TenantsUpdateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	updatedTenants, err := s.manager.UpdateTenants(
		ctx, principal, params.ClassName, params.Body,
	)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewTenantsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsUpdateOK().WithPayload(updatedTenants)
}

func (s *schemaHandlers) deleteTenants(params schema.TenantsDeleteParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	err := s.manager.DeleteTenants(
		ctx, principal, params.ClassName, params.Tenants,
	)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewTenantsDeleteUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsDeleteOK()
}

func (s *schemaHandlers) getTenants(params schema.TenantsGetParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	tenants, err := s.manager.GetConsistentTenants(ctx, principal, params.ClassName, *params.Consistency, nil)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantsGetForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewTenantsGetUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsGetOK().WithPayload(tenants)
}

func (s *schemaHandlers) getTenant(
	params schema.TenantsGetOneParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	tenant, err := s.manager.GetConsistentTenant(ctx, principal, params.ClassName, *params.Consistency, params.TenantName)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		if errors.Is(err, schemaUC.ErrNotFound) {
			return schema.NewTenantsGetOneNotFound()
		}
		if errors.Is(err, schemaUC.ErrUnexpectedMultiple) {
			return schema.NewTenantsGetOneInternalServerError().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantsGetOneForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewTenantsGetOneUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}
	if tenant == nil {
		s.metricRequestsTotal.logUserError(params.ClassName)
		return schema.NewTenantsGetOneUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(principal, fmt.Errorf("tenant '%s' not found when it should have been", params.TenantName)))
	}
	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsGetOneOK().WithPayload(tenant)
}

func (s *schemaHandlers) tenantExists(params schema.TenantExistsParams, principal *models.Principal) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)
	if err := s.manager.ConsistentTenantExists(ctx, principal, params.ClassName, *params.Consistency, params.TenantName); err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		if errors.Is(err, schemaUC.ErrNotFound) {
			return schema.NewTenantExistsNotFound()
		}
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantExistsForbidden().
				WithPayload(errPayloadFromSingleErr(principal, err))
		default:
			return schema.NewTenantExistsUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(principal, err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantExistsOK()
}

func setupSchemaHandlers(api *operations.WeaviateAPI, manager *schemaUC.Manager, metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger, reindexTaskLister reindexInFlightChecker, reindexSubmitLocks reindexSubmitLockProvider, namespacesEnabled bool) {
	h := &schemaHandlers{
		manager:             manager,
		metricRequestsTotal: newSchemaRequestsTotal(metrics, logger),
		reindexTaskLister:   reindexTaskLister,
		reindexSubmitLocks:  reindexSubmitLocks,
		namespacesEnabled:   namespacesEnabled,
	}

	api.SchemaSchemaObjectsCreateHandler = schema.
		SchemaObjectsCreateHandlerFunc(h.addClass)
	api.SchemaSchemaObjectsDeleteHandler = schema.
		SchemaObjectsDeleteHandlerFunc(h.deleteClass)
	api.SchemaSchemaObjectsPropertiesAddHandler = schema.
		SchemaObjectsPropertiesAddHandlerFunc(h.addClassProperty)
	api.SchemaSchemaObjectsPropertiesDeleteHandler = schema.
		SchemaObjectsPropertiesDeleteHandlerFunc(h.deleteClassPropertyIndex)
	api.SchemaSchemaObjectsVectorsDeleteHandler = schema.
		SchemaObjectsVectorsDeleteHandlerFunc(h.deleteClassVectorIndex)

	api.SchemaSchemaObjectsUpdateHandler = schema.
		SchemaObjectsUpdateHandlerFunc(h.updateClass)

	api.SchemaSchemaObjectsGetHandler = schema.
		SchemaObjectsGetHandlerFunc(h.getClass)
	api.SchemaSchemaDumpHandler = schema.
		SchemaDumpHandlerFunc(h.getSchema)

	api.SchemaSchemaObjectsShardsGetHandler = schema.
		SchemaObjectsShardsGetHandlerFunc(h.getShardsStatus)
	api.SchemaSchemaObjectsShardsUpdateHandler = schema.
		SchemaObjectsShardsUpdateHandlerFunc(h.updateShardStatus)

	api.SchemaTenantsCreateHandler = schema.TenantsCreateHandlerFunc(h.createTenants)
	api.SchemaTenantsUpdateHandler = schema.TenantsUpdateHandlerFunc(h.updateTenants)
	api.SchemaTenantsDeleteHandler = schema.TenantsDeleteHandlerFunc(h.deleteTenants)
	api.SchemaTenantsGetHandler = schema.TenantsGetHandlerFunc(h.getTenants)
	api.SchemaTenantExistsHandler = schema.TenantExistsHandlerFunc(h.tenantExists)
	api.SchemaTenantsGetOneHandler = schema.TenantsGetOneHandlerFunc(h.getTenant)
}

type schemaRequestsTotal struct {
	*restApiRequestsTotalImpl
}

func newSchemaRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) restApiRequestsTotal {
	return &schemaRequestsTotal{
		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{newRequestsTotalMetric(metrics, "rest"), "rest", "schema", logger},
	}
}

func (e *schemaRequestsTotal) logError(className string, err error) {
	switch {
	case errors.As(err, &authzerrors.Forbidden{}):
		e.logUserError(className)
	case errors.As(err, &uco.ErrMultiTenancy{}):
		e.logUserError(className)
	default:
		e.logUserError(className)
	}
}
