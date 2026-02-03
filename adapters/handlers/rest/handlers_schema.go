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
	"errors"
	"fmt"
	"net/http"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"

	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/entities/models"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/namespace"
	uco "github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type schemaHandlers struct {
	manager             *schemaUC.Manager
	metricRequestsTotal restApiRequestsTotal
}

// getNamespaceFromRequest extracts the namespace for the request.
// The namespace comes from:
// 1. The authenticated principal's bound namespace (for non-admin users)
// 2. The X-Weaviate-Namespace header (only for admin users or anonymous access)
// Returns the default namespace if neither is set.
func getNamespaceFromRequest(r *http.Request, principal *models.Principal) (string, error) {
	headerNs := r.Header.Get(namespace.HeaderName)

	// Get namespace from auth principal (may allow header override if admin)
	ns := GetNamespaceForPrincipal(principal, headerNs)

	// Validate the final namespace
	if ns != namespace.DefaultNamespace {
		if err := namespace.ValidateNamespace(ns); err != nil {
			return "", err
		}
	}
	return ns, nil
}

func (s *schemaHandlers) addClass(params schema.SchemaObjectsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewSchemaObjectsCreateUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Store original class name for response
	originalClassName := params.ObjectClass.Class
	// Prefix class name with namespace for internal storage
	params.ObjectClass.Class = namespace.PrefixClassName(ns, params.ObjectClass.Class)

	_, _, err = s.manager.AddClass(ctx, principal, params.ObjectClass)
	if err != nil {
		s.metricRequestsTotal.logError(originalClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	// Restore original class name for response
	params.ObjectClass.Class = originalClassName
	s.metricRequestsTotal.logOk(originalClassName)
	return schema.NewSchemaObjectsCreateOK().WithPayload(params.ObjectClass)
}

func (s *schemaHandlers) updateClass(params schema.SchemaObjectsUpdateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewSchemaObjectsUpdateUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Store original class name for response
	originalClassName := params.ClassName
	originalBodyClassName := params.ObjectClass.Class
	// Prefix class names with namespace for internal storage
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)
	params.ObjectClass.Class = namespace.PrefixClassName(ns, params.ObjectClass.Class)

	err = s.manager.UpdateClass(ctx, principal, prefixedClassName,
		params.ObjectClass)
	if err != nil {
		s.metricRequestsTotal.logError(originalClassName, err)
		if errors.Is(err, schemaUC.ErrNotFound) {
			return schema.NewSchemaObjectsUpdateNotFound()
		}

		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	// Restore original class name for response
	params.ObjectClass.Class = originalBodyClassName
	s.metricRequestsTotal.logOk(originalClassName)
	return schema.NewSchemaObjectsUpdateOK().WithPayload(params.ObjectClass)
}

func (s *schemaHandlers) getClass(params schema.SchemaObjectsGetParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewSchemaObjectsGetInternalServerError().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Prefix class name with namespace for lookup
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)

	class, _, err := s.manager.GetConsistentClass(ctx, principal, prefixedClassName, *params.Consistency)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	if class == nil {
		s.metricRequestsTotal.logUserError(params.ClassName)
		return schema.NewSchemaObjectsGetNotFound()
	}

	// Strip namespace prefix from class name in response
	class.Class = namespace.StripNamespacePrefix(class.Class)
	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsGetOK().WithPayload(class)
}

func (s *schemaHandlers) deleteClass(params schema.SchemaObjectsDeleteParams, principal *models.Principal) middleware.Responder {
	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewSchemaObjectsDeleteBadRequest().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Prefix class name with namespace for deletion
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)

	err = s.manager.DeleteClass(params.HTTPRequest.Context(), principal, prefixedClassName)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsDeleteOK()
}

func (s *schemaHandlers) addClassProperty(params schema.SchemaObjectsPropertiesAddParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewSchemaObjectsPropertiesAddUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Prefix class name with namespace
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)

	_, _, err = s.manager.AddClassProperty(ctx, principal, s.manager.ReadOnlyClass(prefixedClassName), prefixedClassName, false, params.Body)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsPropertiesAddForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsPropertiesAddUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsPropertiesAddOK().WithPayload(params.Body)
}

func (s *schemaHandlers) getSchema(params schema.SchemaDumpParams, principal *models.Principal) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewSchemaDumpForbidden().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Get schema with RBAC filtering applied
	// Note: For production, RBAC would need to be namespace-aware so permissions
	// can be defined per collection rather than per namespace-prefixed name.
	dbSchema, err := s.manager.GetConsistentSchema(ctx, principal, *params.Consistency)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaDumpForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaDumpInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	// Filter classes by namespace and strip prefix
	var filteredClasses []*models.Class
	for _, class := range dbSchema.Objects.Classes {
		if namespace.BelongsToNamespace(class.Class, ns) {
			// Strip namespace prefix from class name
			class.Class = namespace.StripNamespacePrefix(class.Class)
			filteredClasses = append(filteredClasses, class)
		}
	}

	payload := &models.Schema{
		Classes: filteredClasses,
	}

	s.metricRequestsTotal.logOk("")
	return schema.NewSchemaDumpOK().WithPayload(payload)
}

func (s *schemaHandlers) getShardsStatus(params schema.SchemaObjectsShardsGetParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewSchemaObjectsShardsGetNotFound().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Prefix class name with namespace
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)

	var tenant string
	if params.Tenant == nil {
		tenant = ""
	} else {
		tenant = *params.Tenant
	}

	status, err := s.manager.ShardsStatus(ctx, principal, prefixedClassName, tenant)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsShardsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsShardsGetNotFound().
				WithPayload(errPayloadFromSingleErr(err))
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

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewSchemaObjectsShardsUpdateUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Prefix class name with namespace
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)

	_, err = s.manager.UpdateShardStatus(
		ctx, principal, prefixedClassName, params.ShardName, params.Body.Status)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewSchemaObjectsShardsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsShardsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
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

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewTenantsCreateUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Prefix class name with namespace
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)

	_, err = s.manager.AddTenants(
		ctx, principal, prefixedClassName, params.Body)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewTenantsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsCreateOK().WithPayload(params.Body)
}

func (s *schemaHandlers) updateTenants(params schema.TenantsUpdateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewTenantsUpdateUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Prefix class name with namespace
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)

	updatedTenants, err := s.manager.UpdateTenants(
		ctx, principal, prefixedClassName, params.Body)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewTenantsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsUpdateOK().WithPayload(updatedTenants)
}

func (s *schemaHandlers) deleteTenants(params schema.TenantsDeleteParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewTenantsDeleteUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Prefix class name with namespace
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)

	err = s.manager.DeleteTenants(
		ctx, principal, prefixedClassName, params.Tenants)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewTenantsDeleteUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsDeleteOK()
}

func (s *schemaHandlers) getTenants(params schema.TenantsGetParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewTenantsGetUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Prefix class name with namespace
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)

	tenants, err := s.manager.GetConsistentTenants(ctx, principal, prefixedClassName, *params.Consistency, nil)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewTenantsGetUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
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

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewTenantsGetOneUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Prefix class name with namespace
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)

	tenant, err := s.manager.GetConsistentTenant(ctx, principal, prefixedClassName, *params.Consistency, params.TenantName)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		if errors.Is(err, schemaUC.ErrNotFound) {
			return schema.NewTenantsGetOneNotFound()
		}
		if errors.Is(err, schemaUC.ErrUnexpectedMultiple) {
			return schema.NewTenantsGetOneInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantsGetOneForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewTenantsGetOneUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}
	if tenant == nil {
		s.metricRequestsTotal.logUserError(params.ClassName)
		return schema.NewTenantsGetOneUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(fmt.Errorf("tenant '%s' not found when it should have been", params.TenantName)))
	}
	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsGetOneOK().WithPayload(tenant)
}

func (s *schemaHandlers) tenantExists(params schema.TenantExistsParams, principal *models.Principal) middleware.Responder {
	ctx := restCtx.AddPrincipalToContext(params.HTTPRequest.Context(), principal)

	ns, err := getNamespaceFromRequest(params.HTTPRequest, principal)
	if err != nil {
		return schema.NewTenantExistsUnprocessableEntity().
			WithPayload(errPayloadFromSingleErr(err))
	}

	// Prefix class name with namespace
	prefixedClassName := namespace.PrefixClassName(ns, params.ClassName)

	if err := s.manager.ConsistentTenantExists(ctx, principal, prefixedClassName, *params.Consistency, params.TenantName); err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		if errors.Is(err, schemaUC.ErrNotFound) {
			return schema.NewTenantExistsNotFound()
		}
		switch {
		case errors.As(err, &authzerrors.Forbidden{}):
			return schema.NewTenantExistsForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewTenantExistsUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return schema.NewTenantExistsOK()
}

func setupSchemaHandlers(api *operations.WeaviateAPI, manager *schemaUC.Manager, metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) {
	h := &schemaHandlers{manager, newSchemaRequestsTotal(metrics, logger)}

	api.SchemaSchemaObjectsCreateHandler = schema.
		SchemaObjectsCreateHandlerFunc(h.addClass)
	api.SchemaSchemaObjectsDeleteHandler = schema.
		SchemaObjectsDeleteHandlerFunc(h.deleteClass)
	api.SchemaSchemaObjectsPropertiesAddHandler = schema.
		SchemaObjectsPropertiesAddHandlerFunc(h.addClassProperty)

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
