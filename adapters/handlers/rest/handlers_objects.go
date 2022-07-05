//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package rest

import (
	"context"
	"fmt"
	"strings"

	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/objects"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	"github.com/semi-technologies/weaviate/usecases/config"
	uco "github.com/semi-technologies/weaviate/usecases/objects"
	"github.com/sirupsen/logrus"
)

type objectHandlers struct {
	manager         objectsManager
	logger          logrus.FieldLogger
	config          config.Config
	modulesProvider ModulesProvider
}

type ModulesProvider interface {
	RestApiAdditionalProperties(includeProp string, class *models.Class) map[string]interface{}
	GetMeta() (map[string]interface{}, error)
	HasMultipleVectorizers() bool
}

type objectsManager interface {
	AddObject(context.Context, *models.Principal, *models.Object) (*models.Object, error)
	ValidateObject(context.Context, *models.Principal, *models.Object) error
	GetObject(_ context.Context, _ *models.Principal, class string, _ strfmt.UUID, _ additional.Properties) (*models.Object, error)
	DeleteObject(_ context.Context, _ *models.Principal, class string, _ strfmt.UUID) error
	UpdateObject(_ context.Context, _ *models.Principal, class string, _ strfmt.UUID, _ *models.Object) (*models.Object, error)
	HeadObject(_ context.Context, _ *models.Principal, class string, _ strfmt.UUID) (bool, *uco.Error)
	GetObjects(context.Context, *models.Principal, *int64, *int64, *string, *string, additional.Properties) ([]*models.Object, error)
	MergeObject(context.Context, *models.Principal, *models.Object) *uco.Error
	AddObjectReference(context.Context, *models.Principal, *uco.AddReferenceInput) *uco.Error
	UpdateObjectReferences(context.Context, *models.Principal, *uco.PutReferenceInput) *uco.Error
	DeleteObjectReference(context.Context, *models.Principal, *uco.DeleteReferenceInput) *uco.Error
	GetObjectsClass(ctx context.Context, principal *models.Principal, id strfmt.UUID) (*models.Class, error)
}

func (h *objectHandlers) addObject(params objects.ObjectsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	object, err := h.manager.AddObject(params.HTTPRequest.Context(), principal, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return objects.NewObjectsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case uco.ErrInvalidUserInput:
			return objects.NewObjectsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return objects.NewObjectsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	propertiesMap, ok := object.Properties.(map[string]interface{})
	if ok {
		object.Properties = h.extendPropertiesWithAPILinks(propertiesMap)
	}

	return objects.NewObjectsCreateOK().WithPayload(object)
}

func (h *objectHandlers) validateObject(params objects.ObjectsValidateParams,
	principal *models.Principal,
) middleware.Responder {
	err := h.manager.ValidateObject(params.HTTPRequest.Context(), principal, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return objects.NewObjectsValidateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case uco.ErrInvalidUserInput:
			return objects.NewObjectsValidateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return objects.NewObjectsValidateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return objects.NewObjectsValidateOK()
}

// getObject gets object of a specific class
func (h *objectHandlers) getObject(params objects.ObjectsClassGetParams,
	principal *models.Principal,
) middleware.Responder {
	var additional additional.Properties

	// The process to extract additional params depends on knowing the schema
	// which in turn requires a preflight load of the object. We can save this
	// second db request if we know that the user did not specify any additional
	// params. This could potentially be optimized further by checking if only
	// non-module specific params are contained and decide then, but we do not
	// know if this path is critical enough for this level of optimization.
	if params.Include != nil {
		class, err := h.manager.GetObjectsClass(params.HTTPRequest.Context(), principal, params.ID)
		if err != nil {
			return objects.NewObjectsClassGetBadRequest().
				WithPayload(errPayloadFromSingleErr(err))
		}

		additional, err = parseIncludeParam(params.Include, h.modulesProvider, true, class)
		if err != nil {
			return objects.NewObjectsClassGetBadRequest().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	object, err := h.manager.GetObject(params.HTTPRequest.Context(), principal, params.ClassName, params.ID, additional)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return objects.NewObjectsClassGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case uco.ErrNotFound:
			return objects.NewObjectsClassGetNotFound()
		default:
			return objects.NewObjectsClassGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	propertiesMap, ok := object.Properties.(map[string]interface{})
	if ok {
		object.Properties = h.extendPropertiesWithAPILinks(propertiesMap)
	}

	return objects.NewObjectsClassGetOK().WithPayload(object)
}

func (h *objectHandlers) getObjects(params objects.ObjectsListParams,
	principal *models.Principal,
) middleware.Responder {
	additional, err := parseIncludeParam(params.Include, h.modulesProvider, h.shouldIncludeGetObjectsModuleParams(), nil)
	if err != nil {
		return objects.NewObjectsListBadRequest().
			WithPayload(errPayloadFromSingleErr(err))
	}

	var deprecationsRes []*models.Deprecation

	list, err := h.manager.GetObjects(params.HTTPRequest.Context(), principal,
		params.Offset, params.Limit, params.Sort, params.Order, additional)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return objects.NewObjectsListForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return objects.NewObjectsListInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	for i, object := range list {
		propertiesMap, ok := object.Properties.(map[string]interface{})
		if ok {
			list[i].Properties = h.extendPropertiesWithAPILinks(propertiesMap)
		}
	}

	return objects.NewObjectsListOK().
		WithPayload(&models.ObjectsListResponse{
			Objects:      list,
			TotalResults: int64(len(list)),
			Deprecations: deprecationsRes,
		})
}

// deleteObject delete a single object of giving class
func (h *objectHandlers) deleteObject(params objects.ObjectsClassDeleteParams,
	principal *models.Principal,
) middleware.Responder {
	err := h.manager.DeleteObject(params.HTTPRequest.Context(), principal, params.ClassName, params.ID)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return objects.NewObjectsClassDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case uco.ErrNotFound:
			return objects.NewObjectsClassDeleteNotFound()
		default:
			return objects.NewObjectsClassDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return objects.NewObjectsClassDeleteNoContent()
}

func (h *objectHandlers) updateObject(params objects.ObjectsClassPutParams,
	principal *models.Principal,
) middleware.Responder {
	object, err := h.manager.UpdateObject(params.HTTPRequest.Context(), principal, params.ClassName, params.ID, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return objects.NewObjectsClassPutForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case uco.ErrInvalidUserInput:
			return objects.NewObjectsClassPutUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		case uco.ErrNotFound:
			return objects.NewObjectsClassDeleteNotFound()
		default:
			return objects.NewObjectsClassPutInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	propertiesMap, ok := object.Properties.(map[string]interface{})
	if ok {
		object.Properties = h.extendPropertiesWithAPILinks(propertiesMap)
	}

	return objects.NewObjectsClassPutOK().WithPayload(object)
}

func (h *objectHandlers) headObject(r objects.ObjectsClassHeadParams,
	principal *models.Principal,
) middleware.Responder {
	ok, err := h.manager.HeadObject(r.HTTPRequest.Context(), principal, r.ClassName, r.ID)
	if err != nil {
		switch {
		case err.Forbidden():
			return objects.NewObjectsClassHeadForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return objects.NewObjectsClassHeadInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	if !ok {
		return objects.NewObjectsClassHeadNotFound()
	}
	return objects.NewObjectsClassHeadNoContent()
}

func (h *objectHandlers) patchObject(params objects.ObjectsClassPatchParams, principal *models.Principal) middleware.Responder {
	updates := params.Body
	updates.ID = params.ID
	updates.Class = params.ClassName
	err := h.manager.MergeObject(params.HTTPRequest.Context(), principal, updates)
	if err != nil {
		switch {
		case err.NotFound():
			return objects.NewObjectsClassPatchNotFound()
		case err.Forbidden():
			return objects.NewObjectsClassPatchForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case err.BadRequest():
			return objects.NewObjectsClassPatchUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return objects.NewObjectsClassPatchInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return objects.NewObjectsClassPatchNoContent()
}

func (h *objectHandlers) addObjectReference(
	params objects.ObjectsClassReferencesCreateParams,
	principal *models.Principal,
) middleware.Responder {
	input := uco.AddReferenceInput{
		Class:    params.ClassName,
		ID:       params.ID,
		Property: params.PropertyName,
		Ref:      *params.Body,
	}
	err := h.manager.AddObjectReference(params.HTTPRequest.Context(), principal, &input)
	if err != nil {
		switch {
		case err.Forbidden():
			return objects.NewObjectsClassReferencesCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case err.NotFound():
			return objects.NewObjectsClassReferencesCreateNotFound()
		case err.BadRequest():
			return objects.NewObjectsClassReferencesCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return objects.NewObjectsClassReferencesCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return objects.NewObjectsClassReferencesCreateOK()
}

func (h *objectHandlers) putObjectReferences(params objects.ObjectsClassReferencesPutParams,
	principal *models.Principal,
) middleware.Responder {
	input := uco.PutReferenceInput{
		Class:    params.ClassName,
		ID:       params.ID,
		Property: params.PropertyName,
		Refs:     params.Body,
	}
	err := h.manager.UpdateObjectReferences(params.HTTPRequest.Context(), principal, &input)
	if err != nil {
		switch {
		case err.Forbidden():
			return objects.NewObjectsClassReferencesPutForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case err.NotFound():
			return objects.NewObjectsClassReferencesPutNotFound()
		case err.BadRequest():
			return objects.NewObjectsClassReferencesPutUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return objects.NewObjectsClassReferencesPutInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return objects.NewObjectsClassReferencesPutOK()
}

func (h *objectHandlers) deleteObjectReference(params objects.ObjectsClassReferencesDeleteParams,
	principal *models.Principal,
) middleware.Responder {
	input := uco.DeleteReferenceInput{
		Class:     params.ClassName,
		ID:        params.ID,
		Property:  params.PropertyName,
		Reference: *params.Body,
	}
	err := h.manager.DeleteObjectReference(params.HTTPRequest.Context(), principal, &input)
	if err != nil {
		switch err.Code {
		case uco.StatusForbidden:
			return objects.NewObjectsClassReferencesDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case uco.StatusNotFound:
			return objects.NewObjectsClassReferencesDeleteNotFound()
		case uco.StatusBadRequest:
			return objects.NewObjectsClassReferencesDeleteUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return objects.NewObjectsClassReferencesDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return objects.NewObjectsClassReferencesDeleteNoContent()
}

func setupObjectHandlers(api *operations.WeaviateAPI,
	manager *uco.Manager, config config.Config, logger logrus.FieldLogger,
	modulesProvider ModulesProvider,
) {
	h := &objectHandlers{manager, logger, config, modulesProvider}
	api.ObjectsObjectsCreateHandler = objects.
		ObjectsCreateHandlerFunc(h.addObject)
	api.ObjectsObjectsValidateHandler = objects.
		ObjectsValidateHandlerFunc(h.validateObject)
	api.ObjectsObjectsClassGetHandler = objects.
		ObjectsClassGetHandlerFunc(h.getObject)
	api.ObjectsObjectsClassHeadHandler = objects.
		ObjectsClassHeadHandlerFunc(h.headObject)
	api.ObjectsObjectsClassDeleteHandler = objects.
		ObjectsClassDeleteHandlerFunc(h.deleteObject)
	api.ObjectsObjectsListHandler = objects.
		ObjectsListHandlerFunc(h.getObjects)
	api.ObjectsObjectsClassPutHandler = objects.
		ObjectsClassPutHandlerFunc(h.updateObject)
	api.ObjectsObjectsClassPatchHandler = objects.
		ObjectsClassPatchHandlerFunc(h.patchObject)
	api.ObjectsObjectsClassReferencesCreateHandler = objects.
		ObjectsClassReferencesCreateHandlerFunc(h.addObjectReference)
	api.ObjectsObjectsClassReferencesDeleteHandler = objects.
		ObjectsClassReferencesDeleteHandlerFunc(h.deleteObjectReference)
	api.ObjectsObjectsClassReferencesPutHandler = objects.
		ObjectsClassReferencesPutHandlerFunc(h.putObjectReferences)
	// deprecated handlers
	api.ObjectsObjectsGetHandler = objects.
		ObjectsGetHandlerFunc(h.getObjectDeprecated)
	api.ObjectsObjectsDeleteHandler = objects.
		ObjectsDeleteHandlerFunc(h.deleteObjectDeprecated)
	api.ObjectsObjectsHeadHandler = objects.
		ObjectsHeadHandlerFunc(h.headObjectDeprecated)
	api.ObjectsObjectsUpdateHandler = objects.
		ObjectsUpdateHandlerFunc(h.updateObjectDeprecated)
	api.ObjectsObjectsPatchHandler = objects.
		ObjectsPatchHandlerFunc(h.patchObjectDeprecated)
	api.ObjectsObjectsReferencesCreateHandler = objects.
		ObjectsReferencesCreateHandlerFunc(h.addObjectReferenceDeprecated)
	api.ObjectsObjectsReferencesUpdateHandler = objects.
		ObjectsReferencesUpdateHandlerFunc(h.updateObjectReferencesDeprecated)
	api.ObjectsObjectsReferencesDeleteHandler = objects.
		ObjectsReferencesDeleteHandlerFunc(h.deleteObjectReferenceDeprecated)
}

func (h *objectHandlers) getObjectDeprecated(params objects.ObjectsGetParams,
	principal *models.Principal,
) middleware.Responder {
	ps := objects.ObjectsClassGetParams{
		HTTPRequest: params.HTTPRequest,
		ID:          params.ID,
		Include:     params.Include,
	}
	return h.getObject(ps, principal)
}

func (h *objectHandlers) headObjectDeprecated(params objects.ObjectsHeadParams,
	principal *models.Principal,
) middleware.Responder {
	r := objects.ObjectsClassHeadParams{
		HTTPRequest: params.HTTPRequest,
		ID:          params.ID,
	}
	return h.headObject(r, principal)
}

func (h *objectHandlers) patchObjectDeprecated(params objects.ObjectsPatchParams, principal *models.Principal) middleware.Responder {
	args := objects.ObjectsClassPatchParams{
		HTTPRequest: params.HTTPRequest,
		ID:          params.ID,
		Body:        params.Body,
	}
	if params.Body != nil {
		args.ClassName = params.Body.Class
	}
	return h.patchObject(args, principal)
}

func (h *objectHandlers) updateObjectDeprecated(params objects.ObjectsUpdateParams,
	principal *models.Principal,
) middleware.Responder {
	ps := objects.ObjectsClassPutParams{
		HTTPRequest: params.HTTPRequest,
		ClassName:   params.Body.Class,
		Body:        params.Body,
		ID:          params.ID,
	}
	return h.updateObject(ps, principal)
}

func (h *objectHandlers) deleteObjectDeprecated(params objects.ObjectsDeleteParams,
	principal *models.Principal,
) middleware.Responder {
	ps := objects.ObjectsClassDeleteParams{
		HTTPRequest: params.HTTPRequest,
		ID:          params.ID,
	}
	return h.deleteObject(ps, principal)
}

func (h *objectHandlers) addObjectReferenceDeprecated(params objects.ObjectsReferencesCreateParams,
	principal *models.Principal,
) middleware.Responder {
	req := objects.ObjectsClassReferencesCreateParams{
		HTTPRequest:  params.HTTPRequest,
		Body:         params.Body,
		ID:           params.ID,
		PropertyName: params.PropertyName,
	}
	return h.addObjectReference(req, principal)
}

func (h *objectHandlers) updateObjectReferencesDeprecated(params objects.ObjectsReferencesUpdateParams,
	principal *models.Principal,
) middleware.Responder {
	req := objects.ObjectsClassReferencesPutParams{
		HTTPRequest:  params.HTTPRequest,
		ID:           params.ID,
		PropertyName: params.PropertyName,
		Body:         params.Body,
	}
	return h.putObjectReferences(req, principal)
}

func (h *objectHandlers) deleteObjectReferenceDeprecated(params objects.ObjectsReferencesDeleteParams,
	principal *models.Principal,
) middleware.Responder {
	req := objects.ObjectsClassReferencesDeleteParams{
		HTTPRequest:  params.HTTPRequest,
		Body:         params.Body,
		ID:           params.ID,
		PropertyName: params.PropertyName,
	}
	return h.deleteObjectReference(req, principal)
}

func (h *objectHandlers) extendPropertiesWithAPILinks(schema map[string]interface{}) map[string]interface{} {
	if schema == nil {
		return schema
	}

	for key, value := range schema {
		asMultiRef, ok := value.(models.MultipleRef)
		if !ok {
			continue
		}

		schema[key] = h.extendReferencesWithAPILinks(asMultiRef)
	}
	return schema
}

func (h *objectHandlers) extendReferencesWithAPILinks(refs models.MultipleRef) models.MultipleRef {
	for i, ref := range refs {
		refs[i] = h.extendReferenceWithAPILink(ref)
	}

	return refs
}

func (h *objectHandlers) extendReferenceWithAPILink(ref *models.SingleRef) *models.SingleRef {
	parsed, err := crossref.Parse(ref.Beacon.String())
	if err != nil {
		// ignore return unchanged
		return ref
	}
	href := fmt.Sprintf("%s/v1/objects/%s/%s", h.config.Origin, parsed.Class, parsed.TargetID)
	if parsed.Class == "" {
		href = fmt.Sprintf("%s/v1/objects/%s", h.config.Origin, parsed.TargetID)
	}
	ref.Href = strfmt.URI(href)
	return ref
}

func (h *objectHandlers) shouldIncludeGetObjectsModuleParams() bool {
	if h.modulesProvider == nil || !h.modulesProvider.HasMultipleVectorizers() {
		return true
	}
	return false
}

func parseIncludeParam(in *string, modulesProvider ModulesProvider, includeModuleParams bool,
	class *models.Class,
) (additional.Properties, error) {
	out := additional.Properties{}
	if in == nil {
		return out, nil
	}

	parts := strings.Split(*in, ",")

	for _, prop := range parts {
		if prop == "classification" {
			out.Classification = true
			out.RefMeta = true
			continue
		}
		if prop == "vector" {
			out.Vector = true
			continue
		}
		if includeModuleParams && modulesProvider != nil {
			moduleParams := modulesProvider.RestApiAdditionalProperties(prop, class)
			if len(moduleParams) > 0 {
				out.ModuleParams = getModuleParams(out.ModuleParams)
				for param, value := range moduleParams {
					out.ModuleParams[param] = value
				}
				continue
			}
		}
		return out, fmt.Errorf("unrecognized property '%s' in ?include list", prop)
	}

	return out, nil
}

func getModuleParams(moduleParams map[string]interface{}) map[string]interface{} {
	if moduleParams == nil {
		return map[string]interface{}{}
	}
	return moduleParams
}
