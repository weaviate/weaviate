//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package rest

import (
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/actions"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/operations/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization/errors"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/semi-technologies/weaviate/usecases/telemetry"
)

type kindHandlers struct {
	manager     *kinds.Manager
	requestsLog *telemetry.RequestsLog
}

func (h *kindHandlers) addThing(params things.ThingsCreateParams,
	principal *models.Principal) middleware.Responder {
	thing, err := h.manager.AddThing(params.HTTPRequest.Context(), principal, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return things.NewThingsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return things.NewThingsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return things.NewThingsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAdd)
	return things.NewThingsCreateOK().WithPayload(thing)
}

func (h *kindHandlers) validateThing(params things.ThingsValidateParams,
	principal *models.Principal) middleware.Responder {

	err := h.manager.ValidateThing(params.HTTPRequest.Context(), principal, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return things.NewThingsValidateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return things.NewThingsValidateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return things.NewThingsValidateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalQueryMeta)
	return things.NewThingsValidateOK()
}

func (h *kindHandlers) addAction(params actions.ActionsCreateParams,
	principal *models.Principal) middleware.Responder {
	action, err := h.manager.AddAction(params.HTTPRequest.Context(), principal, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return actions.NewActionsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return actions.NewActionsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return actions.NewActionsCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalAdd)
	return actions.NewActionsCreateOK().WithPayload(action)
}

func (h *kindHandlers) validateAction(params actions.ActionsValidateParams,
	principal *models.Principal) middleware.Responder {

	err := h.manager.ValidateAction(params.HTTPRequest.Context(), principal, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return actions.NewActionsValidateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return actions.NewActionsValidateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return actions.NewActionsValidateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalQueryMeta)
	return actions.NewActionsValidateOK()
}

func (h *kindHandlers) getThing(params things.ThingsGetParams,
	principal *models.Principal) middleware.Responder {
	var includeMeta bool
	if params.Meta != nil && *params.Meta {
		includeMeta = true
	}
	thing, err := h.manager.GetThing(params.HTTPRequest.Context(), principal, params.ID, includeMeta)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return things.NewThingsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrNotFound:
			return things.NewThingsGetNotFound()
		default:
			return things.NewThingsGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalQuery)
	return things.NewThingsGetOK().WithPayload(thing)
}

func (h *kindHandlers) getAction(params actions.ActionsGetParams,
	principal *models.Principal) middleware.Responder {
	var includeMeta bool
	if params.Meta != nil && *params.Meta {
		includeMeta = true
	}
	action, err := h.manager.GetAction(params.HTTPRequest.Context(), principal, params.ID, includeMeta)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return actions.NewActionsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrNotFound:
			return actions.NewActionsGetNotFound()
		default:
			return actions.NewActionsGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalQuery)
	return actions.NewActionsGetOK().WithPayload(action)
}

func (h *kindHandlers) getThings(params things.ThingsListParams,
	principal *models.Principal) middleware.Responder {
	list, err := h.manager.GetThings(params.HTTPRequest.Context(), principal, params.Limit)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return things.NewThingsListForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return things.NewThingsListInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalQuery)
	return things.NewThingsListOK().
		WithPayload(&models.ThingsListResponse{
			Things:       list,
			TotalResults: int64(len(list)),
		})
}

func (h *kindHandlers) getActions(params actions.ActionsListParams,
	principal *models.Principal) middleware.Responder {
	list, err := h.manager.GetActions(params.HTTPRequest.Context(), principal, params.Limit)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return actions.NewActionsListForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return actions.NewActionsListInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalQuery)
	return actions.NewActionsListOK().
		WithPayload(&models.ActionsListResponse{
			Actions:      list,
			TotalResults: int64(len(list)),
		})
}

func (h *kindHandlers) updateThing(params things.ThingsUpdateParams,
	principal *models.Principal) middleware.Responder {
	thing, err := h.manager.UpdateThing(params.HTTPRequest.Context(), principal, params.ID, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return things.NewThingsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return things.NewThingsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return things.NewThingsUpdateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return things.NewThingsUpdateOK().WithPayload(thing)
}

func (h *kindHandlers) updateAction(params actions.ActionsUpdateParams,
	principal *models.Principal) middleware.Responder {
	action, err := h.manager.UpdateAction(params.HTTPRequest.Context(), principal, params.ID, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return actions.NewActionsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return actions.NewActionsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return actions.NewActionsUpdateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return actions.NewActionsUpdateOK().WithPayload(action)
}

func (h *kindHandlers) deleteThing(params things.ThingsDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := h.manager.DeleteThing(params.HTTPRequest.Context(), principal, params.ID)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return things.NewThingsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrNotFound:
			return things.NewThingsDeleteNotFound()
		default:
			return things.NewThingsDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return things.NewThingsDeleteNoContent()
}

func (h *kindHandlers) deleteAction(params actions.ActionsDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := h.manager.DeleteAction(params.HTTPRequest.Context(), principal, params.ID)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return actions.NewActionsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrNotFound:
			return actions.NewActionsDeleteNotFound()
		default:
			return actions.NewActionsDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return actions.NewActionsDeleteNoContent()
}

func (h *kindHandlers) patchThing(params things.ThingsPatchParams, principal *models.Principal) middleware.Responder {

	err := h.manager.MergeThing(params.HTTPRequest.Context(), principal, params.ID, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return things.NewThingsPatchForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return things.NewThingsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return things.NewThingsUpdateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)

	// TODO payload
	return things.NewThingsPatchOK().WithPayload(nil)
}

func (h *kindHandlers) patchAction(params actions.ActionsPatchParams, principal *models.Principal) middleware.Responder {
	err := h.manager.MergeAction(params.HTTPRequest.Context(), principal, params.ID, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return actions.NewActionsPatchForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrInvalidUserInput:
			return actions.NewActionsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return actions.NewActionsUpdateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)

	// TODO payload
	return actions.NewActionsPatchOK().WithPayload(nil)
}

func (h *kindHandlers) addThingReference(params things.ThingsReferencesCreateParams,
	principal *models.Principal) middleware.Responder {
	err := h.manager.AddThingReference(params.HTTPRequest.Context(), principal, params.ID, params.PropertyName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return things.NewThingsReferencesCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrNotFound, kinds.ErrInvalidUserInput:
			return things.NewThingsReferencesCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return things.NewThingsReferencesCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return things.NewThingsReferencesCreateOK()
}

func (h *kindHandlers) addActionReference(params actions.ActionsReferencesCreateParams,
	principal *models.Principal) middleware.Responder {
	err := h.manager.AddActionReference(params.HTTPRequest.Context(), principal, params.ID, params.PropertyName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return actions.NewActionsReferencesCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrNotFound, kinds.ErrInvalidUserInput:
			return actions.NewActionsReferencesCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return actions.NewActionsReferencesCreateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return actions.NewActionsReferencesCreateOK()
}

func (h *kindHandlers) updateActionReferences(params actions.ActionsReferencesUpdateParams,
	principal *models.Principal) middleware.Responder {
	err := h.manager.UpdateActionReferences(params.HTTPRequest.Context(), principal, params.ID, params.PropertyName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return actions.NewActionsReferencesUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrNotFound, kinds.ErrInvalidUserInput:
			return actions.NewActionsReferencesUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return actions.NewActionsReferencesUpdateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return actions.NewActionsReferencesUpdateOK()
}

func (h *kindHandlers) updateThingReferences(params things.ThingsReferencesUpdateParams,
	principal *models.Principal) middleware.Responder {
	err := h.manager.UpdateThingReferences(params.HTTPRequest.Context(), principal, params.ID, params.PropertyName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return things.NewThingsReferencesUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrNotFound, kinds.ErrInvalidUserInput:
			return things.NewThingsReferencesUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return things.NewThingsReferencesUpdateInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return things.NewThingsReferencesUpdateOK()
}

func (h *kindHandlers) deleteActionReference(params actions.ActionsReferencesDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := h.manager.DeleteActionReference(params.HTTPRequest.Context(), principal, params.ID, params.PropertyName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return actions.NewActionsReferencesDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrNotFound, kinds.ErrInvalidUserInput:
			return actions.NewActionsReferencesDeleteNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return actions.NewActionsReferencesDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return actions.NewActionsReferencesDeleteNoContent()
}

func (h *kindHandlers) deleteThingReference(params things.ThingsReferencesDeleteParams,
	principal *models.Principal) middleware.Responder {
	err := h.manager.DeleteThingReference(params.HTTPRequest.Context(), principal, params.ID, params.PropertyName, params.Body)
	if err != nil {
		switch err.(type) {
		case errors.Forbidden:
			return things.NewThingsReferencesDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case kinds.ErrNotFound, kinds.ErrInvalidUserInput:
			return things.NewThingsReferencesDeleteNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return things.NewThingsReferencesDeleteInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	h.telemetryLogAsync(telemetry.TypeREST, telemetry.LocalManipulate)
	return things.NewThingsReferencesDeleteNoContent()
}

func setupKindHandlers(api *operations.WeaviateAPI, requestsLog *telemetry.RequestsLog, manager *kinds.Manager) {
	h := &kindHandlers{manager, requestsLog}

	api.ThingsThingsCreateHandler = things.
		ThingsCreateHandlerFunc(h.addThing)
	api.ThingsThingsValidateHandler = things.
		ThingsValidateHandlerFunc(h.validateThing)
	api.ThingsThingsGetHandler = things.
		ThingsGetHandlerFunc(h.getThing)
	api.ThingsThingsDeleteHandler = things.
		ThingsDeleteHandlerFunc(h.deleteThing)
	api.ThingsThingsListHandler = things.
		ThingsListHandlerFunc(h.getThings)
	api.ThingsThingsUpdateHandler = things.
		ThingsUpdateHandlerFunc(h.updateThing)
	api.ThingsThingsPatchHandler = things.
		ThingsPatchHandlerFunc(h.patchThing)
	api.ThingsThingsReferencesCreateHandler = things.
		ThingsReferencesCreateHandlerFunc(h.addThingReference)
	api.ThingsThingsReferencesDeleteHandler = things.
		ThingsReferencesDeleteHandlerFunc(h.deleteThingReference)
	api.ThingsThingsReferencesUpdateHandler = things.
		ThingsReferencesUpdateHandlerFunc(h.updateThingReferences)

	api.ActionsActionsCreateHandler = actions.
		ActionsCreateHandlerFunc(h.addAction)
	api.ActionsActionsValidateHandler = actions.
		ActionsValidateHandlerFunc(h.validateAction)
	api.ActionsActionsGetHandler = actions.
		ActionsGetHandlerFunc(h.getAction)
	api.ActionsActionsDeleteHandler = actions.
		ActionsDeleteHandlerFunc(h.deleteAction)
	api.ActionsActionsListHandler = actions.
		ActionsListHandlerFunc(h.getActions)
	api.ActionsActionsUpdateHandler = actions.
		ActionsUpdateHandlerFunc(h.updateAction)
	api.ActionsActionsPatchHandler = actions.
		ActionsPatchHandlerFunc(h.patchAction)
	api.ActionsActionsReferencesCreateHandler = actions.
		ActionsReferencesCreateHandlerFunc(h.addActionReference)
	api.ActionsActionsReferencesDeleteHandler = actions.
		ActionsReferencesDeleteHandlerFunc(h.deleteActionReference)
	api.ActionsActionsReferencesUpdateHandler = actions.
		ActionsReferencesUpdateHandlerFunc(h.updateActionReferences)

}

func (h *kindHandlers) telemetryLogAsync(requestType, identifier string) {
	go func() {
		h.requestsLog.Register(requestType, identifier)
	}()
}
