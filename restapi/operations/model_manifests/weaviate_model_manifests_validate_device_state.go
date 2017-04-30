/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package model_manifests


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateModelManifestsValidateDeviceStateHandlerFunc turns a function with the right signature into a weaviate model manifests validate device state handler
type WeaviateModelManifestsValidateDeviceStateHandlerFunc func(WeaviateModelManifestsValidateDeviceStateParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateModelManifestsValidateDeviceStateHandlerFunc) Handle(params WeaviateModelManifestsValidateDeviceStateParams) middleware.Responder {
	return fn(params)
}

// WeaviateModelManifestsValidateDeviceStateHandler interface for that can handle valid weaviate model manifests validate device state params
type WeaviateModelManifestsValidateDeviceStateHandler interface {
	Handle(WeaviateModelManifestsValidateDeviceStateParams) middleware.Responder
}

// NewWeaviateModelManifestsValidateDeviceState creates a new http.Handler for the weaviate model manifests validate device state operation
func NewWeaviateModelManifestsValidateDeviceState(ctx *middleware.Context, handler WeaviateModelManifestsValidateDeviceStateHandler) *WeaviateModelManifestsValidateDeviceState {
	return &WeaviateModelManifestsValidateDeviceState{Context: ctx, Handler: handler}
}

/*WeaviateModelManifestsValidateDeviceState swagger:route POST /modelManifests/validateDeviceState modelManifests weaviateModelManifestsValidateDeviceState

Validates given device state object and returns errors.

*/
type WeaviateModelManifestsValidateDeviceState struct {
	Context *middleware.Context
	Handler WeaviateModelManifestsValidateDeviceStateHandler
}

func (o *WeaviateModelManifestsValidateDeviceState) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateModelManifestsValidateDeviceStateParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
