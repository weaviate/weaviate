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

// WeaviateModelManifestsDeleteHandlerFunc turns a function with the right signature into a weaviate model manifests delete handler
type WeaviateModelManifestsDeleteHandlerFunc func(WeaviateModelManifestsDeleteParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateModelManifestsDeleteHandlerFunc) Handle(params WeaviateModelManifestsDeleteParams) middleware.Responder {
	return fn(params)
}

// WeaviateModelManifestsDeleteHandler interface for that can handle valid weaviate model manifests delete params
type WeaviateModelManifestsDeleteHandler interface {
	Handle(WeaviateModelManifestsDeleteParams) middleware.Responder
}

// NewWeaviateModelManifestsDelete creates a new http.Handler for the weaviate model manifests delete operation
func NewWeaviateModelManifestsDelete(ctx *middleware.Context, handler WeaviateModelManifestsDeleteHandler) *WeaviateModelManifestsDelete {
	return &WeaviateModelManifestsDelete{Context: ctx, Handler: handler}
}

/*WeaviateModelManifestsDelete swagger:route DELETE /modelManifests/{modelManifestId} modelManifests weaviateModelManifestsDelete

Deletes a particular model manifest.

*/
type WeaviateModelManifestsDelete struct {
	Context *middleware.Context
	Handler WeaviateModelManifestsDeleteHandler
}

func (o *WeaviateModelManifestsDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateModelManifestsDeleteParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
