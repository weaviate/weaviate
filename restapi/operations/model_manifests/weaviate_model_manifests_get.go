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

// WeaviateModelManifestsGetHandlerFunc turns a function with the right signature into a weaviate model manifests get handler
type WeaviateModelManifestsGetHandlerFunc func(WeaviateModelManifestsGetParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateModelManifestsGetHandlerFunc) Handle(params WeaviateModelManifestsGetParams) middleware.Responder {
	return fn(params)
}

// WeaviateModelManifestsGetHandler interface for that can handle valid weaviate model manifests get params
type WeaviateModelManifestsGetHandler interface {
	Handle(WeaviateModelManifestsGetParams) middleware.Responder
}

// NewWeaviateModelManifestsGet creates a new http.Handler for the weaviate model manifests get operation
func NewWeaviateModelManifestsGet(ctx *middleware.Context, handler WeaviateModelManifestsGetHandler) *WeaviateModelManifestsGet {
	return &WeaviateModelManifestsGet{Context: ctx, Handler: handler}
}

/*WeaviateModelManifestsGet swagger:route GET /modelManifests/{modelManifestId} modelManifests weaviateModelManifestsGet

Returns a particular model manifest.

*/
type WeaviateModelManifestsGet struct {
	Context *middleware.Context
	Handler WeaviateModelManifestsGetHandler
}

func (o *WeaviateModelManifestsGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateModelManifestsGetParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
