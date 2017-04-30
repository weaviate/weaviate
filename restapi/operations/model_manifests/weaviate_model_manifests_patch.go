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

// WeaviateModelManifestsPatchHandlerFunc turns a function with the right signature into a weaviate model manifests patch handler
type WeaviateModelManifestsPatchHandlerFunc func(WeaviateModelManifestsPatchParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateModelManifestsPatchHandlerFunc) Handle(params WeaviateModelManifestsPatchParams) middleware.Responder {
	return fn(params)
}

// WeaviateModelManifestsPatchHandler interface for that can handle valid weaviate model manifests patch params
type WeaviateModelManifestsPatchHandler interface {
	Handle(WeaviateModelManifestsPatchParams) middleware.Responder
}

// NewWeaviateModelManifestsPatch creates a new http.Handler for the weaviate model manifests patch operation
func NewWeaviateModelManifestsPatch(ctx *middleware.Context, handler WeaviateModelManifestsPatchHandler) *WeaviateModelManifestsPatch {
	return &WeaviateModelManifestsPatch{Context: ctx, Handler: handler}
}

/*WeaviateModelManifestsPatch swagger:route PATCH /modelManifests/{modelManifestId} modelManifests weaviateModelManifestsPatch

Updates a particular model manifest.

*/
type WeaviateModelManifestsPatch struct {
	Context *middleware.Context
	Handler WeaviateModelManifestsPatchHandler
}

func (o *WeaviateModelManifestsPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateModelManifestsPatchParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
