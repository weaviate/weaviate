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

// WeaviateModelManifestsListHandlerFunc turns a function with the right signature into a weaviate model manifests list handler
type WeaviateModelManifestsListHandlerFunc func(WeaviateModelManifestsListParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateModelManifestsListHandlerFunc) Handle(params WeaviateModelManifestsListParams) middleware.Responder {
	return fn(params)
}

// WeaviateModelManifestsListHandler interface for that can handle valid weaviate model manifests list params
type WeaviateModelManifestsListHandler interface {
	Handle(WeaviateModelManifestsListParams) middleware.Responder
}

// NewWeaviateModelManifestsList creates a new http.Handler for the weaviate model manifests list operation
func NewWeaviateModelManifestsList(ctx *middleware.Context, handler WeaviateModelManifestsListHandler) *WeaviateModelManifestsList {
	return &WeaviateModelManifestsList{Context: ctx, Handler: handler}
}

/*WeaviateModelManifestsList swagger:route GET /modelManifests modelManifests weaviateModelManifestsList

Lists all model manifests.

*/
type WeaviateModelManifestsList struct {
	Context *middleware.Context
	Handler WeaviateModelManifestsListHandler
}

func (o *WeaviateModelManifestsList) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateModelManifestsListParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
