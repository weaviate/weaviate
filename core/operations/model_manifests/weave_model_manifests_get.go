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
 * See package.json for author and maintainer info
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package model_manifests


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveModelManifestsGetHandlerFunc turns a function with the right signature into a weave model manifests get handler
type WeaveModelManifestsGetHandlerFunc func(WeaveModelManifestsGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveModelManifestsGetHandlerFunc) Handle(params WeaveModelManifestsGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveModelManifestsGetHandler interface for that can handle valid weave model manifests get params
type WeaveModelManifestsGetHandler interface {
	Handle(WeaveModelManifestsGetParams, interface{}) middleware.Responder
}

// NewWeaveModelManifestsGet creates a new http.Handler for the weave model manifests get operation
func NewWeaveModelManifestsGet(ctx *middleware.Context, handler WeaveModelManifestsGetHandler) *WeaveModelManifestsGet {
	return &WeaveModelManifestsGet{Context: ctx, Handler: handler}
}

/*WeaveModelManifestsGet swagger:route GET /modelManifests/{modelManifestId} modelManifests weaveModelManifestsGet

Returns a particular model manifest.

*/
type WeaveModelManifestsGet struct {
	Context *middleware.Context
	Handler WeaveModelManifestsGetHandler
}

func (o *WeaveModelManifestsGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveModelManifestsGetParams()

	uprinc, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	var principal interface{}
	if uprinc != nil {
		principal = uprinc
	}

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params, principal) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
