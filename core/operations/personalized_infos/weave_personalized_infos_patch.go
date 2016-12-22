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
 package personalized_infos


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeavePersonalizedInfosPatchHandlerFunc turns a function with the right signature into a weave personalized infos patch handler
type WeavePersonalizedInfosPatchHandlerFunc func(WeavePersonalizedInfosPatchParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeavePersonalizedInfosPatchHandlerFunc) Handle(params WeavePersonalizedInfosPatchParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeavePersonalizedInfosPatchHandler interface for that can handle valid weave personalized infos patch params
type WeavePersonalizedInfosPatchHandler interface {
	Handle(WeavePersonalizedInfosPatchParams, interface{}) middleware.Responder
}

// NewWeavePersonalizedInfosPatch creates a new http.Handler for the weave personalized infos patch operation
func NewWeavePersonalizedInfosPatch(ctx *middleware.Context, handler WeavePersonalizedInfosPatchHandler) *WeavePersonalizedInfosPatch {
	return &WeavePersonalizedInfosPatch{Context: ctx, Handler: handler}
}

/*WeavePersonalizedInfosPatch swagger:route PATCH /devices/{deviceId}/personalizedInfos/{personalizedInfoId} personalizedInfos weavePersonalizedInfosPatch

Update the personalized info for device. This method supports patch semantics.

*/
type WeavePersonalizedInfosPatch struct {
	Context *middleware.Context
	Handler WeavePersonalizedInfosPatchHandler
}

func (o *WeavePersonalizedInfosPatch) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeavePersonalizedInfosPatchParams()

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
