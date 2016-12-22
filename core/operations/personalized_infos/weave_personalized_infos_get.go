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

// WeavePersonalizedInfosGetHandlerFunc turns a function with the right signature into a weave personalized infos get handler
type WeavePersonalizedInfosGetHandlerFunc func(WeavePersonalizedInfosGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeavePersonalizedInfosGetHandlerFunc) Handle(params WeavePersonalizedInfosGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeavePersonalizedInfosGetHandler interface for that can handle valid weave personalized infos get params
type WeavePersonalizedInfosGetHandler interface {
	Handle(WeavePersonalizedInfosGetParams, interface{}) middleware.Responder
}

// NewWeavePersonalizedInfosGet creates a new http.Handler for the weave personalized infos get operation
func NewWeavePersonalizedInfosGet(ctx *middleware.Context, handler WeavePersonalizedInfosGetHandler) *WeavePersonalizedInfosGet {
	return &WeavePersonalizedInfosGet{Context: ctx, Handler: handler}
}

/*WeavePersonalizedInfosGet swagger:route GET /devices/{deviceId}/personalizedInfos/{personalizedInfoId} personalizedInfos weavePersonalizedInfosGet

Returns the personalized info for device.

*/
type WeavePersonalizedInfosGet struct {
	Context *middleware.Context
	Handler WeavePersonalizedInfosGetHandler
}

func (o *WeavePersonalizedInfosGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeavePersonalizedInfosGetParams()

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
