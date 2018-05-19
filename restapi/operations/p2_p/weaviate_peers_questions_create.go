/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package p2_p

// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviatePeersQuestionsCreateHandlerFunc turns a function with the right signature into a weaviate peers questions create handler
type WeaviatePeersQuestionsCreateHandlerFunc func(WeaviatePeersQuestionsCreateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviatePeersQuestionsCreateHandlerFunc) Handle(params WeaviatePeersQuestionsCreateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviatePeersQuestionsCreateHandler interface for that can handle valid weaviate peers questions create params
type WeaviatePeersQuestionsCreateHandler interface {
	Handle(WeaviatePeersQuestionsCreateParams, interface{}) middleware.Responder
}

// NewWeaviatePeersQuestionsCreate creates a new http.Handler for the weaviate peers questions create operation
func NewWeaviatePeersQuestionsCreate(ctx *middleware.Context, handler WeaviatePeersQuestionsCreateHandler) *WeaviatePeersQuestionsCreate {
	return &WeaviatePeersQuestionsCreate{Context: ctx, Handler: handler}
}

/*WeaviatePeersQuestionsCreate swagger:route POST /peers/questions P2P weaviatePeersQuestionsCreate

Receive a question from a peer in the network.

Receive a question from a peer in the network.

*/
type WeaviatePeersQuestionsCreate struct {
	Context *middleware.Context
	Handler WeaviatePeersQuestionsCreateHandler
}

func (o *WeaviatePeersQuestionsCreate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviatePeersQuestionsCreateParams()

	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		r = aCtx
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
