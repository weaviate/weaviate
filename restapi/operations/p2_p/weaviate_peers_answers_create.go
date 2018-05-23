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

// WeaviatePeersAnswersCreateHandlerFunc turns a function with the right signature into a weaviate peers answers create handler
type WeaviatePeersAnswersCreateHandlerFunc func(WeaviatePeersAnswersCreateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviatePeersAnswersCreateHandlerFunc) Handle(params WeaviatePeersAnswersCreateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaviatePeersAnswersCreateHandler interface for that can handle valid weaviate peers answers create params
type WeaviatePeersAnswersCreateHandler interface {
	Handle(WeaviatePeersAnswersCreateParams, interface{}) middleware.Responder
}

// NewWeaviatePeersAnswersCreate creates a new http.Handler for the weaviate peers answers create operation
func NewWeaviatePeersAnswersCreate(ctx *middleware.Context, handler WeaviatePeersAnswersCreateHandler) *WeaviatePeersAnswersCreate {
	return &WeaviatePeersAnswersCreate{Context: ctx, Handler: handler}
}

/*WeaviatePeersAnswersCreate swagger:route POST /peers/answers/{answerId} P2P weaviatePeersAnswersCreate

Receiving a new answer from a peer.

Receive an answer based on a question from a peer in the network.

*/
type WeaviatePeersAnswersCreate struct {
	Context *middleware.Context
	Handler WeaviatePeersAnswersCreateHandler
}

func (o *WeaviatePeersAnswersCreate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviatePeersAnswersCreateParams()

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
