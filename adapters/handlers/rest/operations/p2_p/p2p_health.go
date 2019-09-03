//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package p2_p

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// P2pHealthHandlerFunc turns a function with the right signature into a p2p health handler
type P2pHealthHandlerFunc func(P2pHealthParams) middleware.Responder

// Handle executing the request and returning a response
func (fn P2pHealthHandlerFunc) Handle(params P2pHealthParams) middleware.Responder {
	return fn(params)
}

// P2pHealthHandler interface for that can handle valid p2p health params
type P2pHealthHandler interface {
	Handle(P2pHealthParams) middleware.Responder
}

// NewP2pHealth creates a new http.Handler for the p2p health operation
func NewP2pHealth(ctx *middleware.Context, handler P2pHealthHandler) *P2pHealth {
	return &P2pHealth{Context: ctx, Handler: handler}
}

/*P2pHealth swagger:route GET /p2p/health P2P p2pHealth

Check if a peer is alive.

Check if a peer is alive and healthy.

*/
type P2pHealth struct {
	Context *middleware.Context
	Handler P2pHealthHandler
}

func (o *P2pHealth) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewP2pHealthParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
