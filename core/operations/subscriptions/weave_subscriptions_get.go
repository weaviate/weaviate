package subscriptions


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveSubscriptionsGetHandlerFunc turns a function with the right signature into a weave subscriptions get handler
type WeaveSubscriptionsGetHandlerFunc func(WeaveSubscriptionsGetParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveSubscriptionsGetHandlerFunc) Handle(params WeaveSubscriptionsGetParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveSubscriptionsGetHandler interface for that can handle valid weave subscriptions get params
type WeaveSubscriptionsGetHandler interface {
	Handle(WeaveSubscriptionsGetParams, interface{}) middleware.Responder
}

// NewWeaveSubscriptionsGet creates a new http.Handler for the weave subscriptions get operation
func NewWeaveSubscriptionsGet(ctx *middleware.Context, handler WeaveSubscriptionsGetHandler) *WeaveSubscriptionsGet {
	return &WeaveSubscriptionsGet{Context: ctx, Handler: handler}
}

/*WeaveSubscriptionsGet swagger:route GET /subscriptions/{subscriptionId} subscriptions weaveSubscriptionsGet

Returns the requested subscription.

*/
type WeaveSubscriptionsGet struct {
	Context *middleware.Context
	Handler WeaveSubscriptionsGetHandler
}

func (o *WeaveSubscriptionsGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveSubscriptionsGetParams()

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
