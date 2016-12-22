package subscriptions


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveSubscriptionsUpdateHandlerFunc turns a function with the right signature into a weave subscriptions update handler
type WeaveSubscriptionsUpdateHandlerFunc func(WeaveSubscriptionsUpdateParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveSubscriptionsUpdateHandlerFunc) Handle(params WeaveSubscriptionsUpdateParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveSubscriptionsUpdateHandler interface for that can handle valid weave subscriptions update params
type WeaveSubscriptionsUpdateHandler interface {
	Handle(WeaveSubscriptionsUpdateParams, interface{}) middleware.Responder
}

// NewWeaveSubscriptionsUpdate creates a new http.Handler for the weave subscriptions update operation
func NewWeaveSubscriptionsUpdate(ctx *middleware.Context, handler WeaveSubscriptionsUpdateHandler) *WeaveSubscriptionsUpdate {
	return &WeaveSubscriptionsUpdate{Context: ctx, Handler: handler}
}

/*WeaveSubscriptionsUpdate swagger:route PUT /subscriptions/{subscriptionId} subscriptions weaveSubscriptionsUpdate

Update a subscription.

*/
type WeaveSubscriptionsUpdate struct {
	Context *middleware.Context
	Handler WeaveSubscriptionsUpdateHandler
}

func (o *WeaveSubscriptionsUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveSubscriptionsUpdateParams()

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
