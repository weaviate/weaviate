package places


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeavePlacesListSuggestionsHandlerFunc turns a function with the right signature into a weave places list suggestions handler
type WeavePlacesListSuggestionsHandlerFunc func(WeavePlacesListSuggestionsParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeavePlacesListSuggestionsHandlerFunc) Handle(params WeavePlacesListSuggestionsParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeavePlacesListSuggestionsHandler interface for that can handle valid weave places list suggestions params
type WeavePlacesListSuggestionsHandler interface {
	Handle(WeavePlacesListSuggestionsParams, interface{}) middleware.Responder
}

// NewWeavePlacesListSuggestions creates a new http.Handler for the weave places list suggestions operation
func NewWeavePlacesListSuggestions(ctx *middleware.Context, handler WeavePlacesListSuggestionsHandler) *WeavePlacesListSuggestions {
	return &WeavePlacesListSuggestions{Context: ctx, Handler: handler}
}

/*WeavePlacesListSuggestions swagger:route POST /places/listSuggestions places weavePlacesListSuggestions

Lists places and room suggestions that a user could use for a particular devices.

*/
type WeavePlacesListSuggestions struct {
	Context *middleware.Context
	Handler WeavePlacesListSuggestionsHandler
}

func (o *WeavePlacesListSuggestions) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeavePlacesListSuggestionsParams()

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
