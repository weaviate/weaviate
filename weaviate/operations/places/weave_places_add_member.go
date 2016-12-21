package places


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeavePlacesAddMemberHandlerFunc turns a function with the right signature into a weave places add member handler
type WeavePlacesAddMemberHandlerFunc func(WeavePlacesAddMemberParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeavePlacesAddMemberHandlerFunc) Handle(params WeavePlacesAddMemberParams) middleware.Responder {
	return fn(params)
}

// WeavePlacesAddMemberHandler interface for that can handle valid weave places add member params
type WeavePlacesAddMemberHandler interface {
	Handle(WeavePlacesAddMemberParams) middleware.Responder
}

// NewWeavePlacesAddMember creates a new http.Handler for the weave places add member operation
func NewWeavePlacesAddMember(ctx *middleware.Context, handler WeavePlacesAddMemberHandler) *WeavePlacesAddMember {
	return &WeavePlacesAddMember{Context: ctx, Handler: handler}
}

/*WeavePlacesAddMember swagger:route POST /places/{placeId}/addMember places weavePlacesAddMember

Adds a member to a place and shares all devices associated with this place with the same member.

*/
type WeavePlacesAddMember struct {
	Context *middleware.Context
	Handler WeavePlacesAddMemberHandler
}

func (o *WeavePlacesAddMember) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeavePlacesAddMemberParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
