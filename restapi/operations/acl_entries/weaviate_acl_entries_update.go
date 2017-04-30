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
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
 package acl_entries


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaviateACLEntriesUpdateHandlerFunc turns a function with the right signature into a weaviate acl entries update handler
type WeaviateACLEntriesUpdateHandlerFunc func(WeaviateACLEntriesUpdateParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateACLEntriesUpdateHandlerFunc) Handle(params WeaviateACLEntriesUpdateParams) middleware.Responder {
	return fn(params)
}

// WeaviateACLEntriesUpdateHandler interface for that can handle valid weaviate acl entries update params
type WeaviateACLEntriesUpdateHandler interface {
	Handle(WeaviateACLEntriesUpdateParams) middleware.Responder
}

// NewWeaviateACLEntriesUpdate creates a new http.Handler for the weaviate acl entries update operation
func NewWeaviateACLEntriesUpdate(ctx *middleware.Context, handler WeaviateACLEntriesUpdateHandler) *WeaviateACLEntriesUpdate {
	return &WeaviateACLEntriesUpdate{Context: ctx, Handler: handler}
}

/*WeaviateACLEntriesUpdate swagger:route PUT /devices/{deviceId}/aclEntries/{aclEntryId} aclEntries weaviateAclEntriesUpdate

Updates an ACL entry.

*/
type WeaviateACLEntriesUpdate struct {
	Context *middleware.Context
	Handler WeaviateACLEntriesUpdateHandler
}

func (o *WeaviateACLEntriesUpdate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateACLEntriesUpdateParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
