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

// WeaviateACLEntriesGetHandlerFunc turns a function with the right signature into a weaviate acl entries get handler
type WeaviateACLEntriesGetHandlerFunc func(WeaviateACLEntriesGetParams) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateACLEntriesGetHandlerFunc) Handle(params WeaviateACLEntriesGetParams) middleware.Responder {
	return fn(params)
}

// WeaviateACLEntriesGetHandler interface for that can handle valid weaviate acl entries get params
type WeaviateACLEntriesGetHandler interface {
	Handle(WeaviateACLEntriesGetParams) middleware.Responder
}

// NewWeaviateACLEntriesGet creates a new http.Handler for the weaviate acl entries get operation
func NewWeaviateACLEntriesGet(ctx *middleware.Context, handler WeaviateACLEntriesGetHandler) *WeaviateACLEntriesGet {
	return &WeaviateACLEntriesGet{Context: ctx, Handler: handler}
}

/*WeaviateACLEntriesGet swagger:route GET /devices/{deviceId}/aclEntries/{aclEntryId} aclEntries weaviateAclEntriesGet

Returns the requested ACL entry.

*/
type WeaviateACLEntriesGet struct {
	Context *middleware.Context
	Handler WeaviateACLEntriesGetHandler
}

func (o *WeaviateACLEntriesGet) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaviateACLEntriesGetParams()

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}
