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
 package acl_entries


// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	middleware "github.com/go-openapi/runtime/middleware"
)

// WeaveACLEntriesDeleteHandlerFunc turns a function with the right signature into a weave acl entries delete handler
type WeaveACLEntriesDeleteHandlerFunc func(WeaveACLEntriesDeleteParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveACLEntriesDeleteHandlerFunc) Handle(params WeaveACLEntriesDeleteParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveACLEntriesDeleteHandler interface for that can handle valid weave acl entries delete params
type WeaveACLEntriesDeleteHandler interface {
	Handle(WeaveACLEntriesDeleteParams, interface{}) middleware.Responder
}

// NewWeaveACLEntriesDelete creates a new http.Handler for the weave acl entries delete operation
func NewWeaveACLEntriesDelete(ctx *middleware.Context, handler WeaveACLEntriesDeleteHandler) *WeaveACLEntriesDelete {
	return &WeaveACLEntriesDelete{Context: ctx, Handler: handler}
}

/*WeaveACLEntriesDelete swagger:route DELETE /devices/{deviceId}/aclEntries/{aclEntryId} aclEntries weaveAclEntriesDelete

Deletes an ACL entry.

*/
type WeaveACLEntriesDelete struct {
	Context *middleware.Context
	Handler WeaveACLEntriesDeleteHandler
}

func (o *WeaveACLEntriesDelete) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveACLEntriesDeleteParams()

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
