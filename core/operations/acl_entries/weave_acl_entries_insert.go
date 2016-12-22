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

// WeaveACLEntriesInsertHandlerFunc turns a function with the right signature into a weave acl entries insert handler
type WeaveACLEntriesInsertHandlerFunc func(WeaveACLEntriesInsertParams, interface{}) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaveACLEntriesInsertHandlerFunc) Handle(params WeaveACLEntriesInsertParams, principal interface{}) middleware.Responder {
	return fn(params, principal)
}

// WeaveACLEntriesInsertHandler interface for that can handle valid weave acl entries insert params
type WeaveACLEntriesInsertHandler interface {
	Handle(WeaveACLEntriesInsertParams, interface{}) middleware.Responder
}

// NewWeaveACLEntriesInsert creates a new http.Handler for the weave acl entries insert operation
func NewWeaveACLEntriesInsert(ctx *middleware.Context, handler WeaveACLEntriesInsertHandler) *WeaveACLEntriesInsert {
	return &WeaveACLEntriesInsert{Context: ctx, Handler: handler}
}

/*WeaveACLEntriesInsert swagger:route POST /devices/{deviceId}/aclEntries aclEntries weaveAclEntriesInsert

Inserts a new ACL entry.

*/
type WeaveACLEntriesInsert struct {
	Context *middleware.Context
	Handler WeaveACLEntriesInsertHandler
}

func (o *WeaveACLEntriesInsert) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, _ := o.Context.RouteInfo(r)
	var Params = NewWeaveACLEntriesInsertParams()

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
