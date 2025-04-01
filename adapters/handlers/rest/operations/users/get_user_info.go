//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package users

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// GetUserInfoHandlerFunc turns a function with the right signature into a get user info handler
type GetUserInfoHandlerFunc func(GetUserInfoParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn GetUserInfoHandlerFunc) Handle(params GetUserInfoParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// GetUserInfoHandler interface for that can handle valid get user info params
type GetUserInfoHandler interface {
	Handle(GetUserInfoParams, *models.Principal) middleware.Responder
}

// NewGetUserInfo creates a new http.Handler for the get user info operation
func NewGetUserInfo(ctx *middleware.Context, handler GetUserInfoHandler) *GetUserInfo {
	return &GetUserInfo{Context: ctx, Handler: handler}
}

/*
	GetUserInfo swagger:route GET /users/db/{user_id} users getUserInfo

get info relevant to user, e.g. username, roles
*/
type GetUserInfo struct {
	Context *middleware.Context
	Handler GetUserInfoHandler
}

func (o *GetUserInfo) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewGetUserInfoParams()
	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		*r = *aCtx
	}
	var principal *models.Principal
	if uprinc != nil {
		principal = uprinc.(*models.Principal) // this is really a models.Principal, I promise
	}

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params, principal) // actually handle the request
	o.Context.Respond(rw, r, route.Produces, route, res)

}
