//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package backups

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/weaviate/weaviate/entities/models"
)

// BackupsRestoreStatusHandlerFunc turns a function with the right signature into a backups restore status handler
type BackupsRestoreStatusHandlerFunc func(BackupsRestoreStatusParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn BackupsRestoreStatusHandlerFunc) Handle(params BackupsRestoreStatusParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// BackupsRestoreStatusHandler interface for that can handle valid backups restore status params
type BackupsRestoreStatusHandler interface {
	Handle(BackupsRestoreStatusParams, *models.Principal) middleware.Responder
}

// NewBackupsRestoreStatus creates a new http.Handler for the backups restore status operation
func NewBackupsRestoreStatus(ctx *middleware.Context, handler BackupsRestoreStatusHandler) *BackupsRestoreStatus {
	return &BackupsRestoreStatus{Context: ctx, Handler: handler}
}

/*
BackupsRestoreStatus swagger:route GET /backups/{backend}/{id}/restore backups backupsRestoreStatus

Returns status of a backup restoration attempt for a set of classes
*/
type BackupsRestoreStatus struct {
	Context *middleware.Context
	Handler BackupsRestoreStatusHandler
}

func (o *BackupsRestoreStatus) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewBackupsRestoreStatusParams()

	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		r = aCtx
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
