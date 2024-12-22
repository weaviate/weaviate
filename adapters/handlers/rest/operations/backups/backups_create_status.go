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

package backups

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"

	"github.com/liutizhong/weaviate/entities/models"
)

// BackupsCreateStatusHandlerFunc turns a function with the right signature into a backups create status handler
type BackupsCreateStatusHandlerFunc func(BackupsCreateStatusParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn BackupsCreateStatusHandlerFunc) Handle(params BackupsCreateStatusParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// BackupsCreateStatusHandler interface for that can handle valid backups create status params
type BackupsCreateStatusHandler interface {
	Handle(BackupsCreateStatusParams, *models.Principal) middleware.Responder
}

// NewBackupsCreateStatus creates a new http.Handler for the backups create status operation
func NewBackupsCreateStatus(ctx *middleware.Context, handler BackupsCreateStatusHandler) *BackupsCreateStatus {
	return &BackupsCreateStatus{Context: ctx, Handler: handler}
}

/*
	BackupsCreateStatus swagger:route GET /backups/{backend}/{id} backups backupsCreateStatus

# Get backup process status

Returns status of backup creation attempt for a set of collections. <br/><br/>All client implementations have a `wait for completion` option which will poll the backup status in the background and only return once the backup has completed (successfully or unsuccessfully). If you set the `wait for completion` option to false, you can also check the status yourself using this endpoint.
*/
type BackupsCreateStatus struct {
	Context *middleware.Context
	Handler BackupsCreateStatusHandler
}

func (o *BackupsCreateStatus) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		*r = *rCtx
	}
	var Params = NewBackupsCreateStatusParams()
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
