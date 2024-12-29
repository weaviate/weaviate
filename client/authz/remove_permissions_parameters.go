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

package authz

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"
	"net/http"
	"time"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	cr "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
)

// NewRemovePermissionsParams creates a new RemovePermissionsParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewRemovePermissionsParams() *RemovePermissionsParams {
	return &RemovePermissionsParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewRemovePermissionsParamsWithTimeout creates a new RemovePermissionsParams object
// with the ability to set a timeout on a request.
func NewRemovePermissionsParamsWithTimeout(timeout time.Duration) *RemovePermissionsParams {
	return &RemovePermissionsParams{
		timeout: timeout,
	}
}

// NewRemovePermissionsParamsWithContext creates a new RemovePermissionsParams object
// with the ability to set a context for a request.
func NewRemovePermissionsParamsWithContext(ctx context.Context) *RemovePermissionsParams {
	return &RemovePermissionsParams{
		Context: ctx,
	}
}

// NewRemovePermissionsParamsWithHTTPClient creates a new RemovePermissionsParams object
// with the ability to set a custom HTTPClient for a request.
func NewRemovePermissionsParamsWithHTTPClient(client *http.Client) *RemovePermissionsParams {
	return &RemovePermissionsParams{
		HTTPClient: client,
	}
}

/*
RemovePermissionsParams contains all the parameters to send to the API endpoint

	for the remove permissions operation.

	Typically these are written to a http.Request.
*/
type RemovePermissionsParams struct {

	// Body.
	Body RemovePermissionsBody

	/* ID.

	   role name
	*/
	ID string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the remove permissions params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *RemovePermissionsParams) WithDefaults() *RemovePermissionsParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the remove permissions params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *RemovePermissionsParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the remove permissions params
func (o *RemovePermissionsParams) WithTimeout(timeout time.Duration) *RemovePermissionsParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the remove permissions params
func (o *RemovePermissionsParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the remove permissions params
func (o *RemovePermissionsParams) WithContext(ctx context.Context) *RemovePermissionsParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the remove permissions params
func (o *RemovePermissionsParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the remove permissions params
func (o *RemovePermissionsParams) WithHTTPClient(client *http.Client) *RemovePermissionsParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the remove permissions params
func (o *RemovePermissionsParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the remove permissions params
func (o *RemovePermissionsParams) WithBody(body RemovePermissionsBody) *RemovePermissionsParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the remove permissions params
func (o *RemovePermissionsParams) SetBody(body RemovePermissionsBody) {
	o.Body = body
}

// WithID adds the id to the remove permissions params
func (o *RemovePermissionsParams) WithID(id string) *RemovePermissionsParams {
	o.SetID(id)
	return o
}

// SetID adds the id to the remove permissions params
func (o *RemovePermissionsParams) SetID(id string) {
	o.ID = id
}

// WriteToRequest writes these params to a swagger request
func (o *RemovePermissionsParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error
	if err := r.SetBodyParam(o.Body); err != nil {
		return err
	}

	// path param id
	if err := r.SetPathParam("id", o.ID); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
