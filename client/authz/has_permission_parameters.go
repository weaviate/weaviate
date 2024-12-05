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

	"github.com/weaviate/weaviate/entities/models"
)

// NewHasPermissionParams creates a new HasPermissionParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewHasPermissionParams() *HasPermissionParams {
	return &HasPermissionParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewHasPermissionParamsWithTimeout creates a new HasPermissionParams object
// with the ability to set a timeout on a request.
func NewHasPermissionParamsWithTimeout(timeout time.Duration) *HasPermissionParams {
	return &HasPermissionParams{
		timeout: timeout,
	}
}

// NewHasPermissionParamsWithContext creates a new HasPermissionParams object
// with the ability to set a context for a request.
func NewHasPermissionParamsWithContext(ctx context.Context) *HasPermissionParams {
	return &HasPermissionParams{
		Context: ctx,
	}
}

// NewHasPermissionParamsWithHTTPClient creates a new HasPermissionParams object
// with the ability to set a custom HTTPClient for a request.
func NewHasPermissionParamsWithHTTPClient(client *http.Client) *HasPermissionParams {
	return &HasPermissionParams{
		HTTPClient: client,
	}
}

/*
HasPermissionParams contains all the parameters to send to the API endpoint

	for the has permission operation.

	Typically these are written to a http.Request.
*/
type HasPermissionParams struct {

	// Body.
	Body *models.Permission

	/* ID.

	   role ID
	*/
	ID string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the has permission params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *HasPermissionParams) WithDefaults() *HasPermissionParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the has permission params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *HasPermissionParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the has permission params
func (o *HasPermissionParams) WithTimeout(timeout time.Duration) *HasPermissionParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the has permission params
func (o *HasPermissionParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the has permission params
func (o *HasPermissionParams) WithContext(ctx context.Context) *HasPermissionParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the has permission params
func (o *HasPermissionParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the has permission params
func (o *HasPermissionParams) WithHTTPClient(client *http.Client) *HasPermissionParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the has permission params
func (o *HasPermissionParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the has permission params
func (o *HasPermissionParams) WithBody(body *models.Permission) *HasPermissionParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the has permission params
func (o *HasPermissionParams) SetBody(body *models.Permission) {
	o.Body = body
}

// WithID adds the id to the has permission params
func (o *HasPermissionParams) WithID(id string) *HasPermissionParams {
	o.SetID(id)
	return o
}

// SetID adds the id to the has permission params
func (o *HasPermissionParams) SetID(id string) {
	o.ID = id
}

// WriteToRequest writes these params to a swagger request
func (o *HasPermissionParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error
	if o.Body != nil {
		if err := r.SetBodyParam(o.Body); err != nil {
			return err
		}
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