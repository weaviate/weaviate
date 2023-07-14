//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package schema

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

// NewTenantsGetParams creates a new TenantsGetParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewTenantsGetParams() *TenantsGetParams {
	return &TenantsGetParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewTenantsGetParamsWithTimeout creates a new TenantsGetParams object
// with the ability to set a timeout on a request.
func NewTenantsGetParamsWithTimeout(timeout time.Duration) *TenantsGetParams {
	return &TenantsGetParams{
		timeout: timeout,
	}
}

// NewTenantsGetParamsWithContext creates a new TenantsGetParams object
// with the ability to set a context for a request.
func NewTenantsGetParamsWithContext(ctx context.Context) *TenantsGetParams {
	return &TenantsGetParams{
		Context: ctx,
	}
}

// NewTenantsGetParamsWithHTTPClient creates a new TenantsGetParams object
// with the ability to set a custom HTTPClient for a request.
func NewTenantsGetParamsWithHTTPClient(client *http.Client) *TenantsGetParams {
	return &TenantsGetParams{
		HTTPClient: client,
	}
}

/*
TenantsGetParams contains all the parameters to send to the API endpoint

	for the tenants get operation.

	Typically these are written to a http.Request.
*/
type TenantsGetParams struct {

	// ClassName.
	ClassName string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the tenants get params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *TenantsGetParams) WithDefaults() *TenantsGetParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the tenants get params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *TenantsGetParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the tenants get params
func (o *TenantsGetParams) WithTimeout(timeout time.Duration) *TenantsGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the tenants get params
func (o *TenantsGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the tenants get params
func (o *TenantsGetParams) WithContext(ctx context.Context) *TenantsGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the tenants get params
func (o *TenantsGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the tenants get params
func (o *TenantsGetParams) WithHTTPClient(client *http.Client) *TenantsGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the tenants get params
func (o *TenantsGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithClassName adds the className to the tenants get params
func (o *TenantsGetParams) WithClassName(className string) *TenantsGetParams {
	o.SetClassName(className)
	return o
}

// SetClassName adds the className to the tenants get params
func (o *TenantsGetParams) SetClassName(className string) {
	o.ClassName = className
}

// WriteToRequest writes these params to a swagger request
func (o *TenantsGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param className
	if err := r.SetPathParam("className", o.ClassName); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
