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

// NewTenantsDeleteParams creates a new TenantsDeleteParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewTenantsDeleteParams() *TenantsDeleteParams {
	return &TenantsDeleteParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewTenantsDeleteParamsWithTimeout creates a new TenantsDeleteParams object
// with the ability to set a timeout on a request.
func NewTenantsDeleteParamsWithTimeout(timeout time.Duration) *TenantsDeleteParams {
	return &TenantsDeleteParams{
		timeout: timeout,
	}
}

// NewTenantsDeleteParamsWithContext creates a new TenantsDeleteParams object
// with the ability to set a context for a request.
func NewTenantsDeleteParamsWithContext(ctx context.Context) *TenantsDeleteParams {
	return &TenantsDeleteParams{
		Context: ctx,
	}
}

// NewTenantsDeleteParamsWithHTTPClient creates a new TenantsDeleteParams object
// with the ability to set a custom HTTPClient for a request.
func NewTenantsDeleteParamsWithHTTPClient(client *http.Client) *TenantsDeleteParams {
	return &TenantsDeleteParams{
		HTTPClient: client,
	}
}

/*
TenantsDeleteParams contains all the parameters to send to the API endpoint

	for the tenants delete operation.

	Typically these are written to a http.Request.
*/
type TenantsDeleteParams struct {

	// ClassName.
	ClassName string

	// Tenants.
	Tenants []string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the tenants delete params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *TenantsDeleteParams) WithDefaults() *TenantsDeleteParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the tenants delete params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *TenantsDeleteParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the tenants delete params
func (o *TenantsDeleteParams) WithTimeout(timeout time.Duration) *TenantsDeleteParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the tenants delete params
func (o *TenantsDeleteParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the tenants delete params
func (o *TenantsDeleteParams) WithContext(ctx context.Context) *TenantsDeleteParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the tenants delete params
func (o *TenantsDeleteParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the tenants delete params
func (o *TenantsDeleteParams) WithHTTPClient(client *http.Client) *TenantsDeleteParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the tenants delete params
func (o *TenantsDeleteParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithClassName adds the className to the tenants delete params
func (o *TenantsDeleteParams) WithClassName(className string) *TenantsDeleteParams {
	o.SetClassName(className)
	return o
}

// SetClassName adds the className to the tenants delete params
func (o *TenantsDeleteParams) SetClassName(className string) {
	o.ClassName = className
}

// WithTenants adds the tenants to the tenants delete params
func (o *TenantsDeleteParams) WithTenants(tenants []string) *TenantsDeleteParams {
	o.SetTenants(tenants)
	return o
}

// SetTenants adds the tenants to the tenants delete params
func (o *TenantsDeleteParams) SetTenants(tenants []string) {
	o.Tenants = tenants
}

// WriteToRequest writes these params to a swagger request
func (o *TenantsDeleteParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param className
	if err := r.SetPathParam("className", o.ClassName); err != nil {
		return err
	}
	if o.Tenants != nil {
		if err := r.SetBodyParam(o.Tenants); err != nil {
			return err
		}
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
