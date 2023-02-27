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

package classifications

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

// NewClassificationsGetParams creates a new ClassificationsGetParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewClassificationsGetParams() *ClassificationsGetParams {
	return &ClassificationsGetParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewClassificationsGetParamsWithTimeout creates a new ClassificationsGetParams object
// with the ability to set a timeout on a request.
func NewClassificationsGetParamsWithTimeout(timeout time.Duration) *ClassificationsGetParams {
	return &ClassificationsGetParams{
		timeout: timeout,
	}
}

// NewClassificationsGetParamsWithContext creates a new ClassificationsGetParams object
// with the ability to set a context for a request.
func NewClassificationsGetParamsWithContext(ctx context.Context) *ClassificationsGetParams {
	return &ClassificationsGetParams{
		Context: ctx,
	}
}

// NewClassificationsGetParamsWithHTTPClient creates a new ClassificationsGetParams object
// with the ability to set a custom HTTPClient for a request.
func NewClassificationsGetParamsWithHTTPClient(client *http.Client) *ClassificationsGetParams {
	return &ClassificationsGetParams{
		HTTPClient: client,
	}
}

/*
ClassificationsGetParams contains all the parameters to send to the API endpoint

	for the classifications get operation.

	Typically these are written to a http.Request.
*/
type ClassificationsGetParams struct {

	/* ID.

	   classification id
	*/
	ID string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the classifications get params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *ClassificationsGetParams) WithDefaults() *ClassificationsGetParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the classifications get params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *ClassificationsGetParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the classifications get params
func (o *ClassificationsGetParams) WithTimeout(timeout time.Duration) *ClassificationsGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the classifications get params
func (o *ClassificationsGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the classifications get params
func (o *ClassificationsGetParams) WithContext(ctx context.Context) *ClassificationsGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the classifications get params
func (o *ClassificationsGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the classifications get params
func (o *ClassificationsGetParams) WithHTTPClient(client *http.Client) *ClassificationsGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the classifications get params
func (o *ClassificationsGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithID adds the id to the classifications get params
func (o *ClassificationsGetParams) WithID(id string) *ClassificationsGetParams {
	o.SetID(id)
	return o
}

// SetID adds the id to the classifications get params
func (o *ClassificationsGetParams) SetID(id string) {
	o.ID = id
}

// WriteToRequest writes these params to a swagger request
func (o *ClassificationsGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param id
	if err := r.SetPathParam("id", o.ID); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
