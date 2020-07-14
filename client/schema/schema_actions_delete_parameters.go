//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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

// NewSchemaActionsDeleteParams creates a new SchemaActionsDeleteParams object
// with the default values initialized.
func NewSchemaActionsDeleteParams() *SchemaActionsDeleteParams {
	var ()
	return &SchemaActionsDeleteParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewSchemaActionsDeleteParamsWithTimeout creates a new SchemaActionsDeleteParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewSchemaActionsDeleteParamsWithTimeout(timeout time.Duration) *SchemaActionsDeleteParams {
	var ()
	return &SchemaActionsDeleteParams{

		timeout: timeout,
	}
}

// NewSchemaActionsDeleteParamsWithContext creates a new SchemaActionsDeleteParams object
// with the default values initialized, and the ability to set a context for a request
func NewSchemaActionsDeleteParamsWithContext(ctx context.Context) *SchemaActionsDeleteParams {
	var ()
	return &SchemaActionsDeleteParams{

		Context: ctx,
	}
}

// NewSchemaActionsDeleteParamsWithHTTPClient creates a new SchemaActionsDeleteParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewSchemaActionsDeleteParamsWithHTTPClient(client *http.Client) *SchemaActionsDeleteParams {
	var ()
	return &SchemaActionsDeleteParams{
		HTTPClient: client,
	}
}

/*SchemaActionsDeleteParams contains all the parameters to send to the API endpoint
for the schema actions delete operation typically these are written to a http.Request
*/
type SchemaActionsDeleteParams struct {

	/*ClassName*/
	ClassName string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the schema actions delete params
func (o *SchemaActionsDeleteParams) WithTimeout(timeout time.Duration) *SchemaActionsDeleteParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the schema actions delete params
func (o *SchemaActionsDeleteParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the schema actions delete params
func (o *SchemaActionsDeleteParams) WithContext(ctx context.Context) *SchemaActionsDeleteParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the schema actions delete params
func (o *SchemaActionsDeleteParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the schema actions delete params
func (o *SchemaActionsDeleteParams) WithHTTPClient(client *http.Client) *SchemaActionsDeleteParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the schema actions delete params
func (o *SchemaActionsDeleteParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithClassName adds the className to the schema actions delete params
func (o *SchemaActionsDeleteParams) WithClassName(className string) *SchemaActionsDeleteParams {
	o.SetClassName(className)
	return o
}

// SetClassName adds the className to the schema actions delete params
func (o *SchemaActionsDeleteParams) SetClassName(className string) {
	o.ClassName = className
}

// WriteToRequest writes these params to a swagger request
func (o *SchemaActionsDeleteParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

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
