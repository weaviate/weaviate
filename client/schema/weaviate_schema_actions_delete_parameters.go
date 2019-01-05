/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"
	"time"

	"golang.org/x/net/context"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	cr "github.com/go-openapi/runtime/client"

	strfmt "github.com/go-openapi/strfmt"
)

// NewWeaviateSchemaActionsDeleteParams creates a new WeaviateSchemaActionsDeleteParams object
// with the default values initialized.
func NewWeaviateSchemaActionsDeleteParams() *WeaviateSchemaActionsDeleteParams {
	var ()
	return &WeaviateSchemaActionsDeleteParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewWeaviateSchemaActionsDeleteParamsWithTimeout creates a new WeaviateSchemaActionsDeleteParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewWeaviateSchemaActionsDeleteParamsWithTimeout(timeout time.Duration) *WeaviateSchemaActionsDeleteParams {
	var ()
	return &WeaviateSchemaActionsDeleteParams{

		timeout: timeout,
	}
}

// NewWeaviateSchemaActionsDeleteParamsWithContext creates a new WeaviateSchemaActionsDeleteParams object
// with the default values initialized, and the ability to set a context for a request
func NewWeaviateSchemaActionsDeleteParamsWithContext(ctx context.Context) *WeaviateSchemaActionsDeleteParams {
	var ()
	return &WeaviateSchemaActionsDeleteParams{

		Context: ctx,
	}
}

// NewWeaviateSchemaActionsDeleteParamsWithHTTPClient creates a new WeaviateSchemaActionsDeleteParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewWeaviateSchemaActionsDeleteParamsWithHTTPClient(client *http.Client) *WeaviateSchemaActionsDeleteParams {
	var ()
	return &WeaviateSchemaActionsDeleteParams{
		HTTPClient: client,
	}
}

/*WeaviateSchemaActionsDeleteParams contains all the parameters to send to the API endpoint
for the weaviate schema actions delete operation typically these are written to a http.Request
*/
type WeaviateSchemaActionsDeleteParams struct {

	/*ClassName*/
	ClassName string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the weaviate schema actions delete params
func (o *WeaviateSchemaActionsDeleteParams) WithTimeout(timeout time.Duration) *WeaviateSchemaActionsDeleteParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the weaviate schema actions delete params
func (o *WeaviateSchemaActionsDeleteParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the weaviate schema actions delete params
func (o *WeaviateSchemaActionsDeleteParams) WithContext(ctx context.Context) *WeaviateSchemaActionsDeleteParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the weaviate schema actions delete params
func (o *WeaviateSchemaActionsDeleteParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the weaviate schema actions delete params
func (o *WeaviateSchemaActionsDeleteParams) WithHTTPClient(client *http.Client) *WeaviateSchemaActionsDeleteParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the weaviate schema actions delete params
func (o *WeaviateSchemaActionsDeleteParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithClassName adds the className to the weaviate schema actions delete params
func (o *WeaviateSchemaActionsDeleteParams) WithClassName(className string) *WeaviateSchemaActionsDeleteParams {
	o.SetClassName(className)
	return o
}

// SetClassName adds the className to the weaviate schema actions delete params
func (o *WeaviateSchemaActionsDeleteParams) SetClassName(className string) {
	o.ClassName = className
}

// WriteToRequest writes these params to a swagger request
func (o *WeaviateSchemaActionsDeleteParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

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
