/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
// Code generated by go-swagger; DO NOT EDIT.

package things

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

// NewWeaviateThingsCreateParams creates a new WeaviateThingsCreateParams object
// with the default values initialized.
func NewWeaviateThingsCreateParams() *WeaviateThingsCreateParams {
	var ()
	return &WeaviateThingsCreateParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewWeaviateThingsCreateParamsWithTimeout creates a new WeaviateThingsCreateParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewWeaviateThingsCreateParamsWithTimeout(timeout time.Duration) *WeaviateThingsCreateParams {
	var ()
	return &WeaviateThingsCreateParams{

		timeout: timeout,
	}
}

// NewWeaviateThingsCreateParamsWithContext creates a new WeaviateThingsCreateParams object
// with the default values initialized, and the ability to set a context for a request
func NewWeaviateThingsCreateParamsWithContext(ctx context.Context) *WeaviateThingsCreateParams {
	var ()
	return &WeaviateThingsCreateParams{

		Context: ctx,
	}
}

// NewWeaviateThingsCreateParamsWithHTTPClient creates a new WeaviateThingsCreateParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewWeaviateThingsCreateParamsWithHTTPClient(client *http.Client) *WeaviateThingsCreateParams {
	var ()
	return &WeaviateThingsCreateParams{
		HTTPClient: client,
	}
}

/*WeaviateThingsCreateParams contains all the parameters to send to the API endpoint
for the weaviate things create operation typically these are written to a http.Request
*/
type WeaviateThingsCreateParams struct {

	/*Body*/
	Body WeaviateThingsCreateBody

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the weaviate things create params
func (o *WeaviateThingsCreateParams) WithTimeout(timeout time.Duration) *WeaviateThingsCreateParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the weaviate things create params
func (o *WeaviateThingsCreateParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the weaviate things create params
func (o *WeaviateThingsCreateParams) WithContext(ctx context.Context) *WeaviateThingsCreateParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the weaviate things create params
func (o *WeaviateThingsCreateParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the weaviate things create params
func (o *WeaviateThingsCreateParams) WithHTTPClient(client *http.Client) *WeaviateThingsCreateParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the weaviate things create params
func (o *WeaviateThingsCreateParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the weaviate things create params
func (o *WeaviateThingsCreateParams) WithBody(body WeaviateThingsCreateBody) *WeaviateThingsCreateParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the weaviate things create params
func (o *WeaviateThingsCreateParams) SetBody(body WeaviateThingsCreateBody) {
	o.Body = body
}

// WriteToRequest writes these params to a swagger request
func (o *WeaviateThingsCreateParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if err := r.SetBodyParam(o.Body); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
