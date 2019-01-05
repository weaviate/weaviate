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
	"context"
	"net/http"
	"time"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	cr "github.com/go-openapi/runtime/client"

	strfmt "github.com/go-openapi/strfmt"
)

// NewWeaviateSchemaDumpParams creates a new WeaviateSchemaDumpParams object
// with the default values initialized.
func NewWeaviateSchemaDumpParams() *WeaviateSchemaDumpParams {

	return &WeaviateSchemaDumpParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewWeaviateSchemaDumpParamsWithTimeout creates a new WeaviateSchemaDumpParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewWeaviateSchemaDumpParamsWithTimeout(timeout time.Duration) *WeaviateSchemaDumpParams {

	return &WeaviateSchemaDumpParams{

		timeout: timeout,
	}
}

// NewWeaviateSchemaDumpParamsWithContext creates a new WeaviateSchemaDumpParams object
// with the default values initialized, and the ability to set a context for a request
func NewWeaviateSchemaDumpParamsWithContext(ctx context.Context) *WeaviateSchemaDumpParams {

	return &WeaviateSchemaDumpParams{

		Context: ctx,
	}
}

// NewWeaviateSchemaDumpParamsWithHTTPClient creates a new WeaviateSchemaDumpParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewWeaviateSchemaDumpParamsWithHTTPClient(client *http.Client) *WeaviateSchemaDumpParams {

	return &WeaviateSchemaDumpParams{
		HTTPClient: client,
	}
}

/*WeaviateSchemaDumpParams contains all the parameters to send to the API endpoint
for the weaviate schema dump operation typically these are written to a http.Request
*/
type WeaviateSchemaDumpParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the weaviate schema dump params
func (o *WeaviateSchemaDumpParams) WithTimeout(timeout time.Duration) *WeaviateSchemaDumpParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the weaviate schema dump params
func (o *WeaviateSchemaDumpParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the weaviate schema dump params
func (o *WeaviateSchemaDumpParams) WithContext(ctx context.Context) *WeaviateSchemaDumpParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the weaviate schema dump params
func (o *WeaviateSchemaDumpParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the weaviate schema dump params
func (o *WeaviateSchemaDumpParams) WithHTTPClient(client *http.Client) *WeaviateSchemaDumpParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the weaviate schema dump params
func (o *WeaviateSchemaDumpParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *WeaviateSchemaDumpParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
