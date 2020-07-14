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

package operations

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

// NewGenesisPeersListParams creates a new GenesisPeersListParams object
// with the default values initialized.
func NewGenesisPeersListParams() *GenesisPeersListParams {

	return &GenesisPeersListParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewGenesisPeersListParamsWithTimeout creates a new GenesisPeersListParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewGenesisPeersListParamsWithTimeout(timeout time.Duration) *GenesisPeersListParams {

	return &GenesisPeersListParams{

		timeout: timeout,
	}
}

// NewGenesisPeersListParamsWithContext creates a new GenesisPeersListParams object
// with the default values initialized, and the ability to set a context for a request
func NewGenesisPeersListParamsWithContext(ctx context.Context) *GenesisPeersListParams {

	return &GenesisPeersListParams{

		Context: ctx,
	}
}

// NewGenesisPeersListParamsWithHTTPClient creates a new GenesisPeersListParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewGenesisPeersListParamsWithHTTPClient(client *http.Client) *GenesisPeersListParams {

	return &GenesisPeersListParams{
		HTTPClient: client,
	}
}

/*GenesisPeersListParams contains all the parameters to send to the API endpoint
for the genesis peers list operation typically these are written to a http.Request
*/
type GenesisPeersListParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the genesis peers list params
func (o *GenesisPeersListParams) WithTimeout(timeout time.Duration) *GenesisPeersListParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the genesis peers list params
func (o *GenesisPeersListParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the genesis peers list params
func (o *GenesisPeersListParams) WithContext(ctx context.Context) *GenesisPeersListParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the genesis peers list params
func (o *GenesisPeersListParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the genesis peers list params
func (o *GenesisPeersListParams) WithHTTPClient(client *http.Client) *GenesisPeersListParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the genesis peers list params
func (o *GenesisPeersListParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *GenesisPeersListParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
