//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package batch

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

// NewBatchObjectsCreateParams creates a new BatchObjectsCreateParams object
// with the default values initialized.
func NewBatchObjectsCreateParams() *BatchObjectsCreateParams {
	var ()
	return &BatchObjectsCreateParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewBatchObjectsCreateParamsWithTimeout creates a new BatchObjectsCreateParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewBatchObjectsCreateParamsWithTimeout(timeout time.Duration) *BatchObjectsCreateParams {
	var ()
	return &BatchObjectsCreateParams{
		timeout: timeout,
	}
}

// NewBatchObjectsCreateParamsWithContext creates a new BatchObjectsCreateParams object
// with the default values initialized, and the ability to set a context for a request
func NewBatchObjectsCreateParamsWithContext(ctx context.Context) *BatchObjectsCreateParams {
	var ()
	return &BatchObjectsCreateParams{
		Context: ctx,
	}
}

// NewBatchObjectsCreateParamsWithHTTPClient creates a new BatchObjectsCreateParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewBatchObjectsCreateParamsWithHTTPClient(client *http.Client) *BatchObjectsCreateParams {
	var ()
	return &BatchObjectsCreateParams{
		HTTPClient: client,
	}
}

/*
BatchObjectsCreateParams contains all the parameters to send to the API endpoint
for the batch objects create operation typically these are written to a http.Request
*/
type BatchObjectsCreateParams struct {
	/*Body*/
	Body BatchObjectsCreateBody

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the batch objects create params
func (o *BatchObjectsCreateParams) WithTimeout(timeout time.Duration) *BatchObjectsCreateParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the batch objects create params
func (o *BatchObjectsCreateParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the batch objects create params
func (o *BatchObjectsCreateParams) WithContext(ctx context.Context) *BatchObjectsCreateParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the batch objects create params
func (o *BatchObjectsCreateParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the batch objects create params
func (o *BatchObjectsCreateParams) WithHTTPClient(client *http.Client) *BatchObjectsCreateParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the batch objects create params
func (o *BatchObjectsCreateParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the batch objects create params
func (o *BatchObjectsCreateParams) WithBody(body BatchObjectsCreateBody) *BatchObjectsCreateParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the batch objects create params
func (o *BatchObjectsCreateParams) SetBody(body BatchObjectsCreateBody) {
	o.Body = body
}

// WriteToRequest writes these params to a swagger request
func (o *BatchObjectsCreateParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {
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
