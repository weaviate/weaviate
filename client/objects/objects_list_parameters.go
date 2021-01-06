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

package objects

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
	"github.com/go-openapi/swag"
)

// NewObjectsListParams creates a new ObjectsListParams object
// with the default values initialized.
func NewObjectsListParams() *ObjectsListParams {
	var ()
	return &ObjectsListParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewObjectsListParamsWithTimeout creates a new ObjectsListParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewObjectsListParamsWithTimeout(timeout time.Duration) *ObjectsListParams {
	var ()
	return &ObjectsListParams{

		timeout: timeout,
	}
}

// NewObjectsListParamsWithContext creates a new ObjectsListParams object
// with the default values initialized, and the ability to set a context for a request
func NewObjectsListParamsWithContext(ctx context.Context) *ObjectsListParams {
	var ()
	return &ObjectsListParams{

		Context: ctx,
	}
}

// NewObjectsListParamsWithHTTPClient creates a new ObjectsListParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewObjectsListParamsWithHTTPClient(client *http.Client) *ObjectsListParams {
	var ()
	return &ObjectsListParams{
		HTTPClient: client,
	}
}

/*ObjectsListParams contains all the parameters to send to the API endpoint
for the objects list operation typically these are written to a http.Request
*/
type ObjectsListParams struct {

	/*Include
	  Include additional information, such as classification infos. Allowed values include: classification, vector, interpretation

	*/
	Include *string
	/*Limit
	  The maximum number of items to be returned per page. Default value is set in Weaviate config.

	*/
	Limit *int64

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the objects list params
func (o *ObjectsListParams) WithTimeout(timeout time.Duration) *ObjectsListParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the objects list params
func (o *ObjectsListParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the objects list params
func (o *ObjectsListParams) WithContext(ctx context.Context) *ObjectsListParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the objects list params
func (o *ObjectsListParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the objects list params
func (o *ObjectsListParams) WithHTTPClient(client *http.Client) *ObjectsListParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the objects list params
func (o *ObjectsListParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithInclude adds the include to the objects list params
func (o *ObjectsListParams) WithInclude(include *string) *ObjectsListParams {
	o.SetInclude(include)
	return o
}

// SetInclude adds the include to the objects list params
func (o *ObjectsListParams) SetInclude(include *string) {
	o.Include = include
}

// WithLimit adds the limit to the objects list params
func (o *ObjectsListParams) WithLimit(limit *int64) *ObjectsListParams {
	o.SetLimit(limit)
	return o
}

// SetLimit adds the limit to the objects list params
func (o *ObjectsListParams) SetLimit(limit *int64) {
	o.Limit = limit
}

// WriteToRequest writes these params to a swagger request
func (o *ObjectsListParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if o.Include != nil {

		// query param include
		var qrInclude string
		if o.Include != nil {
			qrInclude = *o.Include
		}
		qInclude := qrInclude
		if qInclude != "" {
			if err := r.SetQueryParam("include", qInclude); err != nil {
				return err
			}
		}

	}

	if o.Limit != nil {

		// query param limit
		var qrLimit int64
		if o.Limit != nil {
			qrLimit = *o.Limit
		}
		qLimit := swag.FormatInt64(qrLimit)
		if qLimit != "" {
			if err := r.SetQueryParam("limit", qLimit); err != nil {
				return err
			}
		}

	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
