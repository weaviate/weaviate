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

	"github.com/weaviate/weaviate/entities/models"
)

// NewObjectsPatchParams creates a new ObjectsPatchParams object
// with the default values initialized.
func NewObjectsPatchParams() *ObjectsPatchParams {
	var ()
	return &ObjectsPatchParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewObjectsPatchParamsWithTimeout creates a new ObjectsPatchParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewObjectsPatchParamsWithTimeout(timeout time.Duration) *ObjectsPatchParams {
	var ()
	return &ObjectsPatchParams{

		timeout: timeout,
	}
}

// NewObjectsPatchParamsWithContext creates a new ObjectsPatchParams object
// with the default values initialized, and the ability to set a context for a request
func NewObjectsPatchParamsWithContext(ctx context.Context) *ObjectsPatchParams {
	var ()
	return &ObjectsPatchParams{

		Context: ctx,
	}
}

// NewObjectsPatchParamsWithHTTPClient creates a new ObjectsPatchParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewObjectsPatchParamsWithHTTPClient(client *http.Client) *ObjectsPatchParams {
	var ()
	return &ObjectsPatchParams{
		HTTPClient: client,
	}
}

/*ObjectsPatchParams contains all the parameters to send to the API endpoint
for the objects patch operation typically these are written to a http.Request
*/
type ObjectsPatchParams struct {

	/*Body
	  RFC 7396-style patch, the body contains the object to merge into the existing object.

	*/
	Body *models.Object
	/*ConsistencyLevel
	  Determines how many replicas must acknowledge a request before it is considered successful

	*/
	ConsistencyLevel *string
	/*ID
	  Unique ID of the Object.

	*/
	ID strfmt.UUID

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the objects patch params
func (o *ObjectsPatchParams) WithTimeout(timeout time.Duration) *ObjectsPatchParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the objects patch params
func (o *ObjectsPatchParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the objects patch params
func (o *ObjectsPatchParams) WithContext(ctx context.Context) *ObjectsPatchParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the objects patch params
func (o *ObjectsPatchParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the objects patch params
func (o *ObjectsPatchParams) WithHTTPClient(client *http.Client) *ObjectsPatchParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the objects patch params
func (o *ObjectsPatchParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the objects patch params
func (o *ObjectsPatchParams) WithBody(body *models.Object) *ObjectsPatchParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the objects patch params
func (o *ObjectsPatchParams) SetBody(body *models.Object) {
	o.Body = body
}

// WithConsistencyLevel adds the consistencyLevel to the objects patch params
func (o *ObjectsPatchParams) WithConsistencyLevel(consistencyLevel *string) *ObjectsPatchParams {
	o.SetConsistencyLevel(consistencyLevel)
	return o
}

// SetConsistencyLevel adds the consistencyLevel to the objects patch params
func (o *ObjectsPatchParams) SetConsistencyLevel(consistencyLevel *string) {
	o.ConsistencyLevel = consistencyLevel
}

// WithID adds the id to the objects patch params
func (o *ObjectsPatchParams) WithID(id strfmt.UUID) *ObjectsPatchParams {
	o.SetID(id)
	return o
}

// SetID adds the id to the objects patch params
func (o *ObjectsPatchParams) SetID(id strfmt.UUID) {
	o.ID = id
}

// WriteToRequest writes these params to a swagger request
func (o *ObjectsPatchParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if o.Body != nil {
		if err := r.SetBodyParam(o.Body); err != nil {
			return err
		}
	}

	if o.ConsistencyLevel != nil {

		// query param consistency_level
		var qrConsistencyLevel string
		if o.ConsistencyLevel != nil {
			qrConsistencyLevel = *o.ConsistencyLevel
		}
		qConsistencyLevel := qrConsistencyLevel
		if qConsistencyLevel != "" {
			if err := r.SetQueryParam("consistency_level", qConsistencyLevel); err != nil {
				return err
			}
		}

	}

	// path param id
	if err := r.SetPathParam("id", o.ID.String()); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
