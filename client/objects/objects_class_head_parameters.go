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
)

// NewObjectsClassHeadParams creates a new ObjectsClassHeadParams object
// with the default values initialized.
func NewObjectsClassHeadParams() *ObjectsClassHeadParams {
	var ()
	return &ObjectsClassHeadParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewObjectsClassHeadParamsWithTimeout creates a new ObjectsClassHeadParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewObjectsClassHeadParamsWithTimeout(timeout time.Duration) *ObjectsClassHeadParams {
	var ()
	return &ObjectsClassHeadParams{

		timeout: timeout,
	}
}

// NewObjectsClassHeadParamsWithContext creates a new ObjectsClassHeadParams object
// with the default values initialized, and the ability to set a context for a request
func NewObjectsClassHeadParamsWithContext(ctx context.Context) *ObjectsClassHeadParams {
	var ()
	return &ObjectsClassHeadParams{

		Context: ctx,
	}
}

// NewObjectsClassHeadParamsWithHTTPClient creates a new ObjectsClassHeadParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewObjectsClassHeadParamsWithHTTPClient(client *http.Client) *ObjectsClassHeadParams {
	var ()
	return &ObjectsClassHeadParams{
		HTTPClient: client,
	}
}

/*
ObjectsClassHeadParams contains all the parameters to send to the API endpoint
for the objects class head operation typically these are written to a http.Request
*/
type ObjectsClassHeadParams struct {

	/*ClassName
	  The class name as defined in the schema

	*/
	ClassName string
	/*ID
	  The uuid of the data object

	*/
	ID strfmt.UUID

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the objects class head params
func (o *ObjectsClassHeadParams) WithTimeout(timeout time.Duration) *ObjectsClassHeadParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the objects class head params
func (o *ObjectsClassHeadParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the objects class head params
func (o *ObjectsClassHeadParams) WithContext(ctx context.Context) *ObjectsClassHeadParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the objects class head params
func (o *ObjectsClassHeadParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the objects class head params
func (o *ObjectsClassHeadParams) WithHTTPClient(client *http.Client) *ObjectsClassHeadParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the objects class head params
func (o *ObjectsClassHeadParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithClassName adds the className to the objects class head params
func (o *ObjectsClassHeadParams) WithClassName(className string) *ObjectsClassHeadParams {
	o.SetClassName(className)
	return o
}

// SetClassName adds the className to the objects class head params
func (o *ObjectsClassHeadParams) SetClassName(className string) {
	o.ClassName = className
}

// WithID adds the id to the objects class head params
func (o *ObjectsClassHeadParams) WithID(id strfmt.UUID) *ObjectsClassHeadParams {
	o.SetID(id)
	return o
}

// SetID adds the id to the objects class head params
func (o *ObjectsClassHeadParams) SetID(id strfmt.UUID) {
	o.ID = id
}

// WriteToRequest writes these params to a swagger request
func (o *ObjectsClassHeadParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param className
	if err := r.SetPathParam("className", o.ClassName); err != nil {
		return err
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
