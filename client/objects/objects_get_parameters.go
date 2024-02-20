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

// NewObjectsGetParams creates a new ObjectsGetParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewObjectsGetParams() *ObjectsGetParams {
	return &ObjectsGetParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewObjectsGetParamsWithTimeout creates a new ObjectsGetParams object
// with the ability to set a timeout on a request.
func NewObjectsGetParamsWithTimeout(timeout time.Duration) *ObjectsGetParams {
	return &ObjectsGetParams{
		timeout: timeout,
	}
}

// NewObjectsGetParamsWithContext creates a new ObjectsGetParams object
// with the ability to set a context for a request.
func NewObjectsGetParamsWithContext(ctx context.Context) *ObjectsGetParams {
	return &ObjectsGetParams{
		Context: ctx,
	}
}

// NewObjectsGetParamsWithHTTPClient creates a new ObjectsGetParams object
// with the ability to set a custom HTTPClient for a request.
func NewObjectsGetParamsWithHTTPClient(client *http.Client) *ObjectsGetParams {
	return &ObjectsGetParams{
		HTTPClient: client,
	}
}

/*
ObjectsGetParams contains all the parameters to send to the API endpoint

	for the objects get operation.

	Typically these are written to a http.Request.
*/
type ObjectsGetParams struct {

	/* ID.

	   Unique ID of the Object.

	   Format: uuid
	*/
	ID strfmt.UUID

	/* Include.

	   Include additional information, such as classification infos. Allowed values include: classification, vector, interpretation
	*/
	Include *string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the objects get params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *ObjectsGetParams) WithDefaults() *ObjectsGetParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the objects get params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *ObjectsGetParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the objects get params
func (o *ObjectsGetParams) WithTimeout(timeout time.Duration) *ObjectsGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the objects get params
func (o *ObjectsGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the objects get params
func (o *ObjectsGetParams) WithContext(ctx context.Context) *ObjectsGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the objects get params
func (o *ObjectsGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the objects get params
func (o *ObjectsGetParams) WithHTTPClient(client *http.Client) *ObjectsGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the objects get params
func (o *ObjectsGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithID adds the id to the objects get params
func (o *ObjectsGetParams) WithID(id strfmt.UUID) *ObjectsGetParams {
	o.SetID(id)
	return o
}

// SetID adds the id to the objects get params
func (o *ObjectsGetParams) SetID(id strfmt.UUID) {
	o.ID = id
}

// WithInclude adds the include to the objects get params
func (o *ObjectsGetParams) WithInclude(include *string) *ObjectsGetParams {
	o.SetInclude(include)
	return o
}

// SetInclude adds the include to the objects get params
func (o *ObjectsGetParams) SetInclude(include *string) {
	o.Include = include
}

// WriteToRequest writes these params to a swagger request
func (o *ObjectsGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param id
	if err := r.SetPathParam("id", o.ID.String()); err != nil {
		return err
	}

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

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
