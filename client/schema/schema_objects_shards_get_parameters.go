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

// NewSchemaObjectsShardsGetParams creates a new SchemaObjectsShardsGetParams object
// with the default values initialized.
func NewSchemaObjectsShardsGetParams() *SchemaObjectsShardsGetParams {
	var ()
	return &SchemaObjectsShardsGetParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewSchemaObjectsShardsGetParamsWithTimeout creates a new SchemaObjectsShardsGetParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewSchemaObjectsShardsGetParamsWithTimeout(timeout time.Duration) *SchemaObjectsShardsGetParams {
	var ()
	return &SchemaObjectsShardsGetParams{

		timeout: timeout,
	}
}

// NewSchemaObjectsShardsGetParamsWithContext creates a new SchemaObjectsShardsGetParams object
// with the default values initialized, and the ability to set a context for a request
func NewSchemaObjectsShardsGetParamsWithContext(ctx context.Context) *SchemaObjectsShardsGetParams {
	var ()
	return &SchemaObjectsShardsGetParams{

		Context: ctx,
	}
}

// NewSchemaObjectsShardsGetParamsWithHTTPClient creates a new SchemaObjectsShardsGetParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewSchemaObjectsShardsGetParamsWithHTTPClient(client *http.Client) *SchemaObjectsShardsGetParams {
	var ()
	return &SchemaObjectsShardsGetParams{
		HTTPClient: client,
	}
}

/*SchemaObjectsShardsGetParams contains all the parameters to send to the API endpoint
for the schema objects shards get operation typically these are written to a http.Request
*/
type SchemaObjectsShardsGetParams struct {

	/*ClassName*/
	ClassName string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the schema objects shards get params
func (o *SchemaObjectsShardsGetParams) WithTimeout(timeout time.Duration) *SchemaObjectsShardsGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the schema objects shards get params
func (o *SchemaObjectsShardsGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the schema objects shards get params
func (o *SchemaObjectsShardsGetParams) WithContext(ctx context.Context) *SchemaObjectsShardsGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the schema objects shards get params
func (o *SchemaObjectsShardsGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the schema objects shards get params
func (o *SchemaObjectsShardsGetParams) WithHTTPClient(client *http.Client) *SchemaObjectsShardsGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the schema objects shards get params
func (o *SchemaObjectsShardsGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithClassName adds the className to the schema objects shards get params
func (o *SchemaObjectsShardsGetParams) WithClassName(className string) *SchemaObjectsShardsGetParams {
	o.SetClassName(className)
	return o
}

// SetClassName adds the className to the schema objects shards get params
func (o *SchemaObjectsShardsGetParams) SetClassName(className string) {
	o.ClassName = className
}

// WriteToRequest writes these params to a swagger request
func (o *SchemaObjectsShardsGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

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
