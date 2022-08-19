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

	"github.com/semi-technologies/weaviate/entities/models"
)

// NewSchemaObjectsUpdateParams creates a new SchemaObjectsUpdateParams object
// with the default values initialized.
func NewSchemaObjectsUpdateParams() *SchemaObjectsUpdateParams {
	var ()
	return &SchemaObjectsUpdateParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewSchemaObjectsUpdateParamsWithTimeout creates a new SchemaObjectsUpdateParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewSchemaObjectsUpdateParamsWithTimeout(timeout time.Duration) *SchemaObjectsUpdateParams {
	var ()
	return &SchemaObjectsUpdateParams{

		timeout: timeout,
	}
}

// NewSchemaObjectsUpdateParamsWithContext creates a new SchemaObjectsUpdateParams object
// with the default values initialized, and the ability to set a context for a request
func NewSchemaObjectsUpdateParamsWithContext(ctx context.Context) *SchemaObjectsUpdateParams {
	var ()
	return &SchemaObjectsUpdateParams{

		Context: ctx,
	}
}

// NewSchemaObjectsUpdateParamsWithHTTPClient creates a new SchemaObjectsUpdateParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewSchemaObjectsUpdateParamsWithHTTPClient(client *http.Client) *SchemaObjectsUpdateParams {
	var ()
	return &SchemaObjectsUpdateParams{
		HTTPClient: client,
	}
}

/*SchemaObjectsUpdateParams contains all the parameters to send to the API endpoint
for the schema objects update operation typically these are written to a http.Request
*/
type SchemaObjectsUpdateParams struct {

	/*ClassName*/
	ClassName string
	/*ObjectClass*/
	ObjectClass *models.Class

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the schema objects update params
func (o *SchemaObjectsUpdateParams) WithTimeout(timeout time.Duration) *SchemaObjectsUpdateParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the schema objects update params
func (o *SchemaObjectsUpdateParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the schema objects update params
func (o *SchemaObjectsUpdateParams) WithContext(ctx context.Context) *SchemaObjectsUpdateParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the schema objects update params
func (o *SchemaObjectsUpdateParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the schema objects update params
func (o *SchemaObjectsUpdateParams) WithHTTPClient(client *http.Client) *SchemaObjectsUpdateParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the schema objects update params
func (o *SchemaObjectsUpdateParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithClassName adds the className to the schema objects update params
func (o *SchemaObjectsUpdateParams) WithClassName(className string) *SchemaObjectsUpdateParams {
	o.SetClassName(className)
	return o
}

// SetClassName adds the className to the schema objects update params
func (o *SchemaObjectsUpdateParams) SetClassName(className string) {
	o.ClassName = className
}

// WithObjectClass adds the objectClass to the schema objects update params
func (o *SchemaObjectsUpdateParams) WithObjectClass(objectClass *models.Class) *SchemaObjectsUpdateParams {
	o.SetObjectClass(objectClass)
	return o
}

// SetObjectClass adds the objectClass to the schema objects update params
func (o *SchemaObjectsUpdateParams) SetObjectClass(objectClass *models.Class) {
	o.ObjectClass = objectClass
}

// WriteToRequest writes these params to a swagger request
func (o *SchemaObjectsUpdateParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param className
	if err := r.SetPathParam("className", o.ClassName); err != nil {
		return err
	}

	if o.ObjectClass != nil {
		if err := r.SetBodyParam(o.ObjectClass); err != nil {
			return err
		}
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
