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

	"github.com/semi-technologies/weaviate/entities/models"
)

// NewObjectsClassReferencesPutParams creates a new ObjectsClassReferencesPutParams object
// with the default values initialized.
func NewObjectsClassReferencesPutParams() *ObjectsClassReferencesPutParams {
	var ()
	return &ObjectsClassReferencesPutParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewObjectsClassReferencesPutParamsWithTimeout creates a new ObjectsClassReferencesPutParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewObjectsClassReferencesPutParamsWithTimeout(timeout time.Duration) *ObjectsClassReferencesPutParams {
	var ()
	return &ObjectsClassReferencesPutParams{

		timeout: timeout,
	}
}

// NewObjectsClassReferencesPutParamsWithContext creates a new ObjectsClassReferencesPutParams object
// with the default values initialized, and the ability to set a context for a request
func NewObjectsClassReferencesPutParamsWithContext(ctx context.Context) *ObjectsClassReferencesPutParams {
	var ()
	return &ObjectsClassReferencesPutParams{

		Context: ctx,
	}
}

// NewObjectsClassReferencesPutParamsWithHTTPClient creates a new ObjectsClassReferencesPutParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewObjectsClassReferencesPutParamsWithHTTPClient(client *http.Client) *ObjectsClassReferencesPutParams {
	var ()
	return &ObjectsClassReferencesPutParams{
		HTTPClient: client,
	}
}

/*ObjectsClassReferencesPutParams contains all the parameters to send to the API endpoint
for the objects class references put operation typically these are written to a http.Request
*/
type ObjectsClassReferencesPutParams struct {

	/*Body*/
	Body models.MultipleRef
	/*ClassName
	  The class name as defined in the schema

	*/
	ClassName string
	/*ID
	  Unique ID of the Object.

	*/
	ID strfmt.UUID
	/*PropertyName
	  Unique name of the property related to the Object.

	*/
	PropertyName string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the objects class references put params
func (o *ObjectsClassReferencesPutParams) WithTimeout(timeout time.Duration) *ObjectsClassReferencesPutParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the objects class references put params
func (o *ObjectsClassReferencesPutParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the objects class references put params
func (o *ObjectsClassReferencesPutParams) WithContext(ctx context.Context) *ObjectsClassReferencesPutParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the objects class references put params
func (o *ObjectsClassReferencesPutParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the objects class references put params
func (o *ObjectsClassReferencesPutParams) WithHTTPClient(client *http.Client) *ObjectsClassReferencesPutParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the objects class references put params
func (o *ObjectsClassReferencesPutParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the objects class references put params
func (o *ObjectsClassReferencesPutParams) WithBody(body models.MultipleRef) *ObjectsClassReferencesPutParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the objects class references put params
func (o *ObjectsClassReferencesPutParams) SetBody(body models.MultipleRef) {
	o.Body = body
}

// WithClassName adds the className to the objects class references put params
func (o *ObjectsClassReferencesPutParams) WithClassName(className string) *ObjectsClassReferencesPutParams {
	o.SetClassName(className)
	return o
}

// SetClassName adds the className to the objects class references put params
func (o *ObjectsClassReferencesPutParams) SetClassName(className string) {
	o.ClassName = className
}

// WithID adds the id to the objects class references put params
func (o *ObjectsClassReferencesPutParams) WithID(id strfmt.UUID) *ObjectsClassReferencesPutParams {
	o.SetID(id)
	return o
}

// SetID adds the id to the objects class references put params
func (o *ObjectsClassReferencesPutParams) SetID(id strfmt.UUID) {
	o.ID = id
}

// WithPropertyName adds the propertyName to the objects class references put params
func (o *ObjectsClassReferencesPutParams) WithPropertyName(propertyName string) *ObjectsClassReferencesPutParams {
	o.SetPropertyName(propertyName)
	return o
}

// SetPropertyName adds the propertyName to the objects class references put params
func (o *ObjectsClassReferencesPutParams) SetPropertyName(propertyName string) {
	o.PropertyName = propertyName
}

// WriteToRequest writes these params to a swagger request
func (o *ObjectsClassReferencesPutParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if o.Body != nil {
		if err := r.SetBodyParam(o.Body); err != nil {
			return err
		}
	}

	// path param className
	if err := r.SetPathParam("className", o.ClassName); err != nil {
		return err
	}

	// path param id
	if err := r.SetPathParam("id", o.ID.String()); err != nil {
		return err
	}

	// path param propertyName
	if err := r.SetPathParam("propertyName", o.PropertyName); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
