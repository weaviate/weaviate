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

// NewObjectsReferencesUpdateParams creates a new ObjectsReferencesUpdateParams object
// with the default values initialized.
func NewObjectsReferencesUpdateParams() *ObjectsReferencesUpdateParams {
	var ()
	return &ObjectsReferencesUpdateParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewObjectsReferencesUpdateParamsWithTimeout creates a new ObjectsReferencesUpdateParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewObjectsReferencesUpdateParamsWithTimeout(timeout time.Duration) *ObjectsReferencesUpdateParams {
	var ()
	return &ObjectsReferencesUpdateParams{

		timeout: timeout,
	}
}

// NewObjectsReferencesUpdateParamsWithContext creates a new ObjectsReferencesUpdateParams object
// with the default values initialized, and the ability to set a context for a request
func NewObjectsReferencesUpdateParamsWithContext(ctx context.Context) *ObjectsReferencesUpdateParams {
	var ()
	return &ObjectsReferencesUpdateParams{

		Context: ctx,
	}
}

// NewObjectsReferencesUpdateParamsWithHTTPClient creates a new ObjectsReferencesUpdateParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewObjectsReferencesUpdateParamsWithHTTPClient(client *http.Client) *ObjectsReferencesUpdateParams {
	var ()
	return &ObjectsReferencesUpdateParams{
		HTTPClient: client,
	}
}

/*ObjectsReferencesUpdateParams contains all the parameters to send to the API endpoint
for the objects references update operation typically these are written to a http.Request
*/
type ObjectsReferencesUpdateParams struct {

	/*Body*/
	Body models.MultipleRef
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

// WithTimeout adds the timeout to the objects references update params
func (o *ObjectsReferencesUpdateParams) WithTimeout(timeout time.Duration) *ObjectsReferencesUpdateParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the objects references update params
func (o *ObjectsReferencesUpdateParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the objects references update params
func (o *ObjectsReferencesUpdateParams) WithContext(ctx context.Context) *ObjectsReferencesUpdateParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the objects references update params
func (o *ObjectsReferencesUpdateParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the objects references update params
func (o *ObjectsReferencesUpdateParams) WithHTTPClient(client *http.Client) *ObjectsReferencesUpdateParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the objects references update params
func (o *ObjectsReferencesUpdateParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the objects references update params
func (o *ObjectsReferencesUpdateParams) WithBody(body models.MultipleRef) *ObjectsReferencesUpdateParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the objects references update params
func (o *ObjectsReferencesUpdateParams) SetBody(body models.MultipleRef) {
	o.Body = body
}

// WithID adds the id to the objects references update params
func (o *ObjectsReferencesUpdateParams) WithID(id strfmt.UUID) *ObjectsReferencesUpdateParams {
	o.SetID(id)
	return o
}

// SetID adds the id to the objects references update params
func (o *ObjectsReferencesUpdateParams) SetID(id strfmt.UUID) {
	o.ID = id
}

// WithPropertyName adds the propertyName to the objects references update params
func (o *ObjectsReferencesUpdateParams) WithPropertyName(propertyName string) *ObjectsReferencesUpdateParams {
	o.SetPropertyName(propertyName)
	return o
}

// SetPropertyName adds the propertyName to the objects references update params
func (o *ObjectsReferencesUpdateParams) SetPropertyName(propertyName string) {
	o.PropertyName = propertyName
}

// WriteToRequest writes these params to a swagger request
func (o *ObjectsReferencesUpdateParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if o.Body != nil {
		if err := r.SetBodyParam(o.Body); err != nil {
			return err
		}
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
