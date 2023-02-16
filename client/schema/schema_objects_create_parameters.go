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

	"github.com/weaviate/weaviate/entities/models"
)

// NewSchemaObjectsCreateParams creates a new SchemaObjectsCreateParams object
// with the default values initialized.
func NewSchemaObjectsCreateParams() *SchemaObjectsCreateParams {
	var ()
	return &SchemaObjectsCreateParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewSchemaObjectsCreateParamsWithTimeout creates a new SchemaObjectsCreateParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewSchemaObjectsCreateParamsWithTimeout(timeout time.Duration) *SchemaObjectsCreateParams {
	var ()
	return &SchemaObjectsCreateParams{

		timeout: timeout,
	}
}

// NewSchemaObjectsCreateParamsWithContext creates a new SchemaObjectsCreateParams object
// with the default values initialized, and the ability to set a context for a request
func NewSchemaObjectsCreateParamsWithContext(ctx context.Context) *SchemaObjectsCreateParams {
	var ()
	return &SchemaObjectsCreateParams{

		Context: ctx,
	}
}

// NewSchemaObjectsCreateParamsWithHTTPClient creates a new SchemaObjectsCreateParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewSchemaObjectsCreateParamsWithHTTPClient(client *http.Client) *SchemaObjectsCreateParams {
	var ()
	return &SchemaObjectsCreateParams{
		HTTPClient: client,
	}
}

/*SchemaObjectsCreateParams contains all the parameters to send to the API endpoint
for the schema objects create operation typically these are written to a http.Request
*/
type SchemaObjectsCreateParams struct {

	/*ObjectClass*/
	ObjectClass *models.Class

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the schema objects create params
func (o *SchemaObjectsCreateParams) WithTimeout(timeout time.Duration) *SchemaObjectsCreateParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the schema objects create params
func (o *SchemaObjectsCreateParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the schema objects create params
func (o *SchemaObjectsCreateParams) WithContext(ctx context.Context) *SchemaObjectsCreateParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the schema objects create params
func (o *SchemaObjectsCreateParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the schema objects create params
func (o *SchemaObjectsCreateParams) WithHTTPClient(client *http.Client) *SchemaObjectsCreateParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the schema objects create params
func (o *SchemaObjectsCreateParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithObjectClass adds the objectClass to the schema objects create params
func (o *SchemaObjectsCreateParams) WithObjectClass(objectClass *models.Class) *SchemaObjectsCreateParams {
	o.SetObjectClass(objectClass)
	return o
}

// SetObjectClass adds the objectClass to the schema objects create params
func (o *SchemaObjectsCreateParams) SetObjectClass(objectClass *models.Class) {
	o.ObjectClass = objectClass
}

// WriteToRequest writes these params to a swagger request
func (o *SchemaObjectsCreateParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

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
