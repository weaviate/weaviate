//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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

	"github.com/liutizhong/weaviate/entities/models"
)

// NewObjectsClassReferencesCreateParams creates a new ObjectsClassReferencesCreateParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewObjectsClassReferencesCreateParams() *ObjectsClassReferencesCreateParams {
	return &ObjectsClassReferencesCreateParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewObjectsClassReferencesCreateParamsWithTimeout creates a new ObjectsClassReferencesCreateParams object
// with the ability to set a timeout on a request.
func NewObjectsClassReferencesCreateParamsWithTimeout(timeout time.Duration) *ObjectsClassReferencesCreateParams {
	return &ObjectsClassReferencesCreateParams{
		timeout: timeout,
	}
}

// NewObjectsClassReferencesCreateParamsWithContext creates a new ObjectsClassReferencesCreateParams object
// with the ability to set a context for a request.
func NewObjectsClassReferencesCreateParamsWithContext(ctx context.Context) *ObjectsClassReferencesCreateParams {
	return &ObjectsClassReferencesCreateParams{
		Context: ctx,
	}
}

// NewObjectsClassReferencesCreateParamsWithHTTPClient creates a new ObjectsClassReferencesCreateParams object
// with the ability to set a custom HTTPClient for a request.
func NewObjectsClassReferencesCreateParamsWithHTTPClient(client *http.Client) *ObjectsClassReferencesCreateParams {
	return &ObjectsClassReferencesCreateParams{
		HTTPClient: client,
	}
}

/*
ObjectsClassReferencesCreateParams contains all the parameters to send to the API endpoint

	for the objects class references create operation.

	Typically these are written to a http.Request.
*/
type ObjectsClassReferencesCreateParams struct {

	// Body.
	Body *models.SingleRef

	/* ClassName.

	   The class name as defined in the schema
	*/
	ClassName string

	/* ConsistencyLevel.

	   Determines how many replicas must acknowledge a request before it is considered successful
	*/
	ConsistencyLevel *string

	/* ID.

	   Unique ID of the Object.

	   Format: uuid
	*/
	ID strfmt.UUID

	/* PropertyName.

	   Unique name of the property related to the Object.
	*/
	PropertyName string

	/* Tenant.

	   Specifies the tenant in a request targeting a multi-tenant class
	*/
	Tenant *string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the objects class references create params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *ObjectsClassReferencesCreateParams) WithDefaults() *ObjectsClassReferencesCreateParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the objects class references create params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *ObjectsClassReferencesCreateParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) WithTimeout(timeout time.Duration) *ObjectsClassReferencesCreateParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) WithContext(ctx context.Context) *ObjectsClassReferencesCreateParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) WithHTTPClient(client *http.Client) *ObjectsClassReferencesCreateParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) WithBody(body *models.SingleRef) *ObjectsClassReferencesCreateParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) SetBody(body *models.SingleRef) {
	o.Body = body
}

// WithClassName adds the className to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) WithClassName(className string) *ObjectsClassReferencesCreateParams {
	o.SetClassName(className)
	return o
}

// SetClassName adds the className to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) SetClassName(className string) {
	o.ClassName = className
}

// WithConsistencyLevel adds the consistencyLevel to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) WithConsistencyLevel(consistencyLevel *string) *ObjectsClassReferencesCreateParams {
	o.SetConsistencyLevel(consistencyLevel)
	return o
}

// SetConsistencyLevel adds the consistencyLevel to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) SetConsistencyLevel(consistencyLevel *string) {
	o.ConsistencyLevel = consistencyLevel
}

// WithID adds the id to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) WithID(id strfmt.UUID) *ObjectsClassReferencesCreateParams {
	o.SetID(id)
	return o
}

// SetID adds the id to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) SetID(id strfmt.UUID) {
	o.ID = id
}

// WithPropertyName adds the propertyName to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) WithPropertyName(propertyName string) *ObjectsClassReferencesCreateParams {
	o.SetPropertyName(propertyName)
	return o
}

// SetPropertyName adds the propertyName to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) SetPropertyName(propertyName string) {
	o.PropertyName = propertyName
}

// WithTenant adds the tenant to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) WithTenant(tenant *string) *ObjectsClassReferencesCreateParams {
	o.SetTenant(tenant)
	return o
}

// SetTenant adds the tenant to the objects class references create params
func (o *ObjectsClassReferencesCreateParams) SetTenant(tenant *string) {
	o.Tenant = tenant
}

// WriteToRequest writes these params to a swagger request
func (o *ObjectsClassReferencesCreateParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

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

	// path param propertyName
	if err := r.SetPathParam("propertyName", o.PropertyName); err != nil {
		return err
	}

	if o.Tenant != nil {

		// query param tenant
		var qrTenant string

		if o.Tenant != nil {
			qrTenant = *o.Tenant
		}
		qTenant := qrTenant
		if qTenant != "" {

			if err := r.SetQueryParam("tenant", qTenant); err != nil {
				return err
			}
		}
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
