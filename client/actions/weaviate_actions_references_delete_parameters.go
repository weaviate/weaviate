/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */ // Code generated by go-swagger; DO NOT EDIT.

package actions

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"
	"net/http"
	"time"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	cr "github.com/go-openapi/runtime/client"

	strfmt "github.com/go-openapi/strfmt"

	models "github.com/creativesoftwarefdn/weaviate/models"
)

// NewWeaviateActionsReferencesDeleteParams creates a new WeaviateActionsReferencesDeleteParams object
// with the default values initialized.
func NewWeaviateActionsReferencesDeleteParams() *WeaviateActionsReferencesDeleteParams {
	var ()
	return &WeaviateActionsReferencesDeleteParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewWeaviateActionsReferencesDeleteParamsWithTimeout creates a new WeaviateActionsReferencesDeleteParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewWeaviateActionsReferencesDeleteParamsWithTimeout(timeout time.Duration) *WeaviateActionsReferencesDeleteParams {
	var ()
	return &WeaviateActionsReferencesDeleteParams{

		timeout: timeout,
	}
}

// NewWeaviateActionsReferencesDeleteParamsWithContext creates a new WeaviateActionsReferencesDeleteParams object
// with the default values initialized, and the ability to set a context for a request
func NewWeaviateActionsReferencesDeleteParamsWithContext(ctx context.Context) *WeaviateActionsReferencesDeleteParams {
	var ()
	return &WeaviateActionsReferencesDeleteParams{

		Context: ctx,
	}
}

// NewWeaviateActionsReferencesDeleteParamsWithHTTPClient creates a new WeaviateActionsReferencesDeleteParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewWeaviateActionsReferencesDeleteParamsWithHTTPClient(client *http.Client) *WeaviateActionsReferencesDeleteParams {
	var ()
	return &WeaviateActionsReferencesDeleteParams{
		HTTPClient: client,
	}
}

/*WeaviateActionsReferencesDeleteParams contains all the parameters to send to the API endpoint
for the weaviate actions references delete operation typically these are written to a http.Request
*/
type WeaviateActionsReferencesDeleteParams struct {

	/*Body*/
	Body *models.SingleRef
	/*ID
	  Unique ID of the Action.

	*/
	ID strfmt.UUID
	/*PropertyName
	  Unique name of the property related to the Action.

	*/
	PropertyName string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) WithTimeout(timeout time.Duration) *WeaviateActionsReferencesDeleteParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) WithContext(ctx context.Context) *WeaviateActionsReferencesDeleteParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) WithHTTPClient(client *http.Client) *WeaviateActionsReferencesDeleteParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) WithBody(body *models.SingleRef) *WeaviateActionsReferencesDeleteParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) SetBody(body *models.SingleRef) {
	o.Body = body
}

// WithID adds the id to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) WithID(id strfmt.UUID) *WeaviateActionsReferencesDeleteParams {
	o.SetID(id)
	return o
}

// SetID adds the id to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) SetID(id strfmt.UUID) {
	o.ID = id
}

// WithPropertyName adds the propertyName to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) WithPropertyName(propertyName string) *WeaviateActionsReferencesDeleteParams {
	o.SetPropertyName(propertyName)
	return o
}

// SetPropertyName adds the propertyName to the weaviate actions references delete params
func (o *WeaviateActionsReferencesDeleteParams) SetPropertyName(propertyName string) {
	o.PropertyName = propertyName
}

// WriteToRequest writes these params to a swagger request
func (o *WeaviateActionsReferencesDeleteParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

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
