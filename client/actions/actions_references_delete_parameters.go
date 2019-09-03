//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

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

	models "github.com/semi-technologies/weaviate/entities/models"
)

// NewActionsReferencesDeleteParams creates a new ActionsReferencesDeleteParams object
// with the default values initialized.
func NewActionsReferencesDeleteParams() *ActionsReferencesDeleteParams {
	var ()
	return &ActionsReferencesDeleteParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewActionsReferencesDeleteParamsWithTimeout creates a new ActionsReferencesDeleteParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewActionsReferencesDeleteParamsWithTimeout(timeout time.Duration) *ActionsReferencesDeleteParams {
	var ()
	return &ActionsReferencesDeleteParams{

		timeout: timeout,
	}
}

// NewActionsReferencesDeleteParamsWithContext creates a new ActionsReferencesDeleteParams object
// with the default values initialized, and the ability to set a context for a request
func NewActionsReferencesDeleteParamsWithContext(ctx context.Context) *ActionsReferencesDeleteParams {
	var ()
	return &ActionsReferencesDeleteParams{

		Context: ctx,
	}
}

// NewActionsReferencesDeleteParamsWithHTTPClient creates a new ActionsReferencesDeleteParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewActionsReferencesDeleteParamsWithHTTPClient(client *http.Client) *ActionsReferencesDeleteParams {
	var ()
	return &ActionsReferencesDeleteParams{
		HTTPClient: client,
	}
}

/*ActionsReferencesDeleteParams contains all the parameters to send to the API endpoint
for the actions references delete operation typically these are written to a http.Request
*/
type ActionsReferencesDeleteParams struct {

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

// WithTimeout adds the timeout to the actions references delete params
func (o *ActionsReferencesDeleteParams) WithTimeout(timeout time.Duration) *ActionsReferencesDeleteParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the actions references delete params
func (o *ActionsReferencesDeleteParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the actions references delete params
func (o *ActionsReferencesDeleteParams) WithContext(ctx context.Context) *ActionsReferencesDeleteParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the actions references delete params
func (o *ActionsReferencesDeleteParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the actions references delete params
func (o *ActionsReferencesDeleteParams) WithHTTPClient(client *http.Client) *ActionsReferencesDeleteParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the actions references delete params
func (o *ActionsReferencesDeleteParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the actions references delete params
func (o *ActionsReferencesDeleteParams) WithBody(body *models.SingleRef) *ActionsReferencesDeleteParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the actions references delete params
func (o *ActionsReferencesDeleteParams) SetBody(body *models.SingleRef) {
	o.Body = body
}

// WithID adds the id to the actions references delete params
func (o *ActionsReferencesDeleteParams) WithID(id strfmt.UUID) *ActionsReferencesDeleteParams {
	o.SetID(id)
	return o
}

// SetID adds the id to the actions references delete params
func (o *ActionsReferencesDeleteParams) SetID(id strfmt.UUID) {
	o.ID = id
}

// WithPropertyName adds the propertyName to the actions references delete params
func (o *ActionsReferencesDeleteParams) WithPropertyName(propertyName string) *ActionsReferencesDeleteParams {
	o.SetPropertyName(propertyName)
	return o
}

// SetPropertyName adds the propertyName to the actions references delete params
func (o *ActionsReferencesDeleteParams) SetPropertyName(propertyName string) {
	o.PropertyName = propertyName
}

// WriteToRequest writes these params to a swagger request
func (o *ActionsReferencesDeleteParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

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
