//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package users

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

// NewDeactivateUserParams creates a new DeactivateUserParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewDeactivateUserParams() *DeactivateUserParams {
	return &DeactivateUserParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewDeactivateUserParamsWithTimeout creates a new DeactivateUserParams object
// with the ability to set a timeout on a request.
func NewDeactivateUserParamsWithTimeout(timeout time.Duration) *DeactivateUserParams {
	return &DeactivateUserParams{
		timeout: timeout,
	}
}

// NewDeactivateUserParamsWithContext creates a new DeactivateUserParams object
// with the ability to set a context for a request.
func NewDeactivateUserParamsWithContext(ctx context.Context) *DeactivateUserParams {
	return &DeactivateUserParams{
		Context: ctx,
	}
}

// NewDeactivateUserParamsWithHTTPClient creates a new DeactivateUserParams object
// with the ability to set a custom HTTPClient for a request.
func NewDeactivateUserParamsWithHTTPClient(client *http.Client) *DeactivateUserParams {
	return &DeactivateUserParams{
		HTTPClient: client,
	}
}

/*
DeactivateUserParams contains all the parameters to send to the API endpoint

	for the deactivate user operation.

	Typically these are written to a http.Request.
*/
type DeactivateUserParams struct {

	// Body.
	Body DeactivateUserBody

	/* UserID.

	   user id
	*/
	UserID string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the deactivate user params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *DeactivateUserParams) WithDefaults() *DeactivateUserParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the deactivate user params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *DeactivateUserParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the deactivate user params
func (o *DeactivateUserParams) WithTimeout(timeout time.Duration) *DeactivateUserParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the deactivate user params
func (o *DeactivateUserParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the deactivate user params
func (o *DeactivateUserParams) WithContext(ctx context.Context) *DeactivateUserParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the deactivate user params
func (o *DeactivateUserParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the deactivate user params
func (o *DeactivateUserParams) WithHTTPClient(client *http.Client) *DeactivateUserParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the deactivate user params
func (o *DeactivateUserParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the deactivate user params
func (o *DeactivateUserParams) WithBody(body DeactivateUserBody) *DeactivateUserParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the deactivate user params
func (o *DeactivateUserParams) SetBody(body DeactivateUserBody) {
	o.Body = body
}

// WithUserID adds the userID to the deactivate user params
func (o *DeactivateUserParams) WithUserID(userID string) *DeactivateUserParams {
	o.SetUserID(userID)
	return o
}

// SetUserID adds the userId to the deactivate user params
func (o *DeactivateUserParams) SetUserID(userID string) {
	o.UserID = userID
}

// WriteToRequest writes these params to a swagger request
func (o *DeactivateUserParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error
	if err := r.SetBodyParam(o.Body); err != nil {
		return err
	}

	// path param user_id
	if err := r.SetPathParam("user_id", o.UserID); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
