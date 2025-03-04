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

// NewSuspendUserParams creates a new SuspendUserParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewSuspendUserParams() *SuspendUserParams {
	return &SuspendUserParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewSuspendUserParamsWithTimeout creates a new SuspendUserParams object
// with the ability to set a timeout on a request.
func NewSuspendUserParamsWithTimeout(timeout time.Duration) *SuspendUserParams {
	return &SuspendUserParams{
		timeout: timeout,
	}
}

// NewSuspendUserParamsWithContext creates a new SuspendUserParams object
// with the ability to set a context for a request.
func NewSuspendUserParamsWithContext(ctx context.Context) *SuspendUserParams {
	return &SuspendUserParams{
		Context: ctx,
	}
}

// NewSuspendUserParamsWithHTTPClient creates a new SuspendUserParams object
// with the ability to set a custom HTTPClient for a request.
func NewSuspendUserParamsWithHTTPClient(client *http.Client) *SuspendUserParams {
	return &SuspendUserParams{
		HTTPClient: client,
	}
}

/*
SuspendUserParams contains all the parameters to send to the API endpoint

	for the suspend user operation.

	Typically these are written to a http.Request.
*/
type SuspendUserParams struct {

	// Body.
	Body SuspendUserBody

	/* UserID.

	   user id
	*/
	UserID string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the suspend user params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *SuspendUserParams) WithDefaults() *SuspendUserParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the suspend user params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *SuspendUserParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the suspend user params
func (o *SuspendUserParams) WithTimeout(timeout time.Duration) *SuspendUserParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the suspend user params
func (o *SuspendUserParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the suspend user params
func (o *SuspendUserParams) WithContext(ctx context.Context) *SuspendUserParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the suspend user params
func (o *SuspendUserParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the suspend user params
func (o *SuspendUserParams) WithHTTPClient(client *http.Client) *SuspendUserParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the suspend user params
func (o *SuspendUserParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the suspend user params
func (o *SuspendUserParams) WithBody(body SuspendUserBody) *SuspendUserParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the suspend user params
func (o *SuspendUserParams) SetBody(body SuspendUserBody) {
	o.Body = body
}

// WithUserID adds the userID to the suspend user params
func (o *SuspendUserParams) WithUserID(userID string) *SuspendUserParams {
	o.SetUserID(userID)
	return o
}

// SetUserID adds the userId to the suspend user params
func (o *SuspendUserParams) SetUserID(userID string) {
	o.UserID = userID
}

// WriteToRequest writes these params to a swagger request
func (o *SuspendUserParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

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
