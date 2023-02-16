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

package backups

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

// NewBackupsCreateParams creates a new BackupsCreateParams object
// with the default values initialized.
func NewBackupsCreateParams() *BackupsCreateParams {
	var ()
	return &BackupsCreateParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewBackupsCreateParamsWithTimeout creates a new BackupsCreateParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewBackupsCreateParamsWithTimeout(timeout time.Duration) *BackupsCreateParams {
	var ()
	return &BackupsCreateParams{

		timeout: timeout,
	}
}

// NewBackupsCreateParamsWithContext creates a new BackupsCreateParams object
// with the default values initialized, and the ability to set a context for a request
func NewBackupsCreateParamsWithContext(ctx context.Context) *BackupsCreateParams {
	var ()
	return &BackupsCreateParams{

		Context: ctx,
	}
}

// NewBackupsCreateParamsWithHTTPClient creates a new BackupsCreateParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewBackupsCreateParamsWithHTTPClient(client *http.Client) *BackupsCreateParams {
	var ()
	return &BackupsCreateParams{
		HTTPClient: client,
	}
}

/*BackupsCreateParams contains all the parameters to send to the API endpoint
for the backups create operation typically these are written to a http.Request
*/
type BackupsCreateParams struct {

	/*Backend
	  Backup backend name e.g. filesystem, gcs, s3.

	*/
	Backend string
	/*Body*/
	Body *models.BackupCreateRequest

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the backups create params
func (o *BackupsCreateParams) WithTimeout(timeout time.Duration) *BackupsCreateParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the backups create params
func (o *BackupsCreateParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the backups create params
func (o *BackupsCreateParams) WithContext(ctx context.Context) *BackupsCreateParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the backups create params
func (o *BackupsCreateParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the backups create params
func (o *BackupsCreateParams) WithHTTPClient(client *http.Client) *BackupsCreateParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the backups create params
func (o *BackupsCreateParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBackend adds the backend to the backups create params
func (o *BackupsCreateParams) WithBackend(backend string) *BackupsCreateParams {
	o.SetBackend(backend)
	return o
}

// SetBackend adds the backend to the backups create params
func (o *BackupsCreateParams) SetBackend(backend string) {
	o.Backend = backend
}

// WithBody adds the body to the backups create params
func (o *BackupsCreateParams) WithBody(body *models.BackupCreateRequest) *BackupsCreateParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the backups create params
func (o *BackupsCreateParams) SetBody(body *models.BackupCreateRequest) {
	o.Body = body
}

// WriteToRequest writes these params to a swagger request
func (o *BackupsCreateParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param backend
	if err := r.SetPathParam("backend", o.Backend); err != nil {
		return err
	}

	if o.Body != nil {
		if err := r.SetBodyParam(o.Body); err != nil {
			return err
		}
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
