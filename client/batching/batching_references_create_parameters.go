//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package batching

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

// NewBatchingReferencesCreateParams creates a new BatchingReferencesCreateParams object
// with the default values initialized.
func NewBatchingReferencesCreateParams() *BatchingReferencesCreateParams {
	var ()
	return &BatchingReferencesCreateParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewBatchingReferencesCreateParamsWithTimeout creates a new BatchingReferencesCreateParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewBatchingReferencesCreateParamsWithTimeout(timeout time.Duration) *BatchingReferencesCreateParams {
	var ()
	return &BatchingReferencesCreateParams{

		timeout: timeout,
	}
}

// NewBatchingReferencesCreateParamsWithContext creates a new BatchingReferencesCreateParams object
// with the default values initialized, and the ability to set a context for a request
func NewBatchingReferencesCreateParamsWithContext(ctx context.Context) *BatchingReferencesCreateParams {
	var ()
	return &BatchingReferencesCreateParams{

		Context: ctx,
	}
}

// NewBatchingReferencesCreateParamsWithHTTPClient creates a new BatchingReferencesCreateParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewBatchingReferencesCreateParamsWithHTTPClient(client *http.Client) *BatchingReferencesCreateParams {
	var ()
	return &BatchingReferencesCreateParams{
		HTTPClient: client,
	}
}

/*BatchingReferencesCreateParams contains all the parameters to send to the API endpoint
for the batching references create operation typically these are written to a http.Request
*/
type BatchingReferencesCreateParams struct {

	/*Body
	  A list of references to be batched. The ideal size depends on the used database connector. Please see the documentation of the used connector for help

	*/
	Body []*models.BatchReference

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the batching references create params
func (o *BatchingReferencesCreateParams) WithTimeout(timeout time.Duration) *BatchingReferencesCreateParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the batching references create params
func (o *BatchingReferencesCreateParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the batching references create params
func (o *BatchingReferencesCreateParams) WithContext(ctx context.Context) *BatchingReferencesCreateParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the batching references create params
func (o *BatchingReferencesCreateParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the batching references create params
func (o *BatchingReferencesCreateParams) WithHTTPClient(client *http.Client) *BatchingReferencesCreateParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the batching references create params
func (o *BatchingReferencesCreateParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the batching references create params
func (o *BatchingReferencesCreateParams) WithBody(body []*models.BatchReference) *BatchingReferencesCreateParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the batching references create params
func (o *BatchingReferencesCreateParams) SetBody(body []*models.BatchReference) {
	o.Body = body
}

// WriteToRequest writes these params to a swagger request
func (o *BatchingReferencesCreateParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

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
