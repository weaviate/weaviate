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

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"
	"time"

	"golang.org/x/net/context"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	cr "github.com/go-openapi/runtime/client"

	strfmt "github.com/go-openapi/strfmt"

	models "github.com/semi-technologies/weaviate/genesis/models"
)

// NewGenesisPeersRegisterParams creates a new GenesisPeersRegisterParams object
// with the default values initialized.
func NewGenesisPeersRegisterParams() *GenesisPeersRegisterParams {
	var ()
	return &GenesisPeersRegisterParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewGenesisPeersRegisterParamsWithTimeout creates a new GenesisPeersRegisterParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewGenesisPeersRegisterParamsWithTimeout(timeout time.Duration) *GenesisPeersRegisterParams {
	var ()
	return &GenesisPeersRegisterParams{

		timeout: timeout,
	}
}

// NewGenesisPeersRegisterParamsWithContext creates a new GenesisPeersRegisterParams object
// with the default values initialized, and the ability to set a context for a request
func NewGenesisPeersRegisterParamsWithContext(ctx context.Context) *GenesisPeersRegisterParams {
	var ()
	return &GenesisPeersRegisterParams{

		Context: ctx,
	}
}

// NewGenesisPeersRegisterParamsWithHTTPClient creates a new GenesisPeersRegisterParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewGenesisPeersRegisterParamsWithHTTPClient(client *http.Client) *GenesisPeersRegisterParams {
	var ()
	return &GenesisPeersRegisterParams{
		HTTPClient: client,
	}
}

/*GenesisPeersRegisterParams contains all the parameters to send to the API endpoint
for the genesis peers register operation typically these are written to a http.Request
*/
type GenesisPeersRegisterParams struct {

	/*Body*/
	Body *models.PeerUpdate

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the genesis peers register params
func (o *GenesisPeersRegisterParams) WithTimeout(timeout time.Duration) *GenesisPeersRegisterParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the genesis peers register params
func (o *GenesisPeersRegisterParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the genesis peers register params
func (o *GenesisPeersRegisterParams) WithContext(ctx context.Context) *GenesisPeersRegisterParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the genesis peers register params
func (o *GenesisPeersRegisterParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the genesis peers register params
func (o *GenesisPeersRegisterParams) WithHTTPClient(client *http.Client) *GenesisPeersRegisterParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the genesis peers register params
func (o *GenesisPeersRegisterParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithBody adds the body to the genesis peers register params
func (o *GenesisPeersRegisterParams) WithBody(body *models.PeerUpdate) *GenesisPeersRegisterParams {
	o.SetBody(body)
	return o
}

// SetBody adds the body to the genesis peers register params
func (o *GenesisPeersRegisterParams) SetBody(body *models.PeerUpdate) {
	o.Body = body
}

// WriteToRequest writes these params to a swagger request
func (o *GenesisPeersRegisterParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

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
