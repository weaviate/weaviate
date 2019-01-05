/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
// Code generated by go-swagger; DO NOT EDIT.

package things

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"
	"time"

	"golang.org/x/net/context"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	cr "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/swag"

	strfmt "github.com/go-openapi/strfmt"
)

// NewWeaviateThingsListParams creates a new WeaviateThingsListParams object
// with the default values initialized.
func NewWeaviateThingsListParams() *WeaviateThingsListParams {
	var ()
	return &WeaviateThingsListParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewWeaviateThingsListParamsWithTimeout creates a new WeaviateThingsListParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewWeaviateThingsListParamsWithTimeout(timeout time.Duration) *WeaviateThingsListParams {
	var ()
	return &WeaviateThingsListParams{

		timeout: timeout,
	}
}

// NewWeaviateThingsListParamsWithContext creates a new WeaviateThingsListParams object
// with the default values initialized, and the ability to set a context for a request
func NewWeaviateThingsListParamsWithContext(ctx context.Context) *WeaviateThingsListParams {
	var ()
	return &WeaviateThingsListParams{

		Context: ctx,
	}
}

// NewWeaviateThingsListParamsWithHTTPClient creates a new WeaviateThingsListParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewWeaviateThingsListParamsWithHTTPClient(client *http.Client) *WeaviateThingsListParams {
	var ()
	return &WeaviateThingsListParams{
		HTTPClient: client,
	}
}

/*WeaviateThingsListParams contains all the parameters to send to the API endpoint
for the weaviate things list operation typically these are written to a http.Request
*/
type WeaviateThingsListParams struct {

	/*MaxResults
	  The maximum number of items to be returned per page. Default value is set in Weaviate config.

	*/
	MaxResults *int64
	/*Page
	  The page number of the items to be returned.

	*/
	Page *int64

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the weaviate things list params
func (o *WeaviateThingsListParams) WithTimeout(timeout time.Duration) *WeaviateThingsListParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the weaviate things list params
func (o *WeaviateThingsListParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the weaviate things list params
func (o *WeaviateThingsListParams) WithContext(ctx context.Context) *WeaviateThingsListParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the weaviate things list params
func (o *WeaviateThingsListParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the weaviate things list params
func (o *WeaviateThingsListParams) WithHTTPClient(client *http.Client) *WeaviateThingsListParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the weaviate things list params
func (o *WeaviateThingsListParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithMaxResults adds the maxResults to the weaviate things list params
func (o *WeaviateThingsListParams) WithMaxResults(maxResults *int64) *WeaviateThingsListParams {
	o.SetMaxResults(maxResults)
	return o
}

// SetMaxResults adds the maxResults to the weaviate things list params
func (o *WeaviateThingsListParams) SetMaxResults(maxResults *int64) {
	o.MaxResults = maxResults
}

// WithPage adds the page to the weaviate things list params
func (o *WeaviateThingsListParams) WithPage(page *int64) *WeaviateThingsListParams {
	o.SetPage(page)
	return o
}

// SetPage adds the page to the weaviate things list params
func (o *WeaviateThingsListParams) SetPage(page *int64) {
	o.Page = page
}

// WriteToRequest writes these params to a swagger request
func (o *WeaviateThingsListParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if o.MaxResults != nil {

		// query param maxResults
		var qrMaxResults int64
		if o.MaxResults != nil {
			qrMaxResults = *o.MaxResults
		}
		qMaxResults := swag.FormatInt64(qrMaxResults)
		if qMaxResults != "" {
			if err := r.SetQueryParam("maxResults", qMaxResults); err != nil {
				return err
			}
		}

	}

	if o.Page != nil {

		// query param page
		var qrPage int64
		if o.Page != nil {
			qrPage = *o.Page
		}
		qPage := swag.FormatInt64(qrPage)
		if qPage != "" {
			if err := r.SetQueryParam("page", qPage); err != nil {
				return err
			}
		}

	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
