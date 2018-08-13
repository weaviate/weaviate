// Code generated by go-swagger; DO NOT EDIT.

package meta

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
)

// NewWeaviateMetaGetParams creates a new WeaviateMetaGetParams object
// with the default values initialized.
func NewWeaviateMetaGetParams() *WeaviateMetaGetParams {

	return &WeaviateMetaGetParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewWeaviateMetaGetParamsWithTimeout creates a new WeaviateMetaGetParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewWeaviateMetaGetParamsWithTimeout(timeout time.Duration) *WeaviateMetaGetParams {

	return &WeaviateMetaGetParams{

		timeout: timeout,
	}
}

// NewWeaviateMetaGetParamsWithContext creates a new WeaviateMetaGetParams object
// with the default values initialized, and the ability to set a context for a request
func NewWeaviateMetaGetParamsWithContext(ctx context.Context) *WeaviateMetaGetParams {

	return &WeaviateMetaGetParams{

		Context: ctx,
	}
}

// NewWeaviateMetaGetParamsWithHTTPClient creates a new WeaviateMetaGetParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewWeaviateMetaGetParamsWithHTTPClient(client *http.Client) *WeaviateMetaGetParams {

	return &WeaviateMetaGetParams{
		HTTPClient: client,
	}
}

/*WeaviateMetaGetParams contains all the parameters to send to the API endpoint
for the weaviate meta get operation typically these are written to a http.Request
*/
type WeaviateMetaGetParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the weaviate meta get params
func (o *WeaviateMetaGetParams) WithTimeout(timeout time.Duration) *WeaviateMetaGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the weaviate meta get params
func (o *WeaviateMetaGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the weaviate meta get params
func (o *WeaviateMetaGetParams) WithContext(ctx context.Context) *WeaviateMetaGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the weaviate meta get params
func (o *WeaviateMetaGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the weaviate meta get params
func (o *WeaviateMetaGetParams) WithHTTPClient(client *http.Client) *WeaviateMetaGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the weaviate meta get params
func (o *WeaviateMetaGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *WeaviateMetaGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
