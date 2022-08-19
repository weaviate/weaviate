// Code generated by go-swagger; DO NOT EDIT.

package classifications

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

	"github.com/semi-technologies/weaviate/entities/models"
)

// NewClassificationsPostParams creates a new ClassificationsPostParams object
// with the default values initialized.
func NewClassificationsPostParams() *ClassificationsPostParams {
	var ()
	return &ClassificationsPostParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewClassificationsPostParamsWithTimeout creates a new ClassificationsPostParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewClassificationsPostParamsWithTimeout(timeout time.Duration) *ClassificationsPostParams {
	var ()
	return &ClassificationsPostParams{

		timeout: timeout,
	}
}

// NewClassificationsPostParamsWithContext creates a new ClassificationsPostParams object
// with the default values initialized, and the ability to set a context for a request
func NewClassificationsPostParamsWithContext(ctx context.Context) *ClassificationsPostParams {
	var ()
	return &ClassificationsPostParams{

		Context: ctx,
	}
}

// NewClassificationsPostParamsWithHTTPClient creates a new ClassificationsPostParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewClassificationsPostParamsWithHTTPClient(client *http.Client) *ClassificationsPostParams {
	var ()
	return &ClassificationsPostParams{
		HTTPClient: client,
	}
}

/*ClassificationsPostParams contains all the parameters to send to the API endpoint
for the classifications post operation typically these are written to a http.Request
*/
type ClassificationsPostParams struct {

	/*Params
	  parameters to start a classification

	*/
	Params *models.Classification

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the classifications post params
func (o *ClassificationsPostParams) WithTimeout(timeout time.Duration) *ClassificationsPostParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the classifications post params
func (o *ClassificationsPostParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the classifications post params
func (o *ClassificationsPostParams) WithContext(ctx context.Context) *ClassificationsPostParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the classifications post params
func (o *ClassificationsPostParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the classifications post params
func (o *ClassificationsPostParams) WithHTTPClient(client *http.Client) *ClassificationsPostParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the classifications post params
func (o *ClassificationsPostParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithParams adds the params to the classifications post params
func (o *ClassificationsPostParams) WithParams(params *models.Classification) *ClassificationsPostParams {
	o.SetParams(params)
	return o
}

// SetParams adds the params to the classifications post params
func (o *ClassificationsPostParams) SetParams(params *models.Classification) {
	o.Params = params
}

// WriteToRequest writes these params to a swagger request
func (o *ClassificationsPostParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if o.Params != nil {
		if err := r.SetBodyParam(o.Params); err != nil {
			return err
		}
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
