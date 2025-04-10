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

package vectorization

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

// NewVectorizationCancelParams creates a new VectorizationCancelParams object,
// with the default timeout for this client.
//
// Default values are not hydrated, since defaults are normally applied by the API server side.
//
// To enforce default values in parameter, use SetDefaults or WithDefaults.
func NewVectorizationCancelParams() *VectorizationCancelParams {
	return &VectorizationCancelParams{
		timeout: cr.DefaultTimeout,
	}
}

// NewVectorizationCancelParamsWithTimeout creates a new VectorizationCancelParams object
// with the ability to set a timeout on a request.
func NewVectorizationCancelParamsWithTimeout(timeout time.Duration) *VectorizationCancelParams {
	return &VectorizationCancelParams{
		timeout: timeout,
	}
}

// NewVectorizationCancelParamsWithContext creates a new VectorizationCancelParams object
// with the ability to set a context for a request.
func NewVectorizationCancelParamsWithContext(ctx context.Context) *VectorizationCancelParams {
	return &VectorizationCancelParams{
		Context: ctx,
	}
}

// NewVectorizationCancelParamsWithHTTPClient creates a new VectorizationCancelParams object
// with the ability to set a custom HTTPClient for a request.
func NewVectorizationCancelParamsWithHTTPClient(client *http.Client) *VectorizationCancelParams {
	return &VectorizationCancelParams{
		HTTPClient: client,
	}
}

/*
VectorizationCancelParams contains all the parameters to send to the API endpoint

	for the vectorization cancel operation.

	Typically these are written to a http.Request.
*/
type VectorizationCancelParams struct {

	// CollectionName.
	CollectionName string

	// TargetVector.
	TargetVector string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithDefaults hydrates default values in the vectorization cancel params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *VectorizationCancelParams) WithDefaults() *VectorizationCancelParams {
	o.SetDefaults()
	return o
}

// SetDefaults hydrates default values in the vectorization cancel params (not the query body).
//
// All values with no default are reset to their zero value.
func (o *VectorizationCancelParams) SetDefaults() {
	// no default values defined for this parameter
}

// WithTimeout adds the timeout to the vectorization cancel params
func (o *VectorizationCancelParams) WithTimeout(timeout time.Duration) *VectorizationCancelParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the vectorization cancel params
func (o *VectorizationCancelParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the vectorization cancel params
func (o *VectorizationCancelParams) WithContext(ctx context.Context) *VectorizationCancelParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the vectorization cancel params
func (o *VectorizationCancelParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the vectorization cancel params
func (o *VectorizationCancelParams) WithHTTPClient(client *http.Client) *VectorizationCancelParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the vectorization cancel params
func (o *VectorizationCancelParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithCollectionName adds the collectionName to the vectorization cancel params
func (o *VectorizationCancelParams) WithCollectionName(collectionName string) *VectorizationCancelParams {
	o.SetCollectionName(collectionName)
	return o
}

// SetCollectionName adds the collectionName to the vectorization cancel params
func (o *VectorizationCancelParams) SetCollectionName(collectionName string) {
	o.CollectionName = collectionName
}

// WithTargetVector adds the targetVector to the vectorization cancel params
func (o *VectorizationCancelParams) WithTargetVector(targetVector string) *VectorizationCancelParams {
	o.SetTargetVector(targetVector)
	return o
}

// SetTargetVector adds the targetVector to the vectorization cancel params
func (o *VectorizationCancelParams) SetTargetVector(targetVector string) {
	o.TargetVector = targetVector
}

// WriteToRequest writes these params to a swagger request
func (o *VectorizationCancelParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param collectionName
	if err := r.SetPathParam("collectionName", o.CollectionName); err != nil {
		return err
	}

	// path param targetVector
	if err := r.SetPathParam("targetVector", o.TargetVector); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
