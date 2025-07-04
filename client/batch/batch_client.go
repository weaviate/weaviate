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

package batch

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
)

// New creates a new batch API client.
func New(transport runtime.ClientTransport, formats strfmt.Registry) ClientService {
	return &Client{transport: transport, formats: formats}
}

/*
Client for batch API
*/
type Client struct {
	transport runtime.ClientTransport
	formats   strfmt.Registry
}

// ClientOption is the option for Client methods
type ClientOption func(*runtime.ClientOperation)

// ClientService is the interface for Client methods
type ClientService interface {
	BatchObjectsCreate(params *BatchObjectsCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*BatchObjectsCreateOK, error)

	BatchObjectsDelete(params *BatchObjectsDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*BatchObjectsDeleteOK, error)

	BatchReferencesCreate(params *BatchReferencesCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*BatchReferencesCreateOK, error)

	SetTransport(transport runtime.ClientTransport)
}

/*
BatchObjectsCreate creates new objects based on a object template as a batch

Create new objects in bulk. <br/><br/>Meta-data and schema values are validated. <br/><br/>**Note: idempotence of `/batch/objects`**: <br/>`POST /batch/objects` is idempotent, and will overwrite any existing object given the same id.
*/
func (a *Client) BatchObjectsCreate(params *BatchObjectsCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*BatchObjectsCreateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewBatchObjectsCreateParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "batch.objects.create",
		Method:             "POST",
		PathPattern:        "/batch/objects",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &BatchObjectsCreateReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*BatchObjectsCreateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for batch.objects.create: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
BatchObjectsDelete deletes objects based on a match filter as a batch

Batch delete objects that match a particular filter. <br/><br/>The request body takes a single `where` filter and will delete all objects matched. <br/><br/>Note that there is a limit to the number of objects to be deleted at once using this filter, in order to protect against unexpected memory surges and very-long-running requests. The default limit is 10,000 and may be configured by setting the `QUERY_MAXIMUM_RESULTS` environment variable. <br/><br/>Objects are deleted in the same order that they would be returned in an equivalent Get query. To delete more objects than the limit, run the same query multiple times.
*/
func (a *Client) BatchObjectsDelete(params *BatchObjectsDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*BatchObjectsDeleteOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewBatchObjectsDeleteParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "batch.objects.delete",
		Method:             "DELETE",
		PathPattern:        "/batch/objects",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &BatchObjectsDeleteReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*BatchObjectsDeleteOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for batch.objects.delete: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
BatchReferencesCreate creates new cross references between arbitrary classes in bulk

Batch create cross-references between collections items (objects or objects) in bulk.
*/
func (a *Client) BatchReferencesCreate(params *BatchReferencesCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*BatchReferencesCreateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewBatchReferencesCreateParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "batch.references.create",
		Method:             "POST",
		PathPattern:        "/batch/references",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &BatchReferencesCreateReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	}
	for _, opt := range opts {
		opt(op)
	}

	result, err := a.transport.Submit(op)
	if err != nil {
		return nil, err
	}
	success, ok := result.(*BatchReferencesCreateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for batch.references.create: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

// SetTransport changes the transport on the client
func (a *Client) SetTransport(transport runtime.ClientTransport) {
	a.transport = transport
}
