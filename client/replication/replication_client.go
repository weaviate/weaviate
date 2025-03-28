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

package replication

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
)

// New creates a new replication API client.
func New(transport runtime.ClientTransport, formats strfmt.Registry) ClientService {
	return &Client{transport: transport, formats: formats}
}

/*
Client for replication API
*/
type Client struct {
	transport runtime.ClientTransport
	formats   strfmt.Registry
}

// ClientOption is the option for Client methods
type ClientOption func(*runtime.ClientOperation)

// ClientService is the interface for Client methods
type ClientService interface {
	Replicate(params *ReplicateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*ReplicateOK, error)

	ReplicateStatus(params *ReplicateStatusParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*ReplicateStatusOK, error)

	SetTransport(transport runtime.ClientTransport)
}

/*
Replicate starts the async operation to replicate a replica between two nodes
*/
func (a *Client) Replicate(params *ReplicateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*ReplicateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewReplicateParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "replicate",
		Method:             "POST",
		PathPattern:        "/replication/replicate",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ReplicateReader{formats: a.formats},
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
	success, ok := result.(*ReplicateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for replicate: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ReplicateStatus gets the status of a replication operation

Returns the status of a replication operation for a given shard, identified by the provided id.
*/
func (a *Client) ReplicateStatus(params *ReplicateStatusParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*ReplicateStatusOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewReplicateStatusParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "replicateStatus",
		Method:             "GET",
		PathPattern:        "/replication/replicate/{id}/status",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ReplicateStatusReader{formats: a.formats},
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
	success, ok := result.(*ReplicateStatusOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for replicateStatus: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

// SetTransport changes the transport on the client
func (a *Client) SetTransport(transport runtime.ClientTransport) {
	a.transport = transport
}
