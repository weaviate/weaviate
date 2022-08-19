// Code generated by go-swagger; DO NOT EDIT.

package well_known

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
)

// New creates a new well known API client.
func New(transport runtime.ClientTransport, formats strfmt.Registry) ClientService {
	return &Client{transport: transport, formats: formats}
}

/*
Client for well known API
*/
type Client struct {
	transport runtime.ClientTransport
	formats   strfmt.Registry
}

// ClientService is the interface for Client methods
type ClientService interface {
	GetWellKnownOpenidConfiguration(params *GetWellKnownOpenidConfigurationParams, authInfo runtime.ClientAuthInfoWriter) (*GetWellKnownOpenidConfigurationOK, error)

	SetTransport(transport runtime.ClientTransport)
}

/*
  GetWellKnownOpenidConfiguration os ID c discovery information if o ID c auth is enabled

  OIDC Discovery page, redirects to the token issuer if one is configured
*/
func (a *Client) GetWellKnownOpenidConfiguration(params *GetWellKnownOpenidConfigurationParams, authInfo runtime.ClientAuthInfoWriter) (*GetWellKnownOpenidConfigurationOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetWellKnownOpenidConfigurationParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "GetWellKnownOpenidConfiguration",
		Method:             "GET",
		PathPattern:        "/.well-known/openid-configuration",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetWellKnownOpenidConfigurationReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*GetWellKnownOpenidConfigurationOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for GetWellKnownOpenidConfiguration: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

// SetTransport changes the transport on the client
func (a *Client) SetTransport(transport runtime.ClientTransport) {
	a.transport = transport
}
