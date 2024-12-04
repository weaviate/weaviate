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

package authz

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
)

// New creates a new authz API client.
func New(transport runtime.ClientTransport, formats strfmt.Registry) ClientService {
	return &Client{transport: transport, formats: formats}
}

/*
Client for authz API
*/
type Client struct {
	transport runtime.ClientTransport
	formats   strfmt.Registry
}

// ClientOption is the option for Client methods
type ClientOption func(*runtime.ClientOperation)

// ClientService is the interface for Client methods
type ClientService interface {
	AddPermissions(params *AddPermissionsParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AddPermissionsOK, error)

	AssignRole(params *AssignRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AssignRoleOK, error)

	CreateRole(params *CreateRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*CreateRoleCreated, error)

	DeleteRole(params *DeleteRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*DeleteRoleNoContent, error)

	GetRole(params *GetRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRoleOK, error)

	GetRoles(params *GetRolesParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRolesOK, error)

	GetRolesForOwnUser(params *GetRolesForOwnUserParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRolesForOwnUserOK, error)

	GetRolesForUser(params *GetRolesForUserParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRolesForUserOK, error)

	GetUsersForRole(params *GetUsersForRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetUsersForRoleOK, error)

	HasPermission(params *HasPermissionParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*HasPermissionOK, error)

	RemovePermissions(params *RemovePermissionsParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*RemovePermissionsOK, error)

	RevokeRole(params *RevokeRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*RevokeRoleOK, error)

	SetTransport(transport runtime.ClientTransport)
}

/*
AddPermissions adds permission to a role as an upsert if the role doesn t exist then it will be created
*/
func (a *Client) AddPermissions(params *AddPermissionsParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AddPermissionsOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewAddPermissionsParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "addPermissions",
		Method:             "POST",
		PathPattern:        "/authz/roles/add-permissions",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &AddPermissionsReader{formats: a.formats},
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
	success, ok := result.(*AddPermissionsOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for addPermissions: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
AssignRole assigns a role to a user
*/
func (a *Client) AssignRole(params *AssignRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*AssignRoleOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewAssignRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "assignRole",
		Method:             "POST",
		PathPattern:        "/authz/users/{id}/assign",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &AssignRoleReader{formats: a.formats},
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
	success, ok := result.(*AssignRoleOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for assignRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
CreateRole creates new role
*/
func (a *Client) CreateRole(params *CreateRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*CreateRoleCreated, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewCreateRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "createRole",
		Method:             "POST",
		PathPattern:        "/authz/roles",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &CreateRoleReader{formats: a.formats},
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
	success, ok := result.(*CreateRoleCreated)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for createRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
DeleteRole deletes role
*/
func (a *Client) DeleteRole(params *DeleteRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*DeleteRoleNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewDeleteRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "deleteRole",
		Method:             "DELETE",
		PathPattern:        "/authz/roles/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &DeleteRoleReader{formats: a.formats},
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
	success, ok := result.(*DeleteRoleNoContent)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for deleteRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
GetRole gets a role
*/
func (a *Client) GetRole(params *GetRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRoleOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "getRole",
		Method:             "GET",
		PathPattern:        "/authz/roles/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetRoleReader{formats: a.formats},
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
	success, ok := result.(*GetRoleOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for getRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
GetRoles gets all roles
*/
func (a *Client) GetRoles(params *GetRolesParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRolesOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetRolesParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "getRoles",
		Method:             "GET",
		PathPattern:        "/authz/roles",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetRolesReader{formats: a.formats},
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
	success, ok := result.(*GetRolesOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for getRoles: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
GetRolesForOwnUser gets roles assigned to own user
*/
func (a *Client) GetRolesForOwnUser(params *GetRolesForOwnUserParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRolesForOwnUserOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetRolesForOwnUserParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "getRolesForOwnUser",
		Method:             "GET",
		PathPattern:        "/authz/users/own-roles",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetRolesForOwnUserReader{formats: a.formats},
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
	success, ok := result.(*GetRolesForOwnUserOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for getRolesForOwnUser: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
GetRolesForUser gets roles assigned to user
*/
func (a *Client) GetRolesForUser(params *GetRolesForUserParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetRolesForUserOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetRolesForUserParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "getRolesForUser",
		Method:             "GET",
		PathPattern:        "/authz/users/{id}/roles",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetRolesForUserReader{formats: a.formats},
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
	success, ok := result.(*GetRolesForUserOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for getRolesForUser: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
GetUsersForRole gets users or a keys assigned to role
*/
func (a *Client) GetUsersForRole(params *GetUsersForRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*GetUsersForRoleOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewGetUsersForRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "getUsersForRole",
		Method:             "GET",
		PathPattern:        "/authz/roles/{id}/users",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &GetUsersForRoleReader{formats: a.formats},
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
	success, ok := result.(*GetUsersForRoleOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for getUsersForRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
HasPermission checks whether role possesses this permission
*/
func (a *Client) HasPermission(params *HasPermissionParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*HasPermissionOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewHasPermissionParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "hasPermission",
		Method:             "POST",
		PathPattern:        "/authz/roles/{id}/has-permission",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &HasPermissionReader{formats: a.formats},
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
	success, ok := result.(*HasPermissionOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for hasPermission: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
RemovePermissions removes permissions from a role if this results in an empty role the role will be deleted
*/
func (a *Client) RemovePermissions(params *RemovePermissionsParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*RemovePermissionsOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewRemovePermissionsParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "removePermissions",
		Method:             "POST",
		PathPattern:        "/authz/roles/remove-permissions",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &RemovePermissionsReader{formats: a.formats},
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
	success, ok := result.(*RemovePermissionsOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for removePermissions: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
RevokeRole revokes a role from a user
*/
func (a *Client) RevokeRole(params *RevokeRoleParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*RevokeRoleOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewRevokeRoleParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "revokeRole",
		Method:             "POST",
		PathPattern:        "/authz/users/{id}/revoke",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &RevokeRoleReader{formats: a.formats},
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
	success, ok := result.(*RevokeRoleOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for revokeRole: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

// SetTransport changes the transport on the client
func (a *Client) SetTransport(transport runtime.ClientTransport) {
	a.transport = transport
}
