// Code generated by go-swagger; DO NOT EDIT.

package schema

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
)

// New creates a new schema API client.
func New(transport runtime.ClientTransport, formats strfmt.Registry) ClientService {
	return &Client{transport: transport, formats: formats}
}

/*
Client for schema API
*/
type Client struct {
	transport runtime.ClientTransport
	formats   strfmt.Registry
}

// ClientOption is the option for Client methods
type ClientOption func(*runtime.ClientOperation)

// ClientService is the interface for Client methods
type ClientService interface {
	SchemaDump(params *SchemaDumpParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaDumpOK, error)

	SchemaObjectsCreate(params *SchemaObjectsCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsCreateOK, error)

	SchemaObjectsDelete(params *SchemaObjectsDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsDeleteOK, error)

	SchemaObjectsGet(params *SchemaObjectsGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsGetOK, error)

	SchemaObjectsPropertiesAdd(params *SchemaObjectsPropertiesAddParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsPropertiesAddOK, error)

	SchemaObjectsShardsGet(params *SchemaObjectsShardsGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsShardsGetOK, error)

	SchemaObjectsShardsUpdate(params *SchemaObjectsShardsUpdateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsShardsUpdateOK, error)

	SchemaObjectsUpdate(params *SchemaObjectsUpdateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsUpdateOK, error)

	TenantExists(params *TenantExistsParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantExistsOK, error)

	TenantsCreate(params *TenantsCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantsCreateOK, error)

	TenantsDelete(params *TenantsDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantsDeleteOK, error)

	TenantsGet(params *TenantsGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantsGetOK, error)

	TenantsGetOne(params *TenantsGetOneParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantsGetOneOK, error)

	TenantsUpdate(params *TenantsUpdateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantsUpdateOK, error)

	SetTransport(transport runtime.ClientTransport)
}

/*
SchemaDump dumps the current the database schema

Fetch an array of all collection definitions from the schema.
*/
func (a *Client) SchemaDump(params *SchemaDumpParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaDumpOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewSchemaDumpParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "schema.dump",
		Method:             "GET",
		PathPattern:        "/schema",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &SchemaDumpReader{formats: a.formats},
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
	success, ok := result.(*SchemaDumpOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for schema.dump: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
SchemaObjectsCreate creates a new object class in the schema

Create a new data object collection. <br/><br/>If AutoSchema is enabled, Weaviate will attempt to infer the schema from the data at import time. However, manual schema definition is recommended for production environments.
*/
func (a *Client) SchemaObjectsCreate(params *SchemaObjectsCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsCreateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewSchemaObjectsCreateParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "schema.objects.create",
		Method:             "POST",
		PathPattern:        "/schema",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &SchemaObjectsCreateReader{formats: a.formats},
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
	success, ok := result.(*SchemaObjectsCreateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for schema.objects.create: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
SchemaObjectsDelete removes an object class and all data in the instances from the schema

Remove a collection from the schema. This will also delete all the objects in the collection.
*/
func (a *Client) SchemaObjectsDelete(params *SchemaObjectsDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsDeleteOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewSchemaObjectsDeleteParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "schema.objects.delete",
		Method:             "DELETE",
		PathPattern:        "/schema/{className}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &SchemaObjectsDeleteReader{formats: a.formats},
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
	success, ok := result.(*SchemaObjectsDeleteOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for schema.objects.delete: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
SchemaObjectsGet gets a single class from the schema
*/
func (a *Client) SchemaObjectsGet(params *SchemaObjectsGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsGetOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewSchemaObjectsGetParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "schema.objects.get",
		Method:             "GET",
		PathPattern:        "/schema/{className}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &SchemaObjectsGetReader{formats: a.formats},
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
	success, ok := result.(*SchemaObjectsGetOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for schema.objects.get: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
SchemaObjectsPropertiesAdd adds a property to an object class
*/
func (a *Client) SchemaObjectsPropertiesAdd(params *SchemaObjectsPropertiesAddParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsPropertiesAddOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewSchemaObjectsPropertiesAddParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "schema.objects.properties.add",
		Method:             "POST",
		PathPattern:        "/schema/{className}/properties",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &SchemaObjectsPropertiesAddReader{formats: a.formats},
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
	success, ok := result.(*SchemaObjectsPropertiesAddOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for schema.objects.properties.add: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
SchemaObjectsShardsGet gets the shards status of an object class

Get the status of every shard in the cluster.
*/
func (a *Client) SchemaObjectsShardsGet(params *SchemaObjectsShardsGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsShardsGetOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewSchemaObjectsShardsGetParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "schema.objects.shards.get",
		Method:             "GET",
		PathPattern:        "/schema/{className}/shards",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &SchemaObjectsShardsGetReader{formats: a.formats},
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
	success, ok := result.(*SchemaObjectsShardsGetOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for schema.objects.shards.get: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
SchemaObjectsShardsUpdate updates a shard status

Update a shard status for a collection. For example, a shard may have been marked as `READONLY` because its disk was full. After providing more disk space, use this endpoint to set the shard status to `READY` again. There is also a convenience function in each client to set the status of all shards of a collection.
*/
func (a *Client) SchemaObjectsShardsUpdate(params *SchemaObjectsShardsUpdateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsShardsUpdateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewSchemaObjectsShardsUpdateParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "schema.objects.shards.update",
		Method:             "PUT",
		PathPattern:        "/schema/{className}/shards/{shardName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &SchemaObjectsShardsUpdateReader{formats: a.formats},
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
	success, ok := result.(*SchemaObjectsShardsUpdateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for schema.objects.shards.update: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
SchemaObjectsUpdate updates settings of an existing schema class

Add a property to an existing collection.
*/
func (a *Client) SchemaObjectsUpdate(params *SchemaObjectsUpdateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*SchemaObjectsUpdateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewSchemaObjectsUpdateParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "schema.objects.update",
		Method:             "PUT",
		PathPattern:        "/schema/{className}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &SchemaObjectsUpdateReader{formats: a.formats},
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
	success, ok := result.(*SchemaObjectsUpdateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for schema.objects.update: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
TenantExists checks whether a tenant exists

Check if a tenant exists for a specific class
*/
func (a *Client) TenantExists(params *TenantExistsParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantExistsOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewTenantExistsParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "tenant.exists",
		Method:             "HEAD",
		PathPattern:        "/schema/{className}/tenants/{tenantName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &TenantExistsReader{formats: a.formats},
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
	success, ok := result.(*TenantExistsOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for tenant.exists: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
TenantsCreate creates a new tenant

Create a new tenant for a collection. Multi-tenancy must be enabled in the collection definition.
*/
func (a *Client) TenantsCreate(params *TenantsCreateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantsCreateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewTenantsCreateParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "tenants.create",
		Method:             "POST",
		PathPattern:        "/schema/{className}/tenants",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &TenantsCreateReader{formats: a.formats},
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
	success, ok := result.(*TenantsCreateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for tenants.create: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
TenantsDelete delete tenants from a specific class
*/
func (a *Client) TenantsDelete(params *TenantsDeleteParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantsDeleteOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewTenantsDeleteParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "tenants.delete",
		Method:             "DELETE",
		PathPattern:        "/schema/{className}/tenants",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &TenantsDeleteReader{formats: a.formats},
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
	success, ok := result.(*TenantsDeleteOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for tenants.delete: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
TenantsGet gets the list of tenants

get all tenants from a specific class
*/
func (a *Client) TenantsGet(params *TenantsGetParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantsGetOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewTenantsGetParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "tenants.get",
		Method:             "GET",
		PathPattern:        "/schema/{className}/tenants",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &TenantsGetReader{formats: a.formats},
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
	success, ok := result.(*TenantsGetOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for tenants.get: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
TenantsGetOne gets a specific tenant

get a specific tenant for the given class
*/
func (a *Client) TenantsGetOne(params *TenantsGetOneParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantsGetOneOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewTenantsGetOneParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "tenants.get.one",
		Method:             "GET",
		PathPattern:        "/schema/{className}/tenants/{tenantName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &TenantsGetOneReader{formats: a.formats},
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
	success, ok := result.(*TenantsGetOneOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for tenants.get.one: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
TenantsUpdate updates a tenant

Update tenant of a specific class
*/
func (a *Client) TenantsUpdate(params *TenantsUpdateParams, authInfo runtime.ClientAuthInfoWriter, opts ...ClientOption) (*TenantsUpdateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewTenantsUpdateParams()
	}
	op := &runtime.ClientOperation{
		ID:                 "tenants.update",
		Method:             "PUT",
		PathPattern:        "/schema/{className}/tenants",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &TenantsUpdateReader{formats: a.formats},
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
	success, ok := result.(*TenantsUpdateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for tenants.update: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

// SetTransport changes the transport on the client
func (a *Client) SetTransport(transport runtime.ClientTransport) {
	a.transport = transport
}
