//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Code generated by go-swagger; DO NOT EDIT.

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
)

// New creates a new objects API client.
func New(transport runtime.ClientTransport, formats strfmt.Registry) ClientService {
	return &Client{transport: transport, formats: formats}
}

/*
Client for objects API
*/
type Client struct {
	transport runtime.ClientTransport
	formats   strfmt.Registry
}

// ClientService is the interface for Client methods
type ClientService interface {
	ObjectsClassDelete(params *ObjectsClassDeleteParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassDeleteNoContent, error)

	ObjectsClassGet(params *ObjectsClassGetParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassGetOK, error)

	ObjectsClassHead(params *ObjectsClassHeadParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassHeadNoContent, error)

	ObjectsClassPatch(params *ObjectsClassPatchParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassPatchNoContent, error)

	ObjectsClassPut(params *ObjectsClassPutParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassPutOK, error)

	ObjectsClassReferencesCreate(params *ObjectsClassReferencesCreateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassReferencesCreateOK, error)

	ObjectsClassReferencesDelete(params *ObjectsClassReferencesDeleteParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassReferencesDeleteNoContent, error)

	ObjectsClassReferencesPut(params *ObjectsClassReferencesPutParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassReferencesPutOK, error)

	ObjectsCreate(params *ObjectsCreateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsCreateOK, error)

	ObjectsDelete(params *ObjectsDeleteParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsDeleteNoContent, error)

	ObjectsGet(params *ObjectsGetParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsGetOK, error)

	ObjectsHead(params *ObjectsHeadParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsHeadNoContent, error)

	ObjectsList(params *ObjectsListParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsListOK, error)

	ObjectsPatch(params *ObjectsPatchParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsPatchNoContent, error)

	ObjectsReferencesCreate(params *ObjectsReferencesCreateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsReferencesCreateOK, error)

	ObjectsReferencesDelete(params *ObjectsReferencesDeleteParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsReferencesDeleteNoContent, error)

	ObjectsReferencesUpdate(params *ObjectsReferencesUpdateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsReferencesUpdateOK, error)

	ObjectsUpdate(params *ObjectsUpdateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsUpdateOK, error)

	ObjectsValidate(params *ObjectsValidateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsValidateOK, error)

	SetTransport(transport runtime.ClientTransport)
}

/*
ObjectsClassDelete deletes object based on its class and UUID

Delete a single data object.
*/
func (a *Client) ObjectsClassDelete(params *ObjectsClassDeleteParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassDeleteNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsClassDeleteParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.class.delete",
		Method:             "DELETE",
		PathPattern:        "/objects/{className}/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsClassDeleteReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsClassDeleteNoContent)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.class.delete: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsClassGet gets a specific object based on its class and UUID also available as websocket bus

Get a single data object
*/
func (a *Client) ObjectsClassGet(params *ObjectsClassGetParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassGetOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsClassGetParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.class.get",
		Method:             "GET",
		PathPattern:        "/objects/{className}/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsClassGetReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsClassGetOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.class.get: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsClassHead checks object s existence based on its class and uuid

Checks if a data object exists without retrieving it.
*/
func (a *Client) ObjectsClassHead(params *ObjectsClassHeadParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassHeadNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsClassHeadParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.class.head",
		Method:             "HEAD",
		PathPattern:        "/objects/{className}/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsClassHeadReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsClassHeadNoContent)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.class.head: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsClassPatch updates an object based on its UUID using patch semantics

Update an individual data object based on its class and uuid. This method supports json-merge style patch semantics (RFC 7396). Provided meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.
*/
func (a *Client) ObjectsClassPatch(params *ObjectsClassPatchParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassPatchNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsClassPatchParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.class.patch",
		Method:             "PATCH",
		PathPattern:        "/objects/{className}/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsClassPatchReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsClassPatchNoContent)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.class.patch: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsClassPut updates a class object based on its uuid

Update an individual data object based on its class and uuid.
*/
func (a *Client) ObjectsClassPut(params *ObjectsClassPutParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassPutOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsClassPutParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.class.put",
		Method:             "PUT",
		PathPattern:        "/objects/{className}/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsClassPutReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsClassPutOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.class.put: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsClassReferencesCreate adds a single reference to a class property

Add a single reference to a class-property.
*/
func (a *Client) ObjectsClassReferencesCreate(params *ObjectsClassReferencesCreateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassReferencesCreateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsClassReferencesCreateParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.class.references.create",
		Method:             "POST",
		PathPattern:        "/objects/{className}/{id}/references/{propertyName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsClassReferencesCreateReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsClassReferencesCreateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.class.references.create: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsClassReferencesDelete deletes the single reference that is given in the body from the list of references that this property has

Delete the single reference that is given in the body from the list of references that this property of a data object has
*/
func (a *Client) ObjectsClassReferencesDelete(params *ObjectsClassReferencesDeleteParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassReferencesDeleteNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsClassReferencesDeleteParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.class.references.delete",
		Method:             "DELETE",
		PathPattern:        "/objects/{className}/{id}/references/{propertyName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsClassReferencesDeleteReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsClassReferencesDeleteNoContent)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.class.references.delete: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsClassReferencesPut replaces all references to a class property

Update all references of a property of a data object.
*/
func (a *Client) ObjectsClassReferencesPut(params *ObjectsClassReferencesPutParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsClassReferencesPutOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsClassReferencesPutParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.class.references.put",
		Method:             "PUT",
		PathPattern:        "/objects/{className}/{id}/references/{propertyName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsClassReferencesPutReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsClassReferencesPutOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.class.references.put: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsCreate creates objects between two objects object and subject

Registers a new Object. Provided meta-data and schema values are validated.
*/
func (a *Client) ObjectsCreate(params *ObjectsCreateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsCreateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsCreateParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.create",
		Method:             "POST",
		PathPattern:        "/objects",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsCreateReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsCreateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.create: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsDelete deletes an object based on its UUID

Deletes an Object from the system.
*/
func (a *Client) ObjectsDelete(params *ObjectsDeleteParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsDeleteNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsDeleteParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.delete",
		Method:             "DELETE",
		PathPattern:        "/objects/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsDeleteReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsDeleteNoContent)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.delete: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsGet gets a specific object based on its UUID and a object UUID also available as websocket bus

Lists Objects.
*/
func (a *Client) ObjectsGet(params *ObjectsGetParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsGetOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsGetParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.get",
		Method:             "GET",
		PathPattern:        "/objects/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsGetReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsGetOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.get: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsHead checks object s existence based on its UUID

Checks if an Object exists in the system.
*/
func (a *Client) ObjectsHead(params *ObjectsHeadParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsHeadNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsHeadParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.head",
		Method:             "HEAD",
		PathPattern:        "/objects/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsHeadReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsHeadNoContent)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.head: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsList gets a list of objects

Lists all Objects in reverse order of creation, owned by the user that belongs to the used token.
*/
func (a *Client) ObjectsList(params *ObjectsListParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsListOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsListParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.list",
		Method:             "GET",
		PathPattern:        "/objects",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsListReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsListOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.list: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsPatch updates an object based on its UUID using patch semantics

Updates an Object. This method supports json-merge style patch semantics (RFC 7396). Provided meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.
*/
func (a *Client) ObjectsPatch(params *ObjectsPatchParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsPatchNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsPatchParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.patch",
		Method:             "PATCH",
		PathPattern:        "/objects/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsPatchReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsPatchNoContent)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.patch: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsReferencesCreate adds a single reference to a class property

Add a single reference to a class-property.
*/
func (a *Client) ObjectsReferencesCreate(params *ObjectsReferencesCreateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsReferencesCreateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsReferencesCreateParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.references.create",
		Method:             "POST",
		PathPattern:        "/objects/{id}/references/{propertyName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsReferencesCreateReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsReferencesCreateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.references.create: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsReferencesDelete deletes the single reference that is given in the body from the list of references that this property has

Delete the single reference that is given in the body from the list of references that this property has.
*/
func (a *Client) ObjectsReferencesDelete(params *ObjectsReferencesDeleteParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsReferencesDeleteNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsReferencesDeleteParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.references.delete",
		Method:             "DELETE",
		PathPattern:        "/objects/{id}/references/{propertyName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsReferencesDeleteReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsReferencesDeleteNoContent)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.references.delete: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsReferencesUpdate replaces all references to a class property

Replace all references to a class-property.
*/
func (a *Client) ObjectsReferencesUpdate(params *ObjectsReferencesUpdateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsReferencesUpdateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsReferencesUpdateParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.references.update",
		Method:             "PUT",
		PathPattern:        "/objects/{id}/references/{propertyName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsReferencesUpdateReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsReferencesUpdateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.references.update: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsUpdate updates an object based on its UUID

Updates an Object's data. Given meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.
*/
func (a *Client) ObjectsUpdate(params *ObjectsUpdateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsUpdateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsUpdateParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.update",
		Method:             "PUT",
		PathPattern:        "/objects/{id}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsUpdateReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsUpdateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.update: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

/*
ObjectsValidate validates an object based on a schema

Validate an Object's schema and meta-data. It has to be based on a schema, which is related to the given Object to be accepted by this validation.
*/
func (a *Client) ObjectsValidate(params *ObjectsValidateParams, authInfo runtime.ClientAuthInfoWriter) (*ObjectsValidateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewObjectsValidateParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "objects.validate",
		Method:             "POST",
		PathPattern:        "/objects/validate",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json", "application/yaml"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &ObjectsValidateReader{formats: a.formats},
		AuthInfo:           authInfo,
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	success, ok := result.(*ObjectsValidateOK)
	if ok {
		return success, nil
	}
	// unexpected success response
	// safeguard: normally, absent a default response, unknown success responses return an error above: so this is a codegen issue
	msg := fmt.Sprintf("unexpected success response for objects.validate: API contract not enforced by server. Client expected to get an error, but got: %T", result)
	panic(msg)
}

// SetTransport changes the transport on the client
func (a *Client) SetTransport(transport runtime.ClientTransport) {
	a.transport = transport
}
