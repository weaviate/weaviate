/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */ // Code generated by go-swagger; DO NOT EDIT.

package things

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"github.com/go-openapi/runtime"

	strfmt "github.com/go-openapi/strfmt"
)

// New creates a new things API client.
func New(transport runtime.ClientTransport, formats strfmt.Registry) *Client {
	return &Client{transport: transport, formats: formats}
}

/*
Client for things API
*/
type Client struct {
	transport runtime.ClientTransport
	formats   strfmt.Registry
}

/*
WeaviateThingHistoryGet gets a thing s history based on its UUID related to this key

Returns a particular Thing's history.
*/
func (a *Client) WeaviateThingHistoryGet(params *WeaviateThingHistoryGetParams) (*WeaviateThingHistoryGetOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewWeaviateThingHistoryGetParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "weaviate.thing.history.get",
		Method:             "GET",
		PathPattern:        "/things/{thingId}/history",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &WeaviateThingHistoryGetReader{formats: a.formats},
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	return result.(*WeaviateThingHistoryGetOK), nil

}

/*
WeaviateThingsCreate creates a new thing based on a thing template related to this key

Registers a new Thing. Given meta-data and schema values are validated.
*/
func (a *Client) WeaviateThingsCreate(params *WeaviateThingsCreateParams) (*WeaviateThingsCreateOK, *WeaviateThingsCreateAccepted, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewWeaviateThingsCreateParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "weaviate.things.create",
		Method:             "POST",
		PathPattern:        "/things",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &WeaviateThingsCreateReader{formats: a.formats},
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, nil, err
	}
	switch value := result.(type) {
	case *WeaviateThingsCreateOK:
		return value, nil, nil
	case *WeaviateThingsCreateAccepted:
		return nil, value, nil
	}
	return nil, nil, nil

}

/*
WeaviateThingsDelete deletes a thing based on its UUID related to this key

Deletes a Thing from the system. All Actions pointing to this Thing, where the Thing is the object of the Action, are also being deleted.
*/
func (a *Client) WeaviateThingsDelete(params *WeaviateThingsDeleteParams) (*WeaviateThingsDeleteNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewWeaviateThingsDeleteParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "weaviate.things.delete",
		Method:             "DELETE",
		PathPattern:        "/things/{thingId}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &WeaviateThingsDeleteReader{formats: a.formats},
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	return result.(*WeaviateThingsDeleteNoContent), nil

}

/*
WeaviateThingsGet gets a thing based on its UUID related to this key

Returns a particular Thing data.
*/
func (a *Client) WeaviateThingsGet(params *WeaviateThingsGetParams) (*WeaviateThingsGetOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewWeaviateThingsGetParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "weaviate.things.get",
		Method:             "GET",
		PathPattern:        "/things/{thingId}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &WeaviateThingsGetReader{formats: a.formats},
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	return result.(*WeaviateThingsGetOK), nil

}

/*
WeaviateThingsList gets a list of things related to this key

Lists all Things in reverse order of creation, owned by the user that belongs to the used token.
*/
func (a *Client) WeaviateThingsList(params *WeaviateThingsListParams) (*WeaviateThingsListOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewWeaviateThingsListParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "weaviate.things.list",
		Method:             "GET",
		PathPattern:        "/things",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &WeaviateThingsListReader{formats: a.formats},
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	return result.(*WeaviateThingsListOK), nil

}

/*
WeaviateThingsPatch updates a thing based on its UUID using patch semantics related to this key

Updates a Thing's data. This method supports patch semantics. Given meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.
*/
func (a *Client) WeaviateThingsPatch(params *WeaviateThingsPatchParams) (*WeaviateThingsPatchOK, *WeaviateThingsPatchAccepted, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewWeaviateThingsPatchParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "weaviate.things.patch",
		Method:             "PATCH",
		PathPattern:        "/things/{thingId}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &WeaviateThingsPatchReader{formats: a.formats},
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, nil, err
	}
	switch value := result.(type) {
	case *WeaviateThingsPatchOK:
		return value, nil, nil
	case *WeaviateThingsPatchAccepted:
		return nil, value, nil
	}
	return nil, nil, nil

}

/*
WeaviateThingsPropertiesCreate adds a single reference to a class property when cardinality is set to has many

Add a single reference to a class-property when cardinality is set to 'hasMany'.
*/
func (a *Client) WeaviateThingsPropertiesCreate(params *WeaviateThingsPropertiesCreateParams) (*WeaviateThingsPropertiesCreateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewWeaviateThingsPropertiesCreateParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "weaviate.things.properties.create",
		Method:             "POST",
		PathPattern:        "/things/{thingId}/properties/{propertyName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &WeaviateThingsPropertiesCreateReader{formats: a.formats},
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	return result.(*WeaviateThingsPropertiesCreateOK), nil

}

/*
WeaviateThingsPropertiesDelete deletes the single reference that is given in the body from the list of references that this property has

Delete the single reference that is given in the body from the list of references that this property has.
*/
func (a *Client) WeaviateThingsPropertiesDelete(params *WeaviateThingsPropertiesDeleteParams) (*WeaviateThingsPropertiesDeleteNoContent, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewWeaviateThingsPropertiesDeleteParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "weaviate.things.properties.delete",
		Method:             "DELETE",
		PathPattern:        "/things/{thingId}/properties/{propertyName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &WeaviateThingsPropertiesDeleteReader{formats: a.formats},
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	return result.(*WeaviateThingsPropertiesDeleteNoContent), nil

}

/*
WeaviateThingsPropertiesUpdate replaces all references to a class property

Replace all references to a class-property.
*/
func (a *Client) WeaviateThingsPropertiesUpdate(params *WeaviateThingsPropertiesUpdateParams) (*WeaviateThingsPropertiesUpdateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewWeaviateThingsPropertiesUpdateParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "weaviate.things.properties.update",
		Method:             "PUT",
		PathPattern:        "/things/{thingId}/properties/{propertyName}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &WeaviateThingsPropertiesUpdateReader{formats: a.formats},
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	return result.(*WeaviateThingsPropertiesUpdateOK), nil

}

/*
WeaviateThingsUpdate updates a thing based on its UUID related to this key

Updates a Thing's data. Given meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.
*/
func (a *Client) WeaviateThingsUpdate(params *WeaviateThingsUpdateParams) (*WeaviateThingsUpdateAccepted, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewWeaviateThingsUpdateParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "weaviate.things.update",
		Method:             "PUT",
		PathPattern:        "/things/{thingId}",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &WeaviateThingsUpdateReader{formats: a.formats},
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	return result.(*WeaviateThingsUpdateAccepted), nil

}

/*
WeaviateThingsValidate validates things schema

Validate a Thing's schema and meta-data. It has to be based on a schema, which is related to the given Thing to be accepted by this validation.
*/
func (a *Client) WeaviateThingsValidate(params *WeaviateThingsValidateParams) (*WeaviateThingsValidateOK, error) {
	// TODO: Validate the params before sending
	if params == nil {
		params = NewWeaviateThingsValidateParams()
	}

	result, err := a.transport.Submit(&runtime.ClientOperation{
		ID:                 "weaviate.things.validate",
		Method:             "POST",
		PathPattern:        "/things/validate",
		ProducesMediaTypes: []string{"application/json"},
		ConsumesMediaTypes: []string{"application/json"},
		Schemes:            []string{"https"},
		Params:             params,
		Reader:             &WeaviateThingsValidateReader{formats: a.formats},
		Context:            params.Context,
		Client:             params.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	return result.(*WeaviateThingsValidateOK), nil

}

// SetTransport changes the transport on the client
func (a *Client) SetTransport(transport runtime.ClientTransport) {
	a.transport = transport
}
