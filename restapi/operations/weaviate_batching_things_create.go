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

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the generate command

import (
	"encoding/json"
	"net/http"
	"strconv"

	errors "github.com/go-openapi/errors"
	middleware "github.com/go-openapi/runtime/middleware"
	strfmt "github.com/go-openapi/strfmt"
	swag "github.com/go-openapi/swag"
	validate "github.com/go-openapi/validate"

	models "github.com/creativesoftwarefdn/weaviate/models"
)

// WeaviateBatchingThingsCreateHandlerFunc turns a function with the right signature into a weaviate batching things create handler
type WeaviateBatchingThingsCreateHandlerFunc func(WeaviateBatchingThingsCreateParams, *models.Principal) middleware.Responder

// Handle executing the request and returning a response
func (fn WeaviateBatchingThingsCreateHandlerFunc) Handle(params WeaviateBatchingThingsCreateParams, principal *models.Principal) middleware.Responder {
	return fn(params, principal)
}

// WeaviateBatchingThingsCreateHandler interface for that can handle valid weaviate batching things create params
type WeaviateBatchingThingsCreateHandler interface {
	Handle(WeaviateBatchingThingsCreateParams, *models.Principal) middleware.Responder
}

// NewWeaviateBatchingThingsCreate creates a new http.Handler for the weaviate batching things create operation
func NewWeaviateBatchingThingsCreate(ctx *middleware.Context, handler WeaviateBatchingThingsCreateHandler) *WeaviateBatchingThingsCreate {
	return &WeaviateBatchingThingsCreate{Context: ctx, Handler: handler}
}

/*WeaviateBatchingThingsCreate swagger:route POST /batching/things batching things weaviateBatchingThingsCreate

Creates new Things based on a Thing template as a batch.

Register new Things in bulk. Provided meta-data and schema values are validated.

*/
type WeaviateBatchingThingsCreate struct {
	Context *middleware.Context
	Handler WeaviateBatchingThingsCreateHandler
}

func (o *WeaviateBatchingThingsCreate) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	route, rCtx, _ := o.Context.RouteInfo(r)
	if rCtx != nil {
		r = rCtx
	}
	var Params = NewWeaviateBatchingThingsCreateParams()

	uprinc, aCtx, err := o.Context.Authorize(r, route)
	if err != nil {
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}
	if aCtx != nil {
		r = aCtx
	}
	var principal *models.Principal
	if uprinc != nil {
		principal = uprinc.(*models.Principal) // this is really a models.Principal, I promise
	}

	if err := o.Context.BindValidRequest(r, route, &Params); err != nil { // bind params
		o.Context.Respond(rw, r, route.Produces, route, err)
		return
	}

	res := o.Handler.Handle(Params, principal) // actually handle the request

	o.Context.Respond(rw, r, route.Produces, route, res)

}

// WeaviateBatchingThingsCreateBody weaviate batching things create body
// swagger:model WeaviateBatchingThingsCreateBody
type WeaviateBatchingThingsCreateBody struct {

	// Define which fields need to be returned. Default value is ALL
	Fields []*string `json:"fields"`

	// things
	Things []*models.Thing `json:"things"`
}

// Validate validates this weaviate batching things create body
func (o *WeaviateBatchingThingsCreateBody) Validate(formats strfmt.Registry) error {
	var res []error

	if err := o.validateFields(formats); err != nil {
		res = append(res, err)
	}

	if err := o.validateThings(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

var weaviateBatchingThingsCreateBodyFieldsItemsEnum []interface{}

func init() {
	var res []string
	if err := json.Unmarshal([]byte(`["ALL","@class","schema","key","thingId","creationTimeUnix"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		weaviateBatchingThingsCreateBodyFieldsItemsEnum = append(weaviateBatchingThingsCreateBodyFieldsItemsEnum, v)
	}
}

func (o *WeaviateBatchingThingsCreateBody) validateFieldsItemsEnum(path, location string, value string) error {
	if err := validate.Enum(path, location, value, weaviateBatchingThingsCreateBodyFieldsItemsEnum); err != nil {
		return err
	}
	return nil
}

func (o *WeaviateBatchingThingsCreateBody) validateFields(formats strfmt.Registry) error {

	if swag.IsZero(o.Fields) { // not required
		return nil
	}

	for i := 0; i < len(o.Fields); i++ {
		if swag.IsZero(o.Fields[i]) { // not required
			continue
		}

		// value enum
		if err := o.validateFieldsItemsEnum("body"+"."+"fields"+"."+strconv.Itoa(i), "body", *o.Fields[i]); err != nil {
			return err
		}

	}

	return nil
}

func (o *WeaviateBatchingThingsCreateBody) validateThings(formats strfmt.Registry) error {

	if swag.IsZero(o.Things) { // not required
		return nil
	}

	for i := 0; i < len(o.Things); i++ {
		if swag.IsZero(o.Things[i]) { // not required
			continue
		}

		if o.Things[i] != nil {
			if err := o.Things[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("body" + "." + "things" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (o *WeaviateBatchingThingsCreateBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *WeaviateBatchingThingsCreateBody) UnmarshalBinary(b []byte) error {
	var res WeaviateBatchingThingsCreateBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
