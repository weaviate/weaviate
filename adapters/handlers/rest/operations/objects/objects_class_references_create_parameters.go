// Code generated by go-swagger; DO NOT EDIT.

package objects

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"io"
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/validate"

	"github.com/weaviate/weaviate/entities/models"
)

// NewObjectsClassReferencesCreateParams creates a new ObjectsClassReferencesCreateParams object
//
// There are no default values defined in the spec.
func NewObjectsClassReferencesCreateParams() ObjectsClassReferencesCreateParams {

	return ObjectsClassReferencesCreateParams{}
}

// ObjectsClassReferencesCreateParams contains all the bound params for the objects class references create operation
// typically these are obtained from a http.Request
//
// swagger:parameters objects.class.references.create
type ObjectsClassReferencesCreateParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request `json:"-"`

	/*
	  Required: true
	  In: body
	*/
	Body *models.SingleRef
	/*The class name as defined in the schema
	  Required: true
	  In: path
	*/
	ClassName string
	/*Determines how many replicas must acknowledge a request before it is considered successful
	  In: query
	*/
	ConsistencyLevel *string
	/*Unique ID of the Object.
	  Required: true
	  In: path
	*/
	ID strfmt.UUID
	/*Unique name of the property related to the Object.
	  Required: true
	  In: path
	*/
	PropertyName string
	/*Specifies the tenant in a request targeting a multi-tenant class
	  In: query
	*/
	Tenant *string
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls.
//
// To ensure default values, the struct must have been initialized with NewObjectsClassReferencesCreateParams() beforehand.
func (o *ObjectsClassReferencesCreateParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error

	o.HTTPRequest = r

	qs := runtime.Values(r.URL.Query())

	if runtime.HasBody(r) {
		defer r.Body.Close()
		var body models.SingleRef
		if err := route.Consumer.Consume(r.Body, &body); err != nil {
			if err == io.EOF {
				res = append(res, errors.Required("body", "body", ""))
			} else {
				res = append(res, errors.NewParseError("body", "body", "", err))
			}
		} else {
			// validate body object
			if err := body.Validate(route.Formats); err != nil {
				res = append(res, err)
			}

			ctx := validate.WithOperationRequest(r.Context())
			if err := body.ContextValidate(ctx, route.Formats); err != nil {
				res = append(res, err)
			}

			if len(res) == 0 {
				o.Body = &body
			}
		}
	} else {
		res = append(res, errors.Required("body", "body", ""))
	}

	rClassName, rhkClassName, _ := route.Params.GetOK("className")
	if err := o.bindClassName(rClassName, rhkClassName, route.Formats); err != nil {
		res = append(res, err)
	}

	qConsistencyLevel, qhkConsistencyLevel, _ := qs.GetOK("consistency_level")
	if err := o.bindConsistencyLevel(qConsistencyLevel, qhkConsistencyLevel, route.Formats); err != nil {
		res = append(res, err)
	}

	rID, rhkID, _ := route.Params.GetOK("id")
	if err := o.bindID(rID, rhkID, route.Formats); err != nil {
		res = append(res, err)
	}

	rPropertyName, rhkPropertyName, _ := route.Params.GetOK("propertyName")
	if err := o.bindPropertyName(rPropertyName, rhkPropertyName, route.Formats); err != nil {
		res = append(res, err)
	}

	qTenant, qhkTenant, _ := qs.GetOK("tenant")
	if err := o.bindTenant(qTenant, qhkTenant, route.Formats); err != nil {
		res = append(res, err)
	}
	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// bindClassName binds and validates parameter ClassName from path.
func (o *ObjectsClassReferencesCreateParams) bindClassName(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: true
	// Parameter is provided by construction from the route
	o.ClassName = raw

	return nil
}

// bindConsistencyLevel binds and validates parameter ConsistencyLevel from query.
func (o *ObjectsClassReferencesCreateParams) bindConsistencyLevel(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: false
	// AllowEmptyValue: false

	if raw == "" { // empty values pass all other validations
		return nil
	}
	o.ConsistencyLevel = &raw

	return nil
}

// bindID binds and validates parameter ID from path.
func (o *ObjectsClassReferencesCreateParams) bindID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: true
	// Parameter is provided by construction from the route

	// Format: uuid
	value, err := formats.Parse("uuid", raw)
	if err != nil {
		return errors.InvalidType("id", "path", "strfmt.UUID", raw)
	}
	o.ID = *(value.(*strfmt.UUID))

	if err := o.validateID(formats); err != nil {
		return err
	}

	return nil
}

// validateID carries on validations for parameter ID
func (o *ObjectsClassReferencesCreateParams) validateID(formats strfmt.Registry) error {

	if err := validate.FormatOf("id", "path", "uuid", o.ID.String(), formats); err != nil {
		return err
	}
	return nil
}

// bindPropertyName binds and validates parameter PropertyName from path.
func (o *ObjectsClassReferencesCreateParams) bindPropertyName(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: true
	// Parameter is provided by construction from the route
	o.PropertyName = raw

	return nil
}

// bindTenant binds and validates parameter Tenant from query.
func (o *ObjectsClassReferencesCreateParams) bindTenant(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: false
	// AllowEmptyValue: false

	if raw == "" { // empty values pass all other validations
		return nil
	}
	o.Tenant = &raw

	return nil
}
