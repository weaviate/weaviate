// Code generated by go-swagger; DO NOT EDIT.

package batch

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
)

// NewBatchObjectsCreateParams creates a new BatchObjectsCreateParams object
//
// There are no default values defined in the spec.
func NewBatchObjectsCreateParams() BatchObjectsCreateParams {

	return BatchObjectsCreateParams{}
}

// BatchObjectsCreateParams contains all the bound params for the batch objects create operation
// typically these are obtained from a http.Request
//
// swagger:parameters batch.objects.create
type BatchObjectsCreateParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request `json:"-"`

	/*
	  Required: true
	  In: body
	*/
	Body BatchObjectsCreateBody
	/*Determines how many replicas must acknowledge a request before it is considered successful
	  In: query
	*/
	ConsistencyLevel *string
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls.
//
// To ensure default values, the struct must have been initialized with NewBatchObjectsCreateParams() beforehand.
func (o *BatchObjectsCreateParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error

	o.HTTPRequest = r

	qs := runtime.Values(r.URL.Query())

	if runtime.HasBody(r) {
		defer r.Body.Close()
		var body BatchObjectsCreateBody
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
				o.Body = body
			}
		}
	} else {
		res = append(res, errors.Required("body", "body", ""))
	}

	qConsistencyLevel, qhkConsistencyLevel, _ := qs.GetOK("consistency_level")
	if err := o.bindConsistencyLevel(qConsistencyLevel, qhkConsistencyLevel, route.Formats); err != nil {
		res = append(res, err)
	}
	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// bindConsistencyLevel binds and validates parameter ConsistencyLevel from query.
func (o *BatchObjectsCreateParams) bindConsistencyLevel(rawData []string, hasKey bool, formats strfmt.Registry) error {
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
