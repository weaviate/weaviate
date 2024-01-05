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

package backups

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

// NewBackupsRestoreParams creates a new BackupsRestoreParams object
//
// There are no default values defined in the spec.
func NewBackupsRestoreParams() BackupsRestoreParams {

	return BackupsRestoreParams{}
}

// BackupsRestoreParams contains all the bound params for the backups restore operation
// typically these are obtained from a http.Request
//
// swagger:parameters backups.restore
type BackupsRestoreParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request `json:"-"`

	/*Backup backend name e.g. filesystem, gcs, s3.
	  Required: true
	  In: path
	*/
	Backend string
	/*
	  Required: true
	  In: body
	*/
	Body *models.BackupRestoreRequest
	/*The ID of a backup. Must be URL-safe and work as a filesystem path, only lowercase, numbers, underscore, minus characters allowed.
	  Required: true
	  In: path
	*/
	ID string
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls.
//
// To ensure default values, the struct must have been initialized with NewBackupsRestoreParams() beforehand.
func (o *BackupsRestoreParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error

	o.HTTPRequest = r

	rBackend, rhkBackend, _ := route.Params.GetOK("backend")
	if err := o.bindBackend(rBackend, rhkBackend, route.Formats); err != nil {
		res = append(res, err)
	}

	if runtime.HasBody(r) {
		defer r.Body.Close()
		var body models.BackupRestoreRequest
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

	rID, rhkID, _ := route.Params.GetOK("id")
	if err := o.bindID(rID, rhkID, route.Formats); err != nil {
		res = append(res, err)
	}
	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// bindBackend binds and validates parameter Backend from path.
func (o *BackupsRestoreParams) bindBackend(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: true
	// Parameter is provided by construction from the route
	o.Backend = raw

	return nil
}

// bindID binds and validates parameter ID from path.
func (o *BackupsRestoreParams) bindID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: true
	// Parameter is provided by construction from the route
	o.ID = raw

	return nil
}
