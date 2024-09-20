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
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
)

// NewBackupsRestoreStatusParams creates a new BackupsRestoreStatusParams object
//
// There are no default values defined in the spec.
func NewBackupsRestoreStatusParams() BackupsRestoreStatusParams {

	return BackupsRestoreStatusParams{}
}

// BackupsRestoreStatusParams contains all the bound params for the backups restore status operation
// typically these are obtained from a http.Request
//
// swagger:parameters backups.restore.status
type BackupsRestoreStatusParams struct {

	// HTTP Request Object
	HTTPRequest *http.Request `json:"-"`

	/*Backup backend name e.g. filesystem, gcs, s3.
	  Required: true
	  In: path
	*/
	Backend string
	/*The ID of a backup. Must be URL-safe and work as a filesystem path, only lowercase, numbers, underscore, minus characters allowed.
	  Required: true
	  In: path
	*/
	ID string
	/*The name of the bucket
	  In: query
	*/
	S3bucket *string
	/*The path within the bucket
	  In: query
	*/
	S3path *string
}

// BindRequest both binds and validates a request, it assumes that complex things implement a Validatable(strfmt.Registry) error interface
// for simple values it will use straight method calls.
//
// To ensure default values, the struct must have been initialized with NewBackupsRestoreStatusParams() beforehand.
func (o *BackupsRestoreStatusParams) BindRequest(r *http.Request, route *middleware.MatchedRoute) error {
	var res []error

	o.HTTPRequest = r

	qs := runtime.Values(r.URL.Query())

	rBackend, rhkBackend, _ := route.Params.GetOK("backend")
	if err := o.bindBackend(rBackend, rhkBackend, route.Formats); err != nil {
		res = append(res, err)
	}

	rID, rhkID, _ := route.Params.GetOK("id")
	if err := o.bindID(rID, rhkID, route.Formats); err != nil {
		res = append(res, err)
	}

	qS3bucket, qhkS3bucket, _ := qs.GetOK("s3bucket")
	if err := o.bindS3bucket(qS3bucket, qhkS3bucket, route.Formats); err != nil {
		res = append(res, err)
	}

	qS3path, qhkS3path, _ := qs.GetOK("s3path")
	if err := o.bindS3path(qS3path, qhkS3path, route.Formats); err != nil {
		res = append(res, err)
	}
	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

// bindBackend binds and validates parameter Backend from path.
func (o *BackupsRestoreStatusParams) bindBackend(rawData []string, hasKey bool, formats strfmt.Registry) error {
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
func (o *BackupsRestoreStatusParams) bindID(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: true
	// Parameter is provided by construction from the route
	o.ID = raw

	return nil
}

// bindS3bucket binds and validates parameter S3bucket from query.
func (o *BackupsRestoreStatusParams) bindS3bucket(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: false
	// AllowEmptyValue: false

	if raw == "" { // empty values pass all other validations
		return nil
	}
	o.S3bucket = &raw

	return nil
}

// bindS3path binds and validates parameter S3path from query.
func (o *BackupsRestoreStatusParams) bindS3path(rawData []string, hasKey bool, formats strfmt.Registry) error {
	var raw string
	if len(rawData) > 0 {
		raw = rawData[len(rawData)-1]
	}

	// Required: false
	// AllowEmptyValue: false

	if raw == "" { // empty values pass all other validations
		return nil
	}
	o.S3path = &raw

	return nil
}
