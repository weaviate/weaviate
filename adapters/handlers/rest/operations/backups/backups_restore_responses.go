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

	"github.com/go-openapi/runtime"

	"github.com/liutizhong/weaviate/entities/models"
)

// BackupsRestoreOKCode is the HTTP code returned for type BackupsRestoreOK
const BackupsRestoreOKCode int = 200

/*
BackupsRestoreOK Backup restoration process successfully started.

swagger:response backupsRestoreOK
*/
type BackupsRestoreOK struct {

	/*
	  In: Body
	*/
	Payload *models.BackupRestoreResponse `json:"body,omitempty"`
}

// NewBackupsRestoreOK creates BackupsRestoreOK with default headers values
func NewBackupsRestoreOK() *BackupsRestoreOK {

	return &BackupsRestoreOK{}
}

// WithPayload adds the payload to the backups restore o k response
func (o *BackupsRestoreOK) WithPayload(payload *models.BackupRestoreResponse) *BackupsRestoreOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the backups restore o k response
func (o *BackupsRestoreOK) SetPayload(payload *models.BackupRestoreResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BackupsRestoreOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// BackupsRestoreUnauthorizedCode is the HTTP code returned for type BackupsRestoreUnauthorized
const BackupsRestoreUnauthorizedCode int = 401

/*
BackupsRestoreUnauthorized Unauthorized or invalid credentials.

swagger:response backupsRestoreUnauthorized
*/
type BackupsRestoreUnauthorized struct {
}

// NewBackupsRestoreUnauthorized creates BackupsRestoreUnauthorized with default headers values
func NewBackupsRestoreUnauthorized() *BackupsRestoreUnauthorized {

	return &BackupsRestoreUnauthorized{}
}

// WriteResponse to the client
func (o *BackupsRestoreUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// BackupsRestoreForbiddenCode is the HTTP code returned for type BackupsRestoreForbidden
const BackupsRestoreForbiddenCode int = 403

/*
BackupsRestoreForbidden Forbidden

swagger:response backupsRestoreForbidden
*/
type BackupsRestoreForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewBackupsRestoreForbidden creates BackupsRestoreForbidden with default headers values
func NewBackupsRestoreForbidden() *BackupsRestoreForbidden {

	return &BackupsRestoreForbidden{}
}

// WithPayload adds the payload to the backups restore forbidden response
func (o *BackupsRestoreForbidden) WithPayload(payload *models.ErrorResponse) *BackupsRestoreForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the backups restore forbidden response
func (o *BackupsRestoreForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BackupsRestoreForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// BackupsRestoreNotFoundCode is the HTTP code returned for type BackupsRestoreNotFound
const BackupsRestoreNotFoundCode int = 404

/*
BackupsRestoreNotFound Not Found - Backup does not exist

swagger:response backupsRestoreNotFound
*/
type BackupsRestoreNotFound struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewBackupsRestoreNotFound creates BackupsRestoreNotFound with default headers values
func NewBackupsRestoreNotFound() *BackupsRestoreNotFound {

	return &BackupsRestoreNotFound{}
}

// WithPayload adds the payload to the backups restore not found response
func (o *BackupsRestoreNotFound) WithPayload(payload *models.ErrorResponse) *BackupsRestoreNotFound {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the backups restore not found response
func (o *BackupsRestoreNotFound) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BackupsRestoreNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// BackupsRestoreUnprocessableEntityCode is the HTTP code returned for type BackupsRestoreUnprocessableEntity
const BackupsRestoreUnprocessableEntityCode int = 422

/*
BackupsRestoreUnprocessableEntity Invalid backup restoration attempt.

swagger:response backupsRestoreUnprocessableEntity
*/
type BackupsRestoreUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewBackupsRestoreUnprocessableEntity creates BackupsRestoreUnprocessableEntity with default headers values
func NewBackupsRestoreUnprocessableEntity() *BackupsRestoreUnprocessableEntity {

	return &BackupsRestoreUnprocessableEntity{}
}

// WithPayload adds the payload to the backups restore unprocessable entity response
func (o *BackupsRestoreUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *BackupsRestoreUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the backups restore unprocessable entity response
func (o *BackupsRestoreUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BackupsRestoreUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// BackupsRestoreInternalServerErrorCode is the HTTP code returned for type BackupsRestoreInternalServerError
const BackupsRestoreInternalServerErrorCode int = 500

/*
BackupsRestoreInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response backupsRestoreInternalServerError
*/
type BackupsRestoreInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewBackupsRestoreInternalServerError creates BackupsRestoreInternalServerError with default headers values
func NewBackupsRestoreInternalServerError() *BackupsRestoreInternalServerError {

	return &BackupsRestoreInternalServerError{}
}

// WithPayload adds the payload to the backups restore internal server error response
func (o *BackupsRestoreInternalServerError) WithPayload(payload *models.ErrorResponse) *BackupsRestoreInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the backups restore internal server error response
func (o *BackupsRestoreInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *BackupsRestoreInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
