/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN: Bob van Luijt (bob@k10y.co)
 */
// Code generated by go-swagger; DO NOT EDIT.

package things

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	models "github.com/creativesoftwarefdn/weaviate/models"
)

// WeaviateThingsPropertiesDeleteNoContentCode is the HTTP code returned for type WeaviateThingsPropertiesDeleteNoContent
const WeaviateThingsPropertiesDeleteNoContentCode int = 204

/*WeaviateThingsPropertiesDeleteNoContent Successfully deleted.

swagger:response weaviateThingsPropertiesDeleteNoContent
*/
type WeaviateThingsPropertiesDeleteNoContent struct {
}

// NewWeaviateThingsPropertiesDeleteNoContent creates WeaviateThingsPropertiesDeleteNoContent with default headers values
func NewWeaviateThingsPropertiesDeleteNoContent() *WeaviateThingsPropertiesDeleteNoContent {

	return &WeaviateThingsPropertiesDeleteNoContent{}
}

// WriteResponse to the client
func (o *WeaviateThingsPropertiesDeleteNoContent) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(204)
}

// WeaviateThingsPropertiesDeleteUnauthorizedCode is the HTTP code returned for type WeaviateThingsPropertiesDeleteUnauthorized
const WeaviateThingsPropertiesDeleteUnauthorizedCode int = 401

/*WeaviateThingsPropertiesDeleteUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsPropertiesDeleteUnauthorized
*/
type WeaviateThingsPropertiesDeleteUnauthorized struct {
}

// NewWeaviateThingsPropertiesDeleteUnauthorized creates WeaviateThingsPropertiesDeleteUnauthorized with default headers values
func NewWeaviateThingsPropertiesDeleteUnauthorized() *WeaviateThingsPropertiesDeleteUnauthorized {

	return &WeaviateThingsPropertiesDeleteUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsPropertiesDeleteUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// WeaviateThingsPropertiesDeleteForbiddenCode is the HTTP code returned for type WeaviateThingsPropertiesDeleteForbidden
const WeaviateThingsPropertiesDeleteForbiddenCode int = 403

/*WeaviateThingsPropertiesDeleteForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsPropertiesDeleteForbidden
*/
type WeaviateThingsPropertiesDeleteForbidden struct {
}

// NewWeaviateThingsPropertiesDeleteForbidden creates WeaviateThingsPropertiesDeleteForbidden with default headers values
func NewWeaviateThingsPropertiesDeleteForbidden() *WeaviateThingsPropertiesDeleteForbidden {

	return &WeaviateThingsPropertiesDeleteForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsPropertiesDeleteForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(403)
}

// WeaviateThingsPropertiesDeleteNotFoundCode is the HTTP code returned for type WeaviateThingsPropertiesDeleteNotFound
const WeaviateThingsPropertiesDeleteNotFoundCode int = 404

/*WeaviateThingsPropertiesDeleteNotFound Successful query result but no resource was found.

swagger:response weaviateThingsPropertiesDeleteNotFound
*/
type WeaviateThingsPropertiesDeleteNotFound struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewWeaviateThingsPropertiesDeleteNotFound creates WeaviateThingsPropertiesDeleteNotFound with default headers values
func NewWeaviateThingsPropertiesDeleteNotFound() *WeaviateThingsPropertiesDeleteNotFound {

	return &WeaviateThingsPropertiesDeleteNotFound{}
}

// WithPayload adds the payload to the weaviate things properties delete not found response
func (o *WeaviateThingsPropertiesDeleteNotFound) WithPayload(payload *models.ErrorResponse) *WeaviateThingsPropertiesDeleteNotFound {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things properties delete not found response
func (o *WeaviateThingsPropertiesDeleteNotFound) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsPropertiesDeleteNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
