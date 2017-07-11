/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/master/LICENSE
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */
  package things

 
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateThingsUpdateOKCode is the HTTP code returned for type WeaviateThingsUpdateOK
const WeaviateThingsUpdateOKCode int = 200

/*WeaviateThingsUpdateOK Successful update.

swagger:response weaviateThingsUpdateOK
*/
type WeaviateThingsUpdateOK struct {

	/*
	  In: Body
	*/
	Payload *models.ThingGetResponse `json:"body,omitempty"`
}

// NewWeaviateThingsUpdateOK creates WeaviateThingsUpdateOK with default headers values
func NewWeaviateThingsUpdateOK() *WeaviateThingsUpdateOK {
	return &WeaviateThingsUpdateOK{}
}

// WithPayload adds the payload to the weaviate things update o k response
func (o *WeaviateThingsUpdateOK) WithPayload(payload *models.ThingGetResponse) *WeaviateThingsUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things update o k response
func (o *WeaviateThingsUpdateOK) SetPayload(payload *models.ThingGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsUpdateUnauthorizedCode is the HTTP code returned for type WeaviateThingsUpdateUnauthorized
const WeaviateThingsUpdateUnauthorizedCode int = 401

/*WeaviateThingsUpdateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsUpdateUnauthorized
*/
type WeaviateThingsUpdateUnauthorized struct {
}

// NewWeaviateThingsUpdateUnauthorized creates WeaviateThingsUpdateUnauthorized with default headers values
func NewWeaviateThingsUpdateUnauthorized() *WeaviateThingsUpdateUnauthorized {
	return &WeaviateThingsUpdateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsUpdateForbiddenCode is the HTTP code returned for type WeaviateThingsUpdateForbidden
const WeaviateThingsUpdateForbiddenCode int = 403

/*WeaviateThingsUpdateForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsUpdateForbidden
*/
type WeaviateThingsUpdateForbidden struct {
}

// NewWeaviateThingsUpdateForbidden creates WeaviateThingsUpdateForbidden with default headers values
func NewWeaviateThingsUpdateForbidden() *WeaviateThingsUpdateForbidden {
	return &WeaviateThingsUpdateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsUpdateNotFoundCode is the HTTP code returned for type WeaviateThingsUpdateNotFound
const WeaviateThingsUpdateNotFoundCode int = 404

/*WeaviateThingsUpdateNotFound Successful query result but no resource was found.

swagger:response weaviateThingsUpdateNotFound
*/
type WeaviateThingsUpdateNotFound struct {
}

// NewWeaviateThingsUpdateNotFound creates WeaviateThingsUpdateNotFound with default headers values
func NewWeaviateThingsUpdateNotFound() *WeaviateThingsUpdateNotFound {
	return &WeaviateThingsUpdateNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingsUpdateUnprocessableEntityCode is the HTTP code returned for type WeaviateThingsUpdateUnprocessableEntity
const WeaviateThingsUpdateUnprocessableEntityCode int = 422

/*WeaviateThingsUpdateUnprocessableEntity Can not validate, check the body.

swagger:response weaviateThingsUpdateUnprocessableEntity
*/
type WeaviateThingsUpdateUnprocessableEntity struct {
}

// NewWeaviateThingsUpdateUnprocessableEntity creates WeaviateThingsUpdateUnprocessableEntity with default headers values
func NewWeaviateThingsUpdateUnprocessableEntity() *WeaviateThingsUpdateUnprocessableEntity {
	return &WeaviateThingsUpdateUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateThingsUpdateNotImplementedCode is the HTTP code returned for type WeaviateThingsUpdateNotImplemented
const WeaviateThingsUpdateNotImplementedCode int = 501

/*WeaviateThingsUpdateNotImplemented Not (yet) implemented.

swagger:response weaviateThingsUpdateNotImplemented
*/
type WeaviateThingsUpdateNotImplemented struct {
}

// NewWeaviateThingsUpdateNotImplemented creates WeaviateThingsUpdateNotImplemented with default headers values
func NewWeaviateThingsUpdateNotImplemented() *WeaviateThingsUpdateNotImplemented {
	return &WeaviateThingsUpdateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
