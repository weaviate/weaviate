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
   

package events

 
 

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateEventsPatchOKCode is the HTTP code returned for type WeaviateEventsPatchOK
const WeaviateEventsPatchOKCode int = 200

/*WeaviateEventsPatchOK Successful updated.

swagger:response weaviateEventsPatchOK
*/
type WeaviateEventsPatchOK struct {

	/*
	  In: Body
	*/
	Payload *models.EventGetResponse `json:"body,omitempty"`
}

// NewWeaviateEventsPatchOK creates WeaviateEventsPatchOK with default headers values
func NewWeaviateEventsPatchOK() *WeaviateEventsPatchOK {
	return &WeaviateEventsPatchOK{}
}

// WithPayload adds the payload to the weaviate events patch o k response
func (o *WeaviateEventsPatchOK) WithPayload(payload *models.EventGetResponse) *WeaviateEventsPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate events patch o k response
func (o *WeaviateEventsPatchOK) SetPayload(payload *models.EventGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateEventsPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateEventsPatchBadRequestCode is the HTTP code returned for type WeaviateEventsPatchBadRequest
const WeaviateEventsPatchBadRequestCode int = 400

/*WeaviateEventsPatchBadRequest The patch-JSON is malformed.

swagger:response weaviateEventsPatchBadRequest
*/
type WeaviateEventsPatchBadRequest struct {
}

// NewWeaviateEventsPatchBadRequest creates WeaviateEventsPatchBadRequest with default headers values
func NewWeaviateEventsPatchBadRequest() *WeaviateEventsPatchBadRequest {
	return &WeaviateEventsPatchBadRequest{}
}

// WriteResponse to the client
func (o *WeaviateEventsPatchBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
}

// WeaviateEventsPatchUnauthorizedCode is the HTTP code returned for type WeaviateEventsPatchUnauthorized
const WeaviateEventsPatchUnauthorizedCode int = 401

/*WeaviateEventsPatchUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateEventsPatchUnauthorized
*/
type WeaviateEventsPatchUnauthorized struct {
}

// NewWeaviateEventsPatchUnauthorized creates WeaviateEventsPatchUnauthorized with default headers values
func NewWeaviateEventsPatchUnauthorized() *WeaviateEventsPatchUnauthorized {
	return &WeaviateEventsPatchUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateEventsPatchUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateEventsPatchForbiddenCode is the HTTP code returned for type WeaviateEventsPatchForbidden
const WeaviateEventsPatchForbiddenCode int = 403

/*WeaviateEventsPatchForbidden The used API-key has insufficient permissions.

swagger:response weaviateEventsPatchForbidden
*/
type WeaviateEventsPatchForbidden struct {
}

// NewWeaviateEventsPatchForbidden creates WeaviateEventsPatchForbidden with default headers values
func NewWeaviateEventsPatchForbidden() *WeaviateEventsPatchForbidden {
	return &WeaviateEventsPatchForbidden{}
}

// WriteResponse to the client
func (o *WeaviateEventsPatchForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateEventsPatchNotFoundCode is the HTTP code returned for type WeaviateEventsPatchNotFound
const WeaviateEventsPatchNotFoundCode int = 404

/*WeaviateEventsPatchNotFound Successful query result but no resource was found.

swagger:response weaviateEventsPatchNotFound
*/
type WeaviateEventsPatchNotFound struct {
}

// NewWeaviateEventsPatchNotFound creates WeaviateEventsPatchNotFound with default headers values
func NewWeaviateEventsPatchNotFound() *WeaviateEventsPatchNotFound {
	return &WeaviateEventsPatchNotFound{}
}

// WriteResponse to the client
func (o *WeaviateEventsPatchNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateEventsPatchUnprocessableEntityCode is the HTTP code returned for type WeaviateEventsPatchUnprocessableEntity
const WeaviateEventsPatchUnprocessableEntityCode int = 422

/*WeaviateEventsPatchUnprocessableEntity The patch-JSON is valid but unprocessable.

swagger:response weaviateEventsPatchUnprocessableEntity
*/
type WeaviateEventsPatchUnprocessableEntity struct {
}

// NewWeaviateEventsPatchUnprocessableEntity creates WeaviateEventsPatchUnprocessableEntity with default headers values
func NewWeaviateEventsPatchUnprocessableEntity() *WeaviateEventsPatchUnprocessableEntity {
	return &WeaviateEventsPatchUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateEventsPatchUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateEventsPatchNotImplementedCode is the HTTP code returned for type WeaviateEventsPatchNotImplemented
const WeaviateEventsPatchNotImplementedCode int = 501

/*WeaviateEventsPatchNotImplemented Not (yet) implemented.

swagger:response weaviateEventsPatchNotImplemented
*/
type WeaviateEventsPatchNotImplemented struct {
}

// NewWeaviateEventsPatchNotImplemented creates WeaviateEventsPatchNotImplemented with default headers values
func NewWeaviateEventsPatchNotImplemented() *WeaviateEventsPatchNotImplemented {
	return &WeaviateEventsPatchNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateEventsPatchNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
