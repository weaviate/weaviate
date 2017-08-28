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

 
 

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateThingsPatchOKCode is the HTTP code returned for type WeaviateThingsPatchOK
const WeaviateThingsPatchOKCode int = 200

/*WeaviateThingsPatchOK Successful update.

swagger:response weaviateThingsPatchOK
*/
type WeaviateThingsPatchOK struct {

	/*
	  In: Body
	*/
	Payload *models.ThingGetResponse `json:"body,omitempty"`
}

// NewWeaviateThingsPatchOK creates WeaviateThingsPatchOK with default headers values
func NewWeaviateThingsPatchOK() *WeaviateThingsPatchOK {
	return &WeaviateThingsPatchOK{}
}

// WithPayload adds the payload to the weaviate things patch o k response
func (o *WeaviateThingsPatchOK) WithPayload(payload *models.ThingGetResponse) *WeaviateThingsPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things patch o k response
func (o *WeaviateThingsPatchOK) SetPayload(payload *models.ThingGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsPatchBadRequestCode is the HTTP code returned for type WeaviateThingsPatchBadRequest
const WeaviateThingsPatchBadRequestCode int = 400

/*WeaviateThingsPatchBadRequest The patch-JSON is malformed.

swagger:response weaviateThingsPatchBadRequest
*/
type WeaviateThingsPatchBadRequest struct {
}

// NewWeaviateThingsPatchBadRequest creates WeaviateThingsPatchBadRequest with default headers values
func NewWeaviateThingsPatchBadRequest() *WeaviateThingsPatchBadRequest {
	return &WeaviateThingsPatchBadRequest{}
}

// WriteResponse to the client
func (o *WeaviateThingsPatchBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
}

// WeaviateThingsPatchUnauthorizedCode is the HTTP code returned for type WeaviateThingsPatchUnauthorized
const WeaviateThingsPatchUnauthorizedCode int = 401

/*WeaviateThingsPatchUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsPatchUnauthorized
*/
type WeaviateThingsPatchUnauthorized struct {
}

// NewWeaviateThingsPatchUnauthorized creates WeaviateThingsPatchUnauthorized with default headers values
func NewWeaviateThingsPatchUnauthorized() *WeaviateThingsPatchUnauthorized {
	return &WeaviateThingsPatchUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsPatchUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsPatchForbiddenCode is the HTTP code returned for type WeaviateThingsPatchForbidden
const WeaviateThingsPatchForbiddenCode int = 403

/*WeaviateThingsPatchForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsPatchForbidden
*/
type WeaviateThingsPatchForbidden struct {
}

// NewWeaviateThingsPatchForbidden creates WeaviateThingsPatchForbidden with default headers values
func NewWeaviateThingsPatchForbidden() *WeaviateThingsPatchForbidden {
	return &WeaviateThingsPatchForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsPatchForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsPatchNotFoundCode is the HTTP code returned for type WeaviateThingsPatchNotFound
const WeaviateThingsPatchNotFoundCode int = 404

/*WeaviateThingsPatchNotFound Successful query result but no resource was found.

swagger:response weaviateThingsPatchNotFound
*/
type WeaviateThingsPatchNotFound struct {
}

// NewWeaviateThingsPatchNotFound creates WeaviateThingsPatchNotFound with default headers values
func NewWeaviateThingsPatchNotFound() *WeaviateThingsPatchNotFound {
	return &WeaviateThingsPatchNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingsPatchNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingsPatchUnprocessableEntityCode is the HTTP code returned for type WeaviateThingsPatchUnprocessableEntity
const WeaviateThingsPatchUnprocessableEntityCode int = 422

/*WeaviateThingsPatchUnprocessableEntity The patch-JSON is valid but unprocessable.

swagger:response weaviateThingsPatchUnprocessableEntity
*/
type WeaviateThingsPatchUnprocessableEntity struct {
}

// NewWeaviateThingsPatchUnprocessableEntity creates WeaviateThingsPatchUnprocessableEntity with default headers values
func NewWeaviateThingsPatchUnprocessableEntity() *WeaviateThingsPatchUnprocessableEntity {
	return &WeaviateThingsPatchUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateThingsPatchUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateThingsPatchNotImplementedCode is the HTTP code returned for type WeaviateThingsPatchNotImplemented
const WeaviateThingsPatchNotImplementedCode int = 501

/*WeaviateThingsPatchNotImplemented Not (yet) implemented.

swagger:response weaviateThingsPatchNotImplemented
*/
type WeaviateThingsPatchNotImplemented struct {
}

// NewWeaviateThingsPatchNotImplemented creates WeaviateThingsPatchNotImplemented with default headers values
func NewWeaviateThingsPatchNotImplemented() *WeaviateThingsPatchNotImplemented {
	return &WeaviateThingsPatchNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsPatchNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
