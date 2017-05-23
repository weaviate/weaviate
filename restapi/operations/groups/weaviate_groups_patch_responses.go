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
 package groups




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateGroupsPatchOKCode is the HTTP code returned for type WeaviateGroupsPatchOK
const WeaviateGroupsPatchOKCode int = 200

/*WeaviateGroupsPatchOK Successful updated.

swagger:response weaviateGroupsPatchOK
*/
type WeaviateGroupsPatchOK struct {

	/*
	  In: Body
	*/
	Payload *models.Group `json:"body,omitempty"`
}

// NewWeaviateGroupsPatchOK creates WeaviateGroupsPatchOK with default headers values
func NewWeaviateGroupsPatchOK() *WeaviateGroupsPatchOK {
	return &WeaviateGroupsPatchOK{}
}

// WithPayload adds the payload to the weaviate groups patch o k response
func (o *WeaviateGroupsPatchOK) WithPayload(payload *models.Group) *WeaviateGroupsPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate groups patch o k response
func (o *WeaviateGroupsPatchOK) SetPayload(payload *models.Group) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateGroupsPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateGroupsPatchBadRequestCode is the HTTP code returned for type WeaviateGroupsPatchBadRequest
const WeaviateGroupsPatchBadRequestCode int = 400

/*WeaviateGroupsPatchBadRequest The patch-JSON is malformed.

swagger:response weaviateGroupsPatchBadRequest
*/
type WeaviateGroupsPatchBadRequest struct {
}

// NewWeaviateGroupsPatchBadRequest creates WeaviateGroupsPatchBadRequest with default headers values
func NewWeaviateGroupsPatchBadRequest() *WeaviateGroupsPatchBadRequest {
	return &WeaviateGroupsPatchBadRequest{}
}

// WriteResponse to the client
func (o *WeaviateGroupsPatchBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
}

// WeaviateGroupsPatchUnauthorizedCode is the HTTP code returned for type WeaviateGroupsPatchUnauthorized
const WeaviateGroupsPatchUnauthorizedCode int = 401

/*WeaviateGroupsPatchUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateGroupsPatchUnauthorized
*/
type WeaviateGroupsPatchUnauthorized struct {
}

// NewWeaviateGroupsPatchUnauthorized creates WeaviateGroupsPatchUnauthorized with default headers values
func NewWeaviateGroupsPatchUnauthorized() *WeaviateGroupsPatchUnauthorized {
	return &WeaviateGroupsPatchUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateGroupsPatchUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateGroupsPatchForbiddenCode is the HTTP code returned for type WeaviateGroupsPatchForbidden
const WeaviateGroupsPatchForbiddenCode int = 403

/*WeaviateGroupsPatchForbidden The used API-key has insufficient permissions.

swagger:response weaviateGroupsPatchForbidden
*/
type WeaviateGroupsPatchForbidden struct {
}

// NewWeaviateGroupsPatchForbidden creates WeaviateGroupsPatchForbidden with default headers values
func NewWeaviateGroupsPatchForbidden() *WeaviateGroupsPatchForbidden {
	return &WeaviateGroupsPatchForbidden{}
}

// WriteResponse to the client
func (o *WeaviateGroupsPatchForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateGroupsPatchNotFoundCode is the HTTP code returned for type WeaviateGroupsPatchNotFound
const WeaviateGroupsPatchNotFoundCode int = 404

/*WeaviateGroupsPatchNotFound Successful query result but no resource was found.

swagger:response weaviateGroupsPatchNotFound
*/
type WeaviateGroupsPatchNotFound struct {
}

// NewWeaviateGroupsPatchNotFound creates WeaviateGroupsPatchNotFound with default headers values
func NewWeaviateGroupsPatchNotFound() *WeaviateGroupsPatchNotFound {
	return &WeaviateGroupsPatchNotFound{}
}

// WriteResponse to the client
func (o *WeaviateGroupsPatchNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateGroupsPatchUnprocessableEntityCode is the HTTP code returned for type WeaviateGroupsPatchUnprocessableEntity
const WeaviateGroupsPatchUnprocessableEntityCode int = 422

/*WeaviateGroupsPatchUnprocessableEntity The patch-JSON is valid but unprocessable.

swagger:response weaviateGroupsPatchUnprocessableEntity
*/
type WeaviateGroupsPatchUnprocessableEntity struct {
}

// NewWeaviateGroupsPatchUnprocessableEntity creates WeaviateGroupsPatchUnprocessableEntity with default headers values
func NewWeaviateGroupsPatchUnprocessableEntity() *WeaviateGroupsPatchUnprocessableEntity {
	return &WeaviateGroupsPatchUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateGroupsPatchUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateGroupsPatchNotImplementedCode is the HTTP code returned for type WeaviateGroupsPatchNotImplemented
const WeaviateGroupsPatchNotImplementedCode int = 501

/*WeaviateGroupsPatchNotImplemented Not (yet) implemented.

swagger:response weaviateGroupsPatchNotImplemented
*/
type WeaviateGroupsPatchNotImplemented struct {
}

// NewWeaviateGroupsPatchNotImplemented creates WeaviateGroupsPatchNotImplemented with default headers values
func NewWeaviateGroupsPatchNotImplemented() *WeaviateGroupsPatchNotImplemented {
	return &WeaviateGroupsPatchNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateGroupsPatchNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
