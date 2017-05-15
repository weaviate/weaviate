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
 package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateDevicesPatchOKCode is the HTTP code returned for type WeaviateDevicesPatchOK
const WeaviateDevicesPatchOKCode int = 200

/*WeaviateDevicesPatchOK Successful update.

swagger:response weaviateDevicesPatchOK
*/
type WeaviateDevicesPatchOK struct {

	/*
	  In: Body
	*/
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaviateDevicesPatchOK creates WeaviateDevicesPatchOK with default headers values
func NewWeaviateDevicesPatchOK() *WeaviateDevicesPatchOK {
	return &WeaviateDevicesPatchOK{}
}

// WithPayload adds the payload to the weaviate devices patch o k response
func (o *WeaviateDevicesPatchOK) WithPayload(payload *models.Device) *WeaviateDevicesPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate devices patch o k response
func (o *WeaviateDevicesPatchOK) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateDevicesPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateDevicesPatchBadRequestCode is the HTTP code returned for type WeaviateDevicesPatchBadRequest
const WeaviateDevicesPatchBadRequestCode int = 400

/*WeaviateDevicesPatchBadRequest The patch-JSON is malformed.

swagger:response weaviateDevicesPatchBadRequest
*/
type WeaviateDevicesPatchBadRequest struct {
}

// NewWeaviateDevicesPatchBadRequest creates WeaviateDevicesPatchBadRequest with default headers values
func NewWeaviateDevicesPatchBadRequest() *WeaviateDevicesPatchBadRequest {
	return &WeaviateDevicesPatchBadRequest{}
}

// WriteResponse to the client
func (o *WeaviateDevicesPatchBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
}

// WeaviateDevicesPatchUnauthorizedCode is the HTTP code returned for type WeaviateDevicesPatchUnauthorized
const WeaviateDevicesPatchUnauthorizedCode int = 401

/*WeaviateDevicesPatchUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateDevicesPatchUnauthorized
*/
type WeaviateDevicesPatchUnauthorized struct {
}

// NewWeaviateDevicesPatchUnauthorized creates WeaviateDevicesPatchUnauthorized with default headers values
func NewWeaviateDevicesPatchUnauthorized() *WeaviateDevicesPatchUnauthorized {
	return &WeaviateDevicesPatchUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateDevicesPatchUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateDevicesPatchForbiddenCode is the HTTP code returned for type WeaviateDevicesPatchForbidden
const WeaviateDevicesPatchForbiddenCode int = 403

/*WeaviateDevicesPatchForbidden The used API-key has insufficient permissions.

swagger:response weaviateDevicesPatchForbidden
*/
type WeaviateDevicesPatchForbidden struct {
}

// NewWeaviateDevicesPatchForbidden creates WeaviateDevicesPatchForbidden with default headers values
func NewWeaviateDevicesPatchForbidden() *WeaviateDevicesPatchForbidden {
	return &WeaviateDevicesPatchForbidden{}
}

// WriteResponse to the client
func (o *WeaviateDevicesPatchForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateDevicesPatchNotFoundCode is the HTTP code returned for type WeaviateDevicesPatchNotFound
const WeaviateDevicesPatchNotFoundCode int = 404

/*WeaviateDevicesPatchNotFound Successful query result but no resource was found.

swagger:response weaviateDevicesPatchNotFound
*/
type WeaviateDevicesPatchNotFound struct {
}

// NewWeaviateDevicesPatchNotFound creates WeaviateDevicesPatchNotFound with default headers values
func NewWeaviateDevicesPatchNotFound() *WeaviateDevicesPatchNotFound {
	return &WeaviateDevicesPatchNotFound{}
}

// WriteResponse to the client
func (o *WeaviateDevicesPatchNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateDevicesPatchUnprocessableEntityCode is the HTTP code returned for type WeaviateDevicesPatchUnprocessableEntity
const WeaviateDevicesPatchUnprocessableEntityCode int = 422

/*WeaviateDevicesPatchUnprocessableEntity The patch-JSON is valid but unprocessable.

swagger:response weaviateDevicesPatchUnprocessableEntity
*/
type WeaviateDevicesPatchUnprocessableEntity struct {
}

// NewWeaviateDevicesPatchUnprocessableEntity creates WeaviateDevicesPatchUnprocessableEntity with default headers values
func NewWeaviateDevicesPatchUnprocessableEntity() *WeaviateDevicesPatchUnprocessableEntity {
	return &WeaviateDevicesPatchUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateDevicesPatchUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateDevicesPatchNotImplementedCode is the HTTP code returned for type WeaviateDevicesPatchNotImplemented
const WeaviateDevicesPatchNotImplementedCode int = 501

/*WeaviateDevicesPatchNotImplemented Not (yet) implemented.

swagger:response weaviateDevicesPatchNotImplemented
*/
type WeaviateDevicesPatchNotImplemented struct {
}

// NewWeaviateDevicesPatchNotImplemented creates WeaviateDevicesPatchNotImplemented with default headers values
func NewWeaviateDevicesPatchNotImplemented() *WeaviateDevicesPatchNotImplemented {
	return &WeaviateDevicesPatchNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateDevicesPatchNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
