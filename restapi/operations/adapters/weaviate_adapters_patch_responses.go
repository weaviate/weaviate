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
 package adapters




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateAdaptersPatchOKCode is the HTTP code returned for type WeaviateAdaptersPatchOK
const WeaviateAdaptersPatchOKCode int = 200

/*WeaviateAdaptersPatchOK Successful updated.

swagger:response weaviateAdaptersPatchOK
*/
type WeaviateAdaptersPatchOK struct {

	/*
	  In: Body
	*/
	Payload *models.Adapter `json:"body,omitempty"`
}

// NewWeaviateAdaptersPatchOK creates WeaviateAdaptersPatchOK with default headers values
func NewWeaviateAdaptersPatchOK() *WeaviateAdaptersPatchOK {
	return &WeaviateAdaptersPatchOK{}
}

// WithPayload adds the payload to the weaviate adapters patch o k response
func (o *WeaviateAdaptersPatchOK) WithPayload(payload *models.Adapter) *WeaviateAdaptersPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate adapters patch o k response
func (o *WeaviateAdaptersPatchOK) SetPayload(payload *models.Adapter) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateAdaptersPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateAdaptersPatchBadRequestCode is the HTTP code returned for type WeaviateAdaptersPatchBadRequest
const WeaviateAdaptersPatchBadRequestCode int = 400

/*WeaviateAdaptersPatchBadRequest The patch-JSON is malformed.

swagger:response weaviateAdaptersPatchBadRequest
*/
type WeaviateAdaptersPatchBadRequest struct {
}

// NewWeaviateAdaptersPatchBadRequest creates WeaviateAdaptersPatchBadRequest with default headers values
func NewWeaviateAdaptersPatchBadRequest() *WeaviateAdaptersPatchBadRequest {
	return &WeaviateAdaptersPatchBadRequest{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersPatchBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
}

// WeaviateAdaptersPatchUnauthorizedCode is the HTTP code returned for type WeaviateAdaptersPatchUnauthorized
const WeaviateAdaptersPatchUnauthorizedCode int = 401

/*WeaviateAdaptersPatchUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateAdaptersPatchUnauthorized
*/
type WeaviateAdaptersPatchUnauthorized struct {
}

// NewWeaviateAdaptersPatchUnauthorized creates WeaviateAdaptersPatchUnauthorized with default headers values
func NewWeaviateAdaptersPatchUnauthorized() *WeaviateAdaptersPatchUnauthorized {
	return &WeaviateAdaptersPatchUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersPatchUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateAdaptersPatchForbiddenCode is the HTTP code returned for type WeaviateAdaptersPatchForbidden
const WeaviateAdaptersPatchForbiddenCode int = 403

/*WeaviateAdaptersPatchForbidden The used API-key has insufficient permissions.

swagger:response weaviateAdaptersPatchForbidden
*/
type WeaviateAdaptersPatchForbidden struct {
}

// NewWeaviateAdaptersPatchForbidden creates WeaviateAdaptersPatchForbidden with default headers values
func NewWeaviateAdaptersPatchForbidden() *WeaviateAdaptersPatchForbidden {
	return &WeaviateAdaptersPatchForbidden{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersPatchForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateAdaptersPatchNotFoundCode is the HTTP code returned for type WeaviateAdaptersPatchNotFound
const WeaviateAdaptersPatchNotFoundCode int = 404

/*WeaviateAdaptersPatchNotFound Successful query result but no resource was found.

swagger:response weaviateAdaptersPatchNotFound
*/
type WeaviateAdaptersPatchNotFound struct {
}

// NewWeaviateAdaptersPatchNotFound creates WeaviateAdaptersPatchNotFound with default headers values
func NewWeaviateAdaptersPatchNotFound() *WeaviateAdaptersPatchNotFound {
	return &WeaviateAdaptersPatchNotFound{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersPatchNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateAdaptersPatchUnprocessableEntityCode is the HTTP code returned for type WeaviateAdaptersPatchUnprocessableEntity
const WeaviateAdaptersPatchUnprocessableEntityCode int = 422

/*WeaviateAdaptersPatchUnprocessableEntity The patch-JSON is valid but unprocessable.

swagger:response weaviateAdaptersPatchUnprocessableEntity
*/
type WeaviateAdaptersPatchUnprocessableEntity struct {
}

// NewWeaviateAdaptersPatchUnprocessableEntity creates WeaviateAdaptersPatchUnprocessableEntity with default headers values
func NewWeaviateAdaptersPatchUnprocessableEntity() *WeaviateAdaptersPatchUnprocessableEntity {
	return &WeaviateAdaptersPatchUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersPatchUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateAdaptersPatchNotImplementedCode is the HTTP code returned for type WeaviateAdaptersPatchNotImplemented
const WeaviateAdaptersPatchNotImplementedCode int = 501

/*WeaviateAdaptersPatchNotImplemented Not (yet) implemented.

swagger:response weaviateAdaptersPatchNotImplemented
*/
type WeaviateAdaptersPatchNotImplemented struct {
}

// NewWeaviateAdaptersPatchNotImplemented creates WeaviateAdaptersPatchNotImplemented with default headers values
func NewWeaviateAdaptersPatchNotImplemented() *WeaviateAdaptersPatchNotImplemented {
	return &WeaviateAdaptersPatchNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersPatchNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
