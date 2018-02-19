/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

package actions

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/creativesoftwarefdn/weaviate/models"
)

// WeaviateActionsPatchOKCode is the HTTP code returned for type WeaviateActionsPatchOK
const WeaviateActionsPatchOKCode int = 200

/*WeaviateActionsPatchOK Successful updated.

swagger:response weaviateActionsPatchOK
*/
type WeaviateActionsPatchOK struct {

	/*
	  In: Body
	*/
	Payload *models.ActionGetResponse `json:"body,omitempty"`
}

// NewWeaviateActionsPatchOK creates WeaviateActionsPatchOK with default headers values
func NewWeaviateActionsPatchOK() *WeaviateActionsPatchOK {
	return &WeaviateActionsPatchOK{}
}

// WithPayload adds the payload to the weaviate actions patch o k response
func (o *WeaviateActionsPatchOK) WithPayload(payload *models.ActionGetResponse) *WeaviateActionsPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate actions patch o k response
func (o *WeaviateActionsPatchOK) SetPayload(payload *models.ActionGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateActionsPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateActionsPatchBadRequestCode is the HTTP code returned for type WeaviateActionsPatchBadRequest
const WeaviateActionsPatchBadRequestCode int = 400

/*WeaviateActionsPatchBadRequest The patch-JSON is malformed.

swagger:response weaviateActionsPatchBadRequest
*/
type WeaviateActionsPatchBadRequest struct {
}

// NewWeaviateActionsPatchBadRequest creates WeaviateActionsPatchBadRequest with default headers values
func NewWeaviateActionsPatchBadRequest() *WeaviateActionsPatchBadRequest {
	return &WeaviateActionsPatchBadRequest{}
}

// WriteResponse to the client
func (o *WeaviateActionsPatchBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
}

// WeaviateActionsPatchUnauthorizedCode is the HTTP code returned for type WeaviateActionsPatchUnauthorized
const WeaviateActionsPatchUnauthorizedCode int = 401

/*WeaviateActionsPatchUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateActionsPatchUnauthorized
*/
type WeaviateActionsPatchUnauthorized struct {
}

// NewWeaviateActionsPatchUnauthorized creates WeaviateActionsPatchUnauthorized with default headers values
func NewWeaviateActionsPatchUnauthorized() *WeaviateActionsPatchUnauthorized {
	return &WeaviateActionsPatchUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateActionsPatchUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateActionsPatchForbiddenCode is the HTTP code returned for type WeaviateActionsPatchForbidden
const WeaviateActionsPatchForbiddenCode int = 403

/*WeaviateActionsPatchForbidden The used API-key has insufficient permissions.

swagger:response weaviateActionsPatchForbidden
*/
type WeaviateActionsPatchForbidden struct {
}

// NewWeaviateActionsPatchForbidden creates WeaviateActionsPatchForbidden with default headers values
func NewWeaviateActionsPatchForbidden() *WeaviateActionsPatchForbidden {
	return &WeaviateActionsPatchForbidden{}
}

// WriteResponse to the client
func (o *WeaviateActionsPatchForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateActionsPatchNotFoundCode is the HTTP code returned for type WeaviateActionsPatchNotFound
const WeaviateActionsPatchNotFoundCode int = 404

/*WeaviateActionsPatchNotFound Successful query result but no resource was found.

swagger:response weaviateActionsPatchNotFound
*/
type WeaviateActionsPatchNotFound struct {
}

// NewWeaviateActionsPatchNotFound creates WeaviateActionsPatchNotFound with default headers values
func NewWeaviateActionsPatchNotFound() *WeaviateActionsPatchNotFound {
	return &WeaviateActionsPatchNotFound{}
}

// WriteResponse to the client
func (o *WeaviateActionsPatchNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateActionsPatchUnprocessableEntityCode is the HTTP code returned for type WeaviateActionsPatchUnprocessableEntity
const WeaviateActionsPatchUnprocessableEntityCode int = 422

/*WeaviateActionsPatchUnprocessableEntity The patch-JSON is valid but unprocessable.

swagger:response weaviateActionsPatchUnprocessableEntity
*/
type WeaviateActionsPatchUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewWeaviateActionsPatchUnprocessableEntity creates WeaviateActionsPatchUnprocessableEntity with default headers values
func NewWeaviateActionsPatchUnprocessableEntity() *WeaviateActionsPatchUnprocessableEntity {
	return &WeaviateActionsPatchUnprocessableEntity{}
}

// WithPayload adds the payload to the weaviate actions patch unprocessable entity response
func (o *WeaviateActionsPatchUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *WeaviateActionsPatchUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate actions patch unprocessable entity response
func (o *WeaviateActionsPatchUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateActionsPatchUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateActionsPatchNotImplementedCode is the HTTP code returned for type WeaviateActionsPatchNotImplemented
const WeaviateActionsPatchNotImplementedCode int = 501

/*WeaviateActionsPatchNotImplemented Not (yet) implemented.

swagger:response weaviateActionsPatchNotImplemented
*/
type WeaviateActionsPatchNotImplemented struct {
}

// NewWeaviateActionsPatchNotImplemented creates WeaviateActionsPatchNotImplemented with default headers values
func NewWeaviateActionsPatchNotImplemented() *WeaviateActionsPatchNotImplemented {
	return &WeaviateActionsPatchNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateActionsPatchNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
