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

// WeaviateEventsListOKCode is the HTTP code returned for type WeaviateEventsListOK
const WeaviateEventsListOKCode int = 200

/*WeaviateEventsListOK Successful response.

swagger:response weaviateEventsListOK
*/
type WeaviateEventsListOK struct {

	/*
	  In: Body
	*/
	Payload *models.EventsListResponse `json:"body,omitempty"`
}

// NewWeaviateEventsListOK creates WeaviateEventsListOK with default headers values
func NewWeaviateEventsListOK() *WeaviateEventsListOK {
	return &WeaviateEventsListOK{}
}

// WithPayload adds the payload to the weaviate events list o k response
func (o *WeaviateEventsListOK) WithPayload(payload *models.EventsListResponse) *WeaviateEventsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate events list o k response
func (o *WeaviateEventsListOK) SetPayload(payload *models.EventsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateEventsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateEventsListUnauthorizedCode is the HTTP code returned for type WeaviateEventsListUnauthorized
const WeaviateEventsListUnauthorizedCode int = 401

/*WeaviateEventsListUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateEventsListUnauthorized
*/
type WeaviateEventsListUnauthorized struct {
}

// NewWeaviateEventsListUnauthorized creates WeaviateEventsListUnauthorized with default headers values
func NewWeaviateEventsListUnauthorized() *WeaviateEventsListUnauthorized {
	return &WeaviateEventsListUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateEventsListUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateEventsListForbiddenCode is the HTTP code returned for type WeaviateEventsListForbidden
const WeaviateEventsListForbiddenCode int = 403

/*WeaviateEventsListForbidden The used API-key has insufficient permissions.

swagger:response weaviateEventsListForbidden
*/
type WeaviateEventsListForbidden struct {
}

// NewWeaviateEventsListForbidden creates WeaviateEventsListForbidden with default headers values
func NewWeaviateEventsListForbidden() *WeaviateEventsListForbidden {
	return &WeaviateEventsListForbidden{}
}

// WriteResponse to the client
func (o *WeaviateEventsListForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateEventsListNotFoundCode is the HTTP code returned for type WeaviateEventsListNotFound
const WeaviateEventsListNotFoundCode int = 404

/*WeaviateEventsListNotFound Successful query result but no resource was found.

swagger:response weaviateEventsListNotFound
*/
type WeaviateEventsListNotFound struct {
}

// NewWeaviateEventsListNotFound creates WeaviateEventsListNotFound with default headers values
func NewWeaviateEventsListNotFound() *WeaviateEventsListNotFound {
	return &WeaviateEventsListNotFound{}
}

// WriteResponse to the client
func (o *WeaviateEventsListNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateEventsListNotImplementedCode is the HTTP code returned for type WeaviateEventsListNotImplemented
const WeaviateEventsListNotImplementedCode int = 501

/*WeaviateEventsListNotImplemented Not (yet) implemented.

swagger:response weaviateEventsListNotImplemented
*/
type WeaviateEventsListNotImplemented struct {
}

// NewWeaviateEventsListNotImplemented creates WeaviateEventsListNotImplemented with default headers values
func NewWeaviateEventsListNotImplemented() *WeaviateEventsListNotImplemented {
	return &WeaviateEventsListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateEventsListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
