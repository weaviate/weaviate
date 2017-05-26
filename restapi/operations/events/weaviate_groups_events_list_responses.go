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

// WeaviateGroupsEventsListOKCode is the HTTP code returned for type WeaviateGroupsEventsListOK
const WeaviateGroupsEventsListOKCode int = 200

/*WeaviateGroupsEventsListOK Successful response.

swagger:response weaviateGroupsEventsListOK
*/
type WeaviateGroupsEventsListOK struct {

	/*
	  In: Body
	*/
	Payload *models.EventsListResponse `json:"body,omitempty"`
}

// NewWeaviateGroupsEventsListOK creates WeaviateGroupsEventsListOK with default headers values
func NewWeaviateGroupsEventsListOK() *WeaviateGroupsEventsListOK {
	return &WeaviateGroupsEventsListOK{}
}

// WithPayload adds the payload to the weaviate groups events list o k response
func (o *WeaviateGroupsEventsListOK) WithPayload(payload *models.EventsListResponse) *WeaviateGroupsEventsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate groups events list o k response
func (o *WeaviateGroupsEventsListOK) SetPayload(payload *models.EventsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateGroupsEventsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateGroupsEventsListUnauthorizedCode is the HTTP code returned for type WeaviateGroupsEventsListUnauthorized
const WeaviateGroupsEventsListUnauthorizedCode int = 401

/*WeaviateGroupsEventsListUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateGroupsEventsListUnauthorized
*/
type WeaviateGroupsEventsListUnauthorized struct {
}

// NewWeaviateGroupsEventsListUnauthorized creates WeaviateGroupsEventsListUnauthorized with default headers values
func NewWeaviateGroupsEventsListUnauthorized() *WeaviateGroupsEventsListUnauthorized {
	return &WeaviateGroupsEventsListUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateGroupsEventsListUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateGroupsEventsListForbiddenCode is the HTTP code returned for type WeaviateGroupsEventsListForbidden
const WeaviateGroupsEventsListForbiddenCode int = 403

/*WeaviateGroupsEventsListForbidden The used API-key has insufficient permissions.

swagger:response weaviateGroupsEventsListForbidden
*/
type WeaviateGroupsEventsListForbidden struct {
}

// NewWeaviateGroupsEventsListForbidden creates WeaviateGroupsEventsListForbidden with default headers values
func NewWeaviateGroupsEventsListForbidden() *WeaviateGroupsEventsListForbidden {
	return &WeaviateGroupsEventsListForbidden{}
}

// WriteResponse to the client
func (o *WeaviateGroupsEventsListForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateGroupsEventsListNotFoundCode is the HTTP code returned for type WeaviateGroupsEventsListNotFound
const WeaviateGroupsEventsListNotFoundCode int = 404

/*WeaviateGroupsEventsListNotFound Successful query result but no resource was found.

swagger:response weaviateGroupsEventsListNotFound
*/
type WeaviateGroupsEventsListNotFound struct {
}

// NewWeaviateGroupsEventsListNotFound creates WeaviateGroupsEventsListNotFound with default headers values
func NewWeaviateGroupsEventsListNotFound() *WeaviateGroupsEventsListNotFound {
	return &WeaviateGroupsEventsListNotFound{}
}

// WriteResponse to the client
func (o *WeaviateGroupsEventsListNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateGroupsEventsListUnprocessableEntityCode is the HTTP code returned for type WeaviateGroupsEventsListUnprocessableEntity
const WeaviateGroupsEventsListUnprocessableEntityCode int = 422

/*WeaviateGroupsEventsListUnprocessableEntity Can not execute this command, because the commandParameters{} are set incorrectly.

swagger:response weaviateGroupsEventsListUnprocessableEntity
*/
type WeaviateGroupsEventsListUnprocessableEntity struct {
}

// NewWeaviateGroupsEventsListUnprocessableEntity creates WeaviateGroupsEventsListUnprocessableEntity with default headers values
func NewWeaviateGroupsEventsListUnprocessableEntity() *WeaviateGroupsEventsListUnprocessableEntity {
	return &WeaviateGroupsEventsListUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateGroupsEventsListUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateGroupsEventsListNotImplementedCode is the HTTP code returned for type WeaviateGroupsEventsListNotImplemented
const WeaviateGroupsEventsListNotImplementedCode int = 501

/*WeaviateGroupsEventsListNotImplemented Not (yet) implemented.

swagger:response weaviateGroupsEventsListNotImplemented
*/
type WeaviateGroupsEventsListNotImplemented struct {
}

// NewWeaviateGroupsEventsListNotImplemented creates WeaviateGroupsEventsListNotImplemented with default headers values
func NewWeaviateGroupsEventsListNotImplemented() *WeaviateGroupsEventsListNotImplemented {
	return &WeaviateGroupsEventsListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateGroupsEventsListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
