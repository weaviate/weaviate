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

 
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateThingsEventsListOKCode is the HTTP code returned for type WeaviateThingsEventsListOK
const WeaviateThingsEventsListOKCode int = 200

/*WeaviateThingsEventsListOK Successful response.

swagger:response weaviateThingsEventsListOK
*/
type WeaviateThingsEventsListOK struct {

	/*
	  In: Body
	*/
	Payload *models.EventsListResponse `json:"body,omitempty"`
}

// NewWeaviateThingsEventsListOK creates WeaviateThingsEventsListOK with default headers values
func NewWeaviateThingsEventsListOK() *WeaviateThingsEventsListOK {
	return &WeaviateThingsEventsListOK{}
}

// WithPayload adds the payload to the weaviate things events list o k response
func (o *WeaviateThingsEventsListOK) WithPayload(payload *models.EventsListResponse) *WeaviateThingsEventsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things events list o k response
func (o *WeaviateThingsEventsListOK) SetPayload(payload *models.EventsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsEventsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsEventsListUnauthorizedCode is the HTTP code returned for type WeaviateThingsEventsListUnauthorized
const WeaviateThingsEventsListUnauthorizedCode int = 401

/*WeaviateThingsEventsListUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsEventsListUnauthorized
*/
type WeaviateThingsEventsListUnauthorized struct {
}

// NewWeaviateThingsEventsListUnauthorized creates WeaviateThingsEventsListUnauthorized with default headers values
func NewWeaviateThingsEventsListUnauthorized() *WeaviateThingsEventsListUnauthorized {
	return &WeaviateThingsEventsListUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsEventsListUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsEventsListForbiddenCode is the HTTP code returned for type WeaviateThingsEventsListForbidden
const WeaviateThingsEventsListForbiddenCode int = 403

/*WeaviateThingsEventsListForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsEventsListForbidden
*/
type WeaviateThingsEventsListForbidden struct {
}

// NewWeaviateThingsEventsListForbidden creates WeaviateThingsEventsListForbidden with default headers values
func NewWeaviateThingsEventsListForbidden() *WeaviateThingsEventsListForbidden {
	return &WeaviateThingsEventsListForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsEventsListForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsEventsListNotFoundCode is the HTTP code returned for type WeaviateThingsEventsListNotFound
const WeaviateThingsEventsListNotFoundCode int = 404

/*WeaviateThingsEventsListNotFound Successful query result but no resource was found.

swagger:response weaviateThingsEventsListNotFound
*/
type WeaviateThingsEventsListNotFound struct {
}

// NewWeaviateThingsEventsListNotFound creates WeaviateThingsEventsListNotFound with default headers values
func NewWeaviateThingsEventsListNotFound() *WeaviateThingsEventsListNotFound {
	return &WeaviateThingsEventsListNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingsEventsListNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingsEventsListNotImplementedCode is the HTTP code returned for type WeaviateThingsEventsListNotImplemented
const WeaviateThingsEventsListNotImplementedCode int = 501

/*WeaviateThingsEventsListNotImplemented Not (yet) implemented.

swagger:response weaviateThingsEventsListNotImplemented
*/
type WeaviateThingsEventsListNotImplemented struct {
}

// NewWeaviateThingsEventsListNotImplemented creates WeaviateThingsEventsListNotImplemented with default headers values
func NewWeaviateThingsEventsListNotImplemented() *WeaviateThingsEventsListNotImplemented {
	return &WeaviateThingsEventsListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsEventsListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
