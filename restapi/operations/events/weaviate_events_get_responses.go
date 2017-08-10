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

// WeaviateEventsGetOKCode is the HTTP code returned for type WeaviateEventsGetOK
const WeaviateEventsGetOKCode int = 200

/*WeaviateEventsGetOK Successful response.

swagger:response weaviateEventsGetOK
*/
type WeaviateEventsGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.EventGetResponse `json:"body,omitempty"`
}

// NewWeaviateEventsGetOK creates WeaviateEventsGetOK with default headers values
func NewWeaviateEventsGetOK() *WeaviateEventsGetOK {
	return &WeaviateEventsGetOK{}
}

// WithPayload adds the payload to the weaviate events get o k response
func (o *WeaviateEventsGetOK) WithPayload(payload *models.EventGetResponse) *WeaviateEventsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate events get o k response
func (o *WeaviateEventsGetOK) SetPayload(payload *models.EventGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateEventsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateEventsGetUnauthorizedCode is the HTTP code returned for type WeaviateEventsGetUnauthorized
const WeaviateEventsGetUnauthorizedCode int = 401

/*WeaviateEventsGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateEventsGetUnauthorized
*/
type WeaviateEventsGetUnauthorized struct {
}

// NewWeaviateEventsGetUnauthorized creates WeaviateEventsGetUnauthorized with default headers values
func NewWeaviateEventsGetUnauthorized() *WeaviateEventsGetUnauthorized {
	return &WeaviateEventsGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateEventsGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateEventsGetForbiddenCode is the HTTP code returned for type WeaviateEventsGetForbidden
const WeaviateEventsGetForbiddenCode int = 403

/*WeaviateEventsGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateEventsGetForbidden
*/
type WeaviateEventsGetForbidden struct {
}

// NewWeaviateEventsGetForbidden creates WeaviateEventsGetForbidden with default headers values
func NewWeaviateEventsGetForbidden() *WeaviateEventsGetForbidden {
	return &WeaviateEventsGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateEventsGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateEventsGetNotFoundCode is the HTTP code returned for type WeaviateEventsGetNotFound
const WeaviateEventsGetNotFoundCode int = 404

/*WeaviateEventsGetNotFound Successful query result but no resource was found.

swagger:response weaviateEventsGetNotFound
*/
type WeaviateEventsGetNotFound struct {
}

// NewWeaviateEventsGetNotFound creates WeaviateEventsGetNotFound with default headers values
func NewWeaviateEventsGetNotFound() *WeaviateEventsGetNotFound {
	return &WeaviateEventsGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateEventsGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateEventsGetNotImplementedCode is the HTTP code returned for type WeaviateEventsGetNotImplemented
const WeaviateEventsGetNotImplementedCode int = 501

/*WeaviateEventsGetNotImplemented Not (yet) implemented.

swagger:response weaviateEventsGetNotImplemented
*/
type WeaviateEventsGetNotImplemented struct {
}

// NewWeaviateEventsGetNotImplemented creates WeaviateEventsGetNotImplemented with default headers values
func NewWeaviateEventsGetNotImplemented() *WeaviateEventsGetNotImplemented {
	return &WeaviateEventsGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateEventsGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
