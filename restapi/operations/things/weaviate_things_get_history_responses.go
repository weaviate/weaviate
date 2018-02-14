/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @CreativeSofwFdn / yourfriends@weaviate.com
 */

package things

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/creativesoftwarefdn/weaviate/models"
)

// WeaviateThingsGetHistoryOKCode is the HTTP code returned for type WeaviateThingsGetHistoryOK
const WeaviateThingsGetHistoryOKCode int = 200

/*WeaviateThingsGetHistoryOK Successful response.

swagger:response weaviateThingsGetHistoryOK
*/
type WeaviateThingsGetHistoryOK struct {

	/*
	  In: Body
	*/
	Payload *models.ThingGetHistoryResponse `json:"body,omitempty"`
}

// NewWeaviateThingsGetHistoryOK creates WeaviateThingsGetHistoryOK with default headers values
func NewWeaviateThingsGetHistoryOK() *WeaviateThingsGetHistoryOK {
	return &WeaviateThingsGetHistoryOK{}
}

// WithPayload adds the payload to the weaviate things get history o k response
func (o *WeaviateThingsGetHistoryOK) WithPayload(payload *models.ThingGetHistoryResponse) *WeaviateThingsGetHistoryOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things get history o k response
func (o *WeaviateThingsGetHistoryOK) SetPayload(payload *models.ThingGetHistoryResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsGetHistoryOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsGetHistoryUnauthorizedCode is the HTTP code returned for type WeaviateThingsGetHistoryUnauthorized
const WeaviateThingsGetHistoryUnauthorizedCode int = 401

/*WeaviateThingsGetHistoryUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsGetHistoryUnauthorized
*/
type WeaviateThingsGetHistoryUnauthorized struct {
}

// NewWeaviateThingsGetHistoryUnauthorized creates WeaviateThingsGetHistoryUnauthorized with default headers values
func NewWeaviateThingsGetHistoryUnauthorized() *WeaviateThingsGetHistoryUnauthorized {
	return &WeaviateThingsGetHistoryUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsGetHistoryUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsGetHistoryForbiddenCode is the HTTP code returned for type WeaviateThingsGetHistoryForbidden
const WeaviateThingsGetHistoryForbiddenCode int = 403

/*WeaviateThingsGetHistoryForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsGetHistoryForbidden
*/
type WeaviateThingsGetHistoryForbidden struct {
}

// NewWeaviateThingsGetHistoryForbidden creates WeaviateThingsGetHistoryForbidden with default headers values
func NewWeaviateThingsGetHistoryForbidden() *WeaviateThingsGetHistoryForbidden {
	return &WeaviateThingsGetHistoryForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsGetHistoryForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsGetHistoryNotFoundCode is the HTTP code returned for type WeaviateThingsGetHistoryNotFound
const WeaviateThingsGetHistoryNotFoundCode int = 404

/*WeaviateThingsGetHistoryNotFound Successful query result but no resource was found.

swagger:response weaviateThingsGetHistoryNotFound
*/
type WeaviateThingsGetHistoryNotFound struct {
}

// NewWeaviateThingsGetHistoryNotFound creates WeaviateThingsGetHistoryNotFound with default headers values
func NewWeaviateThingsGetHistoryNotFound() *WeaviateThingsGetHistoryNotFound {
	return &WeaviateThingsGetHistoryNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingsGetHistoryNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingsGetHistoryNotImplementedCode is the HTTP code returned for type WeaviateThingsGetHistoryNotImplemented
const WeaviateThingsGetHistoryNotImplementedCode int = 501

/*WeaviateThingsGetHistoryNotImplemented Not (yet) implemented.

swagger:response weaviateThingsGetHistoryNotImplemented
*/
type WeaviateThingsGetHistoryNotImplemented struct {
}

// NewWeaviateThingsGetHistoryNotImplemented creates WeaviateThingsGetHistoryNotImplemented with default headers values
func NewWeaviateThingsGetHistoryNotImplemented() *WeaviateThingsGetHistoryNotImplemented {
	return &WeaviateThingsGetHistoryNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsGetHistoryNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
