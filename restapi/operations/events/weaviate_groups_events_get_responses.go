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

// WeaviateGroupsEventsGetOKCode is the HTTP code returned for type WeaviateGroupsEventsGetOK
const WeaviateGroupsEventsGetOKCode int = 200

/*WeaviateGroupsEventsGetOK Successful response.

swagger:response weaviateGroupsEventsGetOK
*/
type WeaviateGroupsEventsGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.EventGetResponse `json:"body,omitempty"`
}

// NewWeaviateGroupsEventsGetOK creates WeaviateGroupsEventsGetOK with default headers values
func NewWeaviateGroupsEventsGetOK() *WeaviateGroupsEventsGetOK {
	return &WeaviateGroupsEventsGetOK{}
}

// WithPayload adds the payload to the weaviate groups events get o k response
func (o *WeaviateGroupsEventsGetOK) WithPayload(payload *models.EventGetResponse) *WeaviateGroupsEventsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate groups events get o k response
func (o *WeaviateGroupsEventsGetOK) SetPayload(payload *models.EventGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateGroupsEventsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateGroupsEventsGetUnauthorizedCode is the HTTP code returned for type WeaviateGroupsEventsGetUnauthorized
const WeaviateGroupsEventsGetUnauthorizedCode int = 401

/*WeaviateGroupsEventsGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateGroupsEventsGetUnauthorized
*/
type WeaviateGroupsEventsGetUnauthorized struct {
}

// NewWeaviateGroupsEventsGetUnauthorized creates WeaviateGroupsEventsGetUnauthorized with default headers values
func NewWeaviateGroupsEventsGetUnauthorized() *WeaviateGroupsEventsGetUnauthorized {
	return &WeaviateGroupsEventsGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateGroupsEventsGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateGroupsEventsGetForbiddenCode is the HTTP code returned for type WeaviateGroupsEventsGetForbidden
const WeaviateGroupsEventsGetForbiddenCode int = 403

/*WeaviateGroupsEventsGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateGroupsEventsGetForbidden
*/
type WeaviateGroupsEventsGetForbidden struct {
}

// NewWeaviateGroupsEventsGetForbidden creates WeaviateGroupsEventsGetForbidden with default headers values
func NewWeaviateGroupsEventsGetForbidden() *WeaviateGroupsEventsGetForbidden {
	return &WeaviateGroupsEventsGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateGroupsEventsGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateGroupsEventsGetNotFoundCode is the HTTP code returned for type WeaviateGroupsEventsGetNotFound
const WeaviateGroupsEventsGetNotFoundCode int = 404

/*WeaviateGroupsEventsGetNotFound Successful query result but no resource was found.

swagger:response weaviateGroupsEventsGetNotFound
*/
type WeaviateGroupsEventsGetNotFound struct {
}

// NewWeaviateGroupsEventsGetNotFound creates WeaviateGroupsEventsGetNotFound with default headers values
func NewWeaviateGroupsEventsGetNotFound() *WeaviateGroupsEventsGetNotFound {
	return &WeaviateGroupsEventsGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateGroupsEventsGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateGroupsEventsGetNotImplementedCode is the HTTP code returned for type WeaviateGroupsEventsGetNotImplemented
const WeaviateGroupsEventsGetNotImplementedCode int = 501

/*WeaviateGroupsEventsGetNotImplemented Not (yet) implemented.

swagger:response weaviateGroupsEventsGetNotImplemented
*/
type WeaviateGroupsEventsGetNotImplemented struct {
}

// NewWeaviateGroupsEventsGetNotImplemented creates WeaviateGroupsEventsGetNotImplemented with default headers values
func NewWeaviateGroupsEventsGetNotImplemented() *WeaviateGroupsEventsGetNotImplemented {
	return &WeaviateGroupsEventsGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateGroupsEventsGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
