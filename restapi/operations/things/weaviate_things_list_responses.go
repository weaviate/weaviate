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
  package things

 
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateThingsListOKCode is the HTTP code returned for type WeaviateThingsListOK
const WeaviateThingsListOKCode int = 200

/*WeaviateThingsListOK Successful response.

swagger:response weaviateThingsListOK
*/
type WeaviateThingsListOK struct {

	/*
	  In: Body
	*/
	Payload *models.ThingsListResponse `json:"body,omitempty"`
}

// NewWeaviateThingsListOK creates WeaviateThingsListOK with default headers values
func NewWeaviateThingsListOK() *WeaviateThingsListOK {
	return &WeaviateThingsListOK{}
}

// WithPayload adds the payload to the weaviate things list o k response
func (o *WeaviateThingsListOK) WithPayload(payload *models.ThingsListResponse) *WeaviateThingsListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things list o k response
func (o *WeaviateThingsListOK) SetPayload(payload *models.ThingsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsListUnauthorizedCode is the HTTP code returned for type WeaviateThingsListUnauthorized
const WeaviateThingsListUnauthorizedCode int = 401

/*WeaviateThingsListUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsListUnauthorized
*/
type WeaviateThingsListUnauthorized struct {
}

// NewWeaviateThingsListUnauthorized creates WeaviateThingsListUnauthorized with default headers values
func NewWeaviateThingsListUnauthorized() *WeaviateThingsListUnauthorized {
	return &WeaviateThingsListUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsListUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsListForbiddenCode is the HTTP code returned for type WeaviateThingsListForbidden
const WeaviateThingsListForbiddenCode int = 403

/*WeaviateThingsListForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsListForbidden
*/
type WeaviateThingsListForbidden struct {
}

// NewWeaviateThingsListForbidden creates WeaviateThingsListForbidden with default headers values
func NewWeaviateThingsListForbidden() *WeaviateThingsListForbidden {
	return &WeaviateThingsListForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsListForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsListNotFoundCode is the HTTP code returned for type WeaviateThingsListNotFound
const WeaviateThingsListNotFoundCode int = 404

/*WeaviateThingsListNotFound Successful query result but no resource was found.

swagger:response weaviateThingsListNotFound
*/
type WeaviateThingsListNotFound struct {
}

// NewWeaviateThingsListNotFound creates WeaviateThingsListNotFound with default headers values
func NewWeaviateThingsListNotFound() *WeaviateThingsListNotFound {
	return &WeaviateThingsListNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingsListNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingsListNotImplementedCode is the HTTP code returned for type WeaviateThingsListNotImplemented
const WeaviateThingsListNotImplementedCode int = 501

/*WeaviateThingsListNotImplemented Not (yet) implemented.

swagger:response weaviateThingsListNotImplemented
*/
type WeaviateThingsListNotImplemented struct {
}

// NewWeaviateThingsListNotImplemented creates WeaviateThingsListNotImplemented with default headers values
func NewWeaviateThingsListNotImplemented() *WeaviateThingsListNotImplemented {
	return &WeaviateThingsListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
