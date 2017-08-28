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

 
 

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateThingsGetOKCode is the HTTP code returned for type WeaviateThingsGetOK
const WeaviateThingsGetOKCode int = 200

/*WeaviateThingsGetOK Successful response.

swagger:response weaviateThingsGetOK
*/
type WeaviateThingsGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.ThingGetResponse `json:"body,omitempty"`
}

// NewWeaviateThingsGetOK creates WeaviateThingsGetOK with default headers values
func NewWeaviateThingsGetOK() *WeaviateThingsGetOK {
	return &WeaviateThingsGetOK{}
}

// WithPayload adds the payload to the weaviate things get o k response
func (o *WeaviateThingsGetOK) WithPayload(payload *models.ThingGetResponse) *WeaviateThingsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things get o k response
func (o *WeaviateThingsGetOK) SetPayload(payload *models.ThingGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsGetUnauthorizedCode is the HTTP code returned for type WeaviateThingsGetUnauthorized
const WeaviateThingsGetUnauthorizedCode int = 401

/*WeaviateThingsGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsGetUnauthorized
*/
type WeaviateThingsGetUnauthorized struct {
}

// NewWeaviateThingsGetUnauthorized creates WeaviateThingsGetUnauthorized with default headers values
func NewWeaviateThingsGetUnauthorized() *WeaviateThingsGetUnauthorized {
	return &WeaviateThingsGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsGetForbiddenCode is the HTTP code returned for type WeaviateThingsGetForbidden
const WeaviateThingsGetForbiddenCode int = 403

/*WeaviateThingsGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsGetForbidden
*/
type WeaviateThingsGetForbidden struct {
}

// NewWeaviateThingsGetForbidden creates WeaviateThingsGetForbidden with default headers values
func NewWeaviateThingsGetForbidden() *WeaviateThingsGetForbidden {
	return &WeaviateThingsGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsGetNotFoundCode is the HTTP code returned for type WeaviateThingsGetNotFound
const WeaviateThingsGetNotFoundCode int = 404

/*WeaviateThingsGetNotFound Successful query result but no resource was found.

swagger:response weaviateThingsGetNotFound
*/
type WeaviateThingsGetNotFound struct {
}

// NewWeaviateThingsGetNotFound creates WeaviateThingsGetNotFound with default headers values
func NewWeaviateThingsGetNotFound() *WeaviateThingsGetNotFound {
	return &WeaviateThingsGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingsGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingsGetNotImplementedCode is the HTTP code returned for type WeaviateThingsGetNotImplemented
const WeaviateThingsGetNotImplementedCode int = 501

/*WeaviateThingsGetNotImplemented Not (yet) implemented.

swagger:response weaviateThingsGetNotImplemented
*/
type WeaviateThingsGetNotImplemented struct {
}

// NewWeaviateThingsGetNotImplemented creates WeaviateThingsGetNotImplemented with default headers values
func NewWeaviateThingsGetNotImplemented() *WeaviateThingsGetNotImplemented {
	return &WeaviateThingsGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
