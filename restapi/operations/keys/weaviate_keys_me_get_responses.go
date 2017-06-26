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
 package keys




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateKeysMeGetOKCode is the HTTP code returned for type WeaviateKeysMeGetOK
const WeaviateKeysMeGetOKCode int = 200

/*WeaviateKeysMeGetOK Successful response.

swagger:response weaviateKeysMeGetOK
*/
type WeaviateKeysMeGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.KeyGetResponse `json:"body,omitempty"`
}

// NewWeaviateKeysMeGetOK creates WeaviateKeysMeGetOK with default headers values
func NewWeaviateKeysMeGetOK() *WeaviateKeysMeGetOK {
	return &WeaviateKeysMeGetOK{}
}

// WithPayload adds the payload to the weaviate keys me get o k response
func (o *WeaviateKeysMeGetOK) WithPayload(payload *models.KeyGetResponse) *WeaviateKeysMeGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate keys me get o k response
func (o *WeaviateKeysMeGetOK) SetPayload(payload *models.KeyGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateKeysMeGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateKeysMeGetUnauthorizedCode is the HTTP code returned for type WeaviateKeysMeGetUnauthorized
const WeaviateKeysMeGetUnauthorizedCode int = 401

/*WeaviateKeysMeGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateKeysMeGetUnauthorized
*/
type WeaviateKeysMeGetUnauthorized struct {
}

// NewWeaviateKeysMeGetUnauthorized creates WeaviateKeysMeGetUnauthorized with default headers values
func NewWeaviateKeysMeGetUnauthorized() *WeaviateKeysMeGetUnauthorized {
	return &WeaviateKeysMeGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateKeysMeGetForbiddenCode is the HTTP code returned for type WeaviateKeysMeGetForbidden
const WeaviateKeysMeGetForbiddenCode int = 403

/*WeaviateKeysMeGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateKeysMeGetForbidden
*/
type WeaviateKeysMeGetForbidden struct {
}

// NewWeaviateKeysMeGetForbidden creates WeaviateKeysMeGetForbidden with default headers values
func NewWeaviateKeysMeGetForbidden() *WeaviateKeysMeGetForbidden {
	return &WeaviateKeysMeGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateKeysMeGetNotFoundCode is the HTTP code returned for type WeaviateKeysMeGetNotFound
const WeaviateKeysMeGetNotFoundCode int = 404

/*WeaviateKeysMeGetNotFound Successful query result but no resource was found.

swagger:response weaviateKeysMeGetNotFound
*/
type WeaviateKeysMeGetNotFound struct {
}

// NewWeaviateKeysMeGetNotFound creates WeaviateKeysMeGetNotFound with default headers values
func NewWeaviateKeysMeGetNotFound() *WeaviateKeysMeGetNotFound {
	return &WeaviateKeysMeGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateKeysMeGetNotImplementedCode is the HTTP code returned for type WeaviateKeysMeGetNotImplemented
const WeaviateKeysMeGetNotImplementedCode int = 501

/*WeaviateKeysMeGetNotImplemented Not (yet) implemented.

swagger:response weaviateKeysMeGetNotImplemented
*/
type WeaviateKeysMeGetNotImplemented struct {
}

// NewWeaviateKeysMeGetNotImplemented creates WeaviateKeysMeGetNotImplemented with default headers values
func NewWeaviateKeysMeGetNotImplemented() *WeaviateKeysMeGetNotImplemented {
	return &WeaviateKeysMeGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
