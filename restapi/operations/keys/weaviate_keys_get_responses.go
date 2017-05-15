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

// WeaviateKeysGetOKCode is the HTTP code returned for type WeaviateKeysGetOK
const WeaviateKeysGetOKCode int = 200

/*WeaviateKeysGetOK Successful response.

swagger:response weaviateKeysGetOK
*/
type WeaviateKeysGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.Key `json:"body,omitempty"`
}

// NewWeaviateKeysGetOK creates WeaviateKeysGetOK with default headers values
func NewWeaviateKeysGetOK() *WeaviateKeysGetOK {
	return &WeaviateKeysGetOK{}
}

// WithPayload adds the payload to the weaviate keys get o k response
func (o *WeaviateKeysGetOK) WithPayload(payload *models.Key) *WeaviateKeysGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate keys get o k response
func (o *WeaviateKeysGetOK) SetPayload(payload *models.Key) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateKeysGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateKeysGetUnauthorizedCode is the HTTP code returned for type WeaviateKeysGetUnauthorized
const WeaviateKeysGetUnauthorizedCode int = 401

/*WeaviateKeysGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateKeysGetUnauthorized
*/
type WeaviateKeysGetUnauthorized struct {
}

// NewWeaviateKeysGetUnauthorized creates WeaviateKeysGetUnauthorized with default headers values
func NewWeaviateKeysGetUnauthorized() *WeaviateKeysGetUnauthorized {
	return &WeaviateKeysGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateKeysGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateKeysGetForbiddenCode is the HTTP code returned for type WeaviateKeysGetForbidden
const WeaviateKeysGetForbiddenCode int = 403

/*WeaviateKeysGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateKeysGetForbidden
*/
type WeaviateKeysGetForbidden struct {
}

// NewWeaviateKeysGetForbidden creates WeaviateKeysGetForbidden with default headers values
func NewWeaviateKeysGetForbidden() *WeaviateKeysGetForbidden {
	return &WeaviateKeysGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateKeysGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateKeysGetNotFoundCode is the HTTP code returned for type WeaviateKeysGetNotFound
const WeaviateKeysGetNotFoundCode int = 404

/*WeaviateKeysGetNotFound Successful query result but no resource was found.

swagger:response weaviateKeysGetNotFound
*/
type WeaviateKeysGetNotFound struct {
}

// NewWeaviateKeysGetNotFound creates WeaviateKeysGetNotFound with default headers values
func NewWeaviateKeysGetNotFound() *WeaviateKeysGetNotFound {
	return &WeaviateKeysGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateKeysGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateKeysGetNotImplementedCode is the HTTP code returned for type WeaviateKeysGetNotImplemented
const WeaviateKeysGetNotImplementedCode int = 501

/*WeaviateKeysGetNotImplemented Not (yet) implemented.

swagger:response weaviateKeysGetNotImplemented
*/
type WeaviateKeysGetNotImplemented struct {
}

// NewWeaviateKeysGetNotImplemented creates WeaviateKeysGetNotImplemented with default headers values
func NewWeaviateKeysGetNotImplemented() *WeaviateKeysGetNotImplemented {
	return &WeaviateKeysGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateKeysGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
