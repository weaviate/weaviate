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

// WeaviateChildrenGetOKCode is the HTTP code returned for type WeaviateChildrenGetOK
const WeaviateChildrenGetOKCode int = 200

/*WeaviateChildrenGetOK Successful response.

swagger:response weaviateChildrenGetOK
*/
type WeaviateChildrenGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.KeyChildren `json:"body,omitempty"`
}

// NewWeaviateChildrenGetOK creates WeaviateChildrenGetOK with default headers values
func NewWeaviateChildrenGetOK() *WeaviateChildrenGetOK {
	return &WeaviateChildrenGetOK{}
}

// WithPayload adds the payload to the weaviate children get o k response
func (o *WeaviateChildrenGetOK) WithPayload(payload *models.KeyChildren) *WeaviateChildrenGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate children get o k response
func (o *WeaviateChildrenGetOK) SetPayload(payload *models.KeyChildren) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateChildrenGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateChildrenGetUnauthorizedCode is the HTTP code returned for type WeaviateChildrenGetUnauthorized
const WeaviateChildrenGetUnauthorizedCode int = 401

/*WeaviateChildrenGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateChildrenGetUnauthorized
*/
type WeaviateChildrenGetUnauthorized struct {
}

// NewWeaviateChildrenGetUnauthorized creates WeaviateChildrenGetUnauthorized with default headers values
func NewWeaviateChildrenGetUnauthorized() *WeaviateChildrenGetUnauthorized {
	return &WeaviateChildrenGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateChildrenGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateChildrenGetForbiddenCode is the HTTP code returned for type WeaviateChildrenGetForbidden
const WeaviateChildrenGetForbiddenCode int = 403

/*WeaviateChildrenGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateChildrenGetForbidden
*/
type WeaviateChildrenGetForbidden struct {
}

// NewWeaviateChildrenGetForbidden creates WeaviateChildrenGetForbidden with default headers values
func NewWeaviateChildrenGetForbidden() *WeaviateChildrenGetForbidden {
	return &WeaviateChildrenGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateChildrenGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateChildrenGetNotFoundCode is the HTTP code returned for type WeaviateChildrenGetNotFound
const WeaviateChildrenGetNotFoundCode int = 404

/*WeaviateChildrenGetNotFound Successful query result but no resource was found.

swagger:response weaviateChildrenGetNotFound
*/
type WeaviateChildrenGetNotFound struct {
}

// NewWeaviateChildrenGetNotFound creates WeaviateChildrenGetNotFound with default headers values
func NewWeaviateChildrenGetNotFound() *WeaviateChildrenGetNotFound {
	return &WeaviateChildrenGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateChildrenGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateChildrenGetNotImplementedCode is the HTTP code returned for type WeaviateChildrenGetNotImplemented
const WeaviateChildrenGetNotImplementedCode int = 501

/*WeaviateChildrenGetNotImplemented Not (yet) implemented

swagger:response weaviateChildrenGetNotImplemented
*/
type WeaviateChildrenGetNotImplemented struct {
}

// NewWeaviateChildrenGetNotImplemented creates WeaviateChildrenGetNotImplemented with default headers values
func NewWeaviateChildrenGetNotImplemented() *WeaviateChildrenGetNotImplemented {
	return &WeaviateChildrenGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateChildrenGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
