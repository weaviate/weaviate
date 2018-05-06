/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */

package keys

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/creativesoftwarefdn/weaviate/models"
)

// WeaviateKeysChildrenGetOKCode is the HTTP code returned for type WeaviateKeysChildrenGetOK
const WeaviateKeysChildrenGetOKCode int = 200

/*WeaviateKeysChildrenGetOK Successful response.

swagger:response weaviateKeysChildrenGetOK
*/
type WeaviateKeysChildrenGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.KeyChildrenGetResponse `json:"body,omitempty"`
}

// NewWeaviateKeysChildrenGetOK creates WeaviateKeysChildrenGetOK with default headers values
func NewWeaviateKeysChildrenGetOK() *WeaviateKeysChildrenGetOK {
	return &WeaviateKeysChildrenGetOK{}
}

// WithPayload adds the payload to the weaviate keys children get o k response
func (o *WeaviateKeysChildrenGetOK) WithPayload(payload *models.KeyChildrenGetResponse) *WeaviateKeysChildrenGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate keys children get o k response
func (o *WeaviateKeysChildrenGetOK) SetPayload(payload *models.KeyChildrenGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateKeysChildrenGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateKeysChildrenGetUnauthorizedCode is the HTTP code returned for type WeaviateKeysChildrenGetUnauthorized
const WeaviateKeysChildrenGetUnauthorizedCode int = 401

/*WeaviateKeysChildrenGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateKeysChildrenGetUnauthorized
*/
type WeaviateKeysChildrenGetUnauthorized struct {
}

// NewWeaviateKeysChildrenGetUnauthorized creates WeaviateKeysChildrenGetUnauthorized with default headers values
func NewWeaviateKeysChildrenGetUnauthorized() *WeaviateKeysChildrenGetUnauthorized {
	return &WeaviateKeysChildrenGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateKeysChildrenGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateKeysChildrenGetForbiddenCode is the HTTP code returned for type WeaviateKeysChildrenGetForbidden
const WeaviateKeysChildrenGetForbiddenCode int = 403

/*WeaviateKeysChildrenGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateKeysChildrenGetForbidden
*/
type WeaviateKeysChildrenGetForbidden struct {
}

// NewWeaviateKeysChildrenGetForbidden creates WeaviateKeysChildrenGetForbidden with default headers values
func NewWeaviateKeysChildrenGetForbidden() *WeaviateKeysChildrenGetForbidden {
	return &WeaviateKeysChildrenGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateKeysChildrenGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateKeysChildrenGetNotFoundCode is the HTTP code returned for type WeaviateKeysChildrenGetNotFound
const WeaviateKeysChildrenGetNotFoundCode int = 404

/*WeaviateKeysChildrenGetNotFound Successful query result but no resource was found.

swagger:response weaviateKeysChildrenGetNotFound
*/
type WeaviateKeysChildrenGetNotFound struct {
}

// NewWeaviateKeysChildrenGetNotFound creates WeaviateKeysChildrenGetNotFound with default headers values
func NewWeaviateKeysChildrenGetNotFound() *WeaviateKeysChildrenGetNotFound {
	return &WeaviateKeysChildrenGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateKeysChildrenGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateKeysChildrenGetNotImplementedCode is the HTTP code returned for type WeaviateKeysChildrenGetNotImplemented
const WeaviateKeysChildrenGetNotImplementedCode int = 501

/*WeaviateKeysChildrenGetNotImplemented Not (yet) implemented

swagger:response weaviateKeysChildrenGetNotImplemented
*/
type WeaviateKeysChildrenGetNotImplemented struct {
}

// NewWeaviateKeysChildrenGetNotImplemented creates WeaviateKeysChildrenGetNotImplemented with default headers values
func NewWeaviateKeysChildrenGetNotImplemented() *WeaviateKeysChildrenGetNotImplemented {
	return &WeaviateKeysChildrenGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateKeysChildrenGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
