/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/develop/LICENSE.md
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

// WeaviateKeysMeChildrenGetOKCode is the HTTP code returned for type WeaviateKeysMeChildrenGetOK
const WeaviateKeysMeChildrenGetOKCode int = 200

/*WeaviateKeysMeChildrenGetOK Successful response.

swagger:response weaviateKeysMeChildrenGetOK
*/
type WeaviateKeysMeChildrenGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.KeyChildrenGetResponse `json:"body,omitempty"`
}

// NewWeaviateKeysMeChildrenGetOK creates WeaviateKeysMeChildrenGetOK with default headers values
func NewWeaviateKeysMeChildrenGetOK() *WeaviateKeysMeChildrenGetOK {
	return &WeaviateKeysMeChildrenGetOK{}
}

// WithPayload adds the payload to the weaviate keys me children get o k response
func (o *WeaviateKeysMeChildrenGetOK) WithPayload(payload *models.KeyChildrenGetResponse) *WeaviateKeysMeChildrenGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate keys me children get o k response
func (o *WeaviateKeysMeChildrenGetOK) SetPayload(payload *models.KeyChildrenGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateKeysMeChildrenGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateKeysMeChildrenGetUnauthorizedCode is the HTTP code returned for type WeaviateKeysMeChildrenGetUnauthorized
const WeaviateKeysMeChildrenGetUnauthorizedCode int = 401

/*WeaviateKeysMeChildrenGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateKeysMeChildrenGetUnauthorized
*/
type WeaviateKeysMeChildrenGetUnauthorized struct {
}

// NewWeaviateKeysMeChildrenGetUnauthorized creates WeaviateKeysMeChildrenGetUnauthorized with default headers values
func NewWeaviateKeysMeChildrenGetUnauthorized() *WeaviateKeysMeChildrenGetUnauthorized {
	return &WeaviateKeysMeChildrenGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeChildrenGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateKeysMeChildrenGetNotFoundCode is the HTTP code returned for type WeaviateKeysMeChildrenGetNotFound
const WeaviateKeysMeChildrenGetNotFoundCode int = 404

/*WeaviateKeysMeChildrenGetNotFound Successful query result but no resource was found.

swagger:response weaviateKeysMeChildrenGetNotFound
*/
type WeaviateKeysMeChildrenGetNotFound struct {
}

// NewWeaviateKeysMeChildrenGetNotFound creates WeaviateKeysMeChildrenGetNotFound with default headers values
func NewWeaviateKeysMeChildrenGetNotFound() *WeaviateKeysMeChildrenGetNotFound {
	return &WeaviateKeysMeChildrenGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeChildrenGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateKeysMeChildrenGetNotImplementedCode is the HTTP code returned for type WeaviateKeysMeChildrenGetNotImplemented
const WeaviateKeysMeChildrenGetNotImplementedCode int = 501

/*WeaviateKeysMeChildrenGetNotImplemented Not (yet) implemented

swagger:response weaviateKeysMeChildrenGetNotImplemented
*/
type WeaviateKeysMeChildrenGetNotImplemented struct {
}

// NewWeaviateKeysMeChildrenGetNotImplemented creates WeaviateKeysMeChildrenGetNotImplemented with default headers values
func NewWeaviateKeysMeChildrenGetNotImplemented() *WeaviateKeysMeChildrenGetNotImplemented {
	return &WeaviateKeysMeChildrenGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateKeysMeChildrenGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
