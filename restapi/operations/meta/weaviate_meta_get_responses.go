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

package meta

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateMetaGetOKCode is the HTTP code returned for type WeaviateMetaGetOK
const WeaviateMetaGetOKCode int = 200

/*WeaviateMetaGetOK Successful response.

swagger:response weaviateMetaGetOK
*/
type WeaviateMetaGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.Meta `json:"body,omitempty"`
}

// NewWeaviateMetaGetOK creates WeaviateMetaGetOK with default headers values
func NewWeaviateMetaGetOK() *WeaviateMetaGetOK {
	return &WeaviateMetaGetOK{}
}

// WithPayload adds the payload to the weaviate meta get o k response
func (o *WeaviateMetaGetOK) WithPayload(payload *models.Meta) *WeaviateMetaGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate meta get o k response
func (o *WeaviateMetaGetOK) SetPayload(payload *models.Meta) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateMetaGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateMetaGetUnauthorizedCode is the HTTP code returned for type WeaviateMetaGetUnauthorized
const WeaviateMetaGetUnauthorizedCode int = 401

/*WeaviateMetaGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateMetaGetUnauthorized
*/
type WeaviateMetaGetUnauthorized struct {
}

// NewWeaviateMetaGetUnauthorized creates WeaviateMetaGetUnauthorized with default headers values
func NewWeaviateMetaGetUnauthorized() *WeaviateMetaGetUnauthorized {
	return &WeaviateMetaGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateMetaGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateMetaGetNotImplementedCode is the HTTP code returned for type WeaviateMetaGetNotImplemented
const WeaviateMetaGetNotImplementedCode int = 501

/*WeaviateMetaGetNotImplemented Not (yet) implemented

swagger:response weaviateMetaGetNotImplemented
*/
type WeaviateMetaGetNotImplemented struct {
}

// NewWeaviateMetaGetNotImplemented creates WeaviateMetaGetNotImplemented with default headers values
func NewWeaviateMetaGetNotImplemented() *WeaviateMetaGetNotImplemented {
	return &WeaviateMetaGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateMetaGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
