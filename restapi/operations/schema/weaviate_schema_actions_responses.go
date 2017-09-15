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

package schema

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateSchemaActionsOKCode is the HTTP code returned for type WeaviateSchemaActionsOK
const WeaviateSchemaActionsOKCode int = 200

/*WeaviateSchemaActionsOK Successful response.

swagger:response weaviateSchemaActionsOK
*/
type WeaviateSchemaActionsOK struct {

	/*
	  In: Body
	*/
	Payload runtime.File `json:"body,omitempty"`
}

// NewWeaviateSchemaActionsOK creates WeaviateSchemaActionsOK with default headers values
func NewWeaviateSchemaActionsOK() *WeaviateSchemaActionsOK {
	return &WeaviateSchemaActionsOK{}
}

// WithPayload adds the payload to the weaviate schema actions o k response
func (o *WeaviateSchemaActionsOK) WithPayload(payload runtime.File) *WeaviateSchemaActionsOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate schema actions o k response
func (o *WeaviateSchemaActionsOK) SetPayload(payload runtime.File) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateSchemaActionsOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	payload := o.Payload
	if err := producer.Produce(rw, payload); err != nil {
		panic(err) // let the recovery middleware deal with this
	}

}

// WeaviateSchemaActionsUnauthorizedCode is the HTTP code returned for type WeaviateSchemaActionsUnauthorized
const WeaviateSchemaActionsUnauthorizedCode int = 401

/*WeaviateSchemaActionsUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateSchemaActionsUnauthorized
*/
type WeaviateSchemaActionsUnauthorized struct {
}

// NewWeaviateSchemaActionsUnauthorized creates WeaviateSchemaActionsUnauthorized with default headers values
func NewWeaviateSchemaActionsUnauthorized() *WeaviateSchemaActionsUnauthorized {
	return &WeaviateSchemaActionsUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateSchemaActionsUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateSchemaActionsNotImplementedCode is the HTTP code returned for type WeaviateSchemaActionsNotImplemented
const WeaviateSchemaActionsNotImplementedCode int = 501

/*WeaviateSchemaActionsNotImplemented Not (yet) implemented

swagger:response weaviateSchemaActionsNotImplemented
*/
type WeaviateSchemaActionsNotImplemented struct {
}

// NewWeaviateSchemaActionsNotImplemented creates WeaviateSchemaActionsNotImplemented with default headers values
func NewWeaviateSchemaActionsNotImplemented() *WeaviateSchemaActionsNotImplemented {
	return &WeaviateSchemaActionsNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateSchemaActionsNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
