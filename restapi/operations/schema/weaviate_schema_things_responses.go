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

// WeaviateSchemaThingsOKCode is the HTTP code returned for type WeaviateSchemaThingsOK
const WeaviateSchemaThingsOKCode int = 200

/*WeaviateSchemaThingsOK Successful response.

swagger:response weaviateSchemaThingsOK
*/
type WeaviateSchemaThingsOK struct {

	/*
	  In: Body
	*/
	Payload runtime.File `json:"body,omitempty"`
}

// NewWeaviateSchemaThingsOK creates WeaviateSchemaThingsOK with default headers values
func NewWeaviateSchemaThingsOK() *WeaviateSchemaThingsOK {
	return &WeaviateSchemaThingsOK{}
}

// WithPayload adds the payload to the weaviate schema things o k response
func (o *WeaviateSchemaThingsOK) WithPayload(payload runtime.File) *WeaviateSchemaThingsOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate schema things o k response
func (o *WeaviateSchemaThingsOK) SetPayload(payload runtime.File) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateSchemaThingsOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	payload := o.Payload
	if err := producer.Produce(rw, payload); err != nil {
		panic(err) // let the recovery middleware deal with this
	}

}

// WeaviateSchemaThingsUnauthorizedCode is the HTTP code returned for type WeaviateSchemaThingsUnauthorized
const WeaviateSchemaThingsUnauthorizedCode int = 401

/*WeaviateSchemaThingsUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateSchemaThingsUnauthorized
*/
type WeaviateSchemaThingsUnauthorized struct {
}

// NewWeaviateSchemaThingsUnauthorized creates WeaviateSchemaThingsUnauthorized with default headers values
func NewWeaviateSchemaThingsUnauthorized() *WeaviateSchemaThingsUnauthorized {
	return &WeaviateSchemaThingsUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateSchemaThingsUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateSchemaThingsNotImplementedCode is the HTTP code returned for type WeaviateSchemaThingsNotImplemented
const WeaviateSchemaThingsNotImplementedCode int = 501

/*WeaviateSchemaThingsNotImplemented Not (yet) implemented

swagger:response weaviateSchemaThingsNotImplemented
*/
type WeaviateSchemaThingsNotImplemented struct {
}

// NewWeaviateSchemaThingsNotImplemented creates WeaviateSchemaThingsNotImplemented with default headers values
func NewWeaviateSchemaThingsNotImplemented() *WeaviateSchemaThingsNotImplemented {
	return &WeaviateSchemaThingsNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateSchemaThingsNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
