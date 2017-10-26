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

// WeaviateKeyCreateAcceptedCode is the HTTP code returned for type WeaviateKeyCreateAccepted
const WeaviateKeyCreateAcceptedCode int = 202

/*WeaviateKeyCreateAccepted Successfully received.

swagger:response weaviateKeyCreateAccepted
*/
type WeaviateKeyCreateAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.KeyTokenGetResponse `json:"body,omitempty"`
}

// NewWeaviateKeyCreateAccepted creates WeaviateKeyCreateAccepted with default headers values
func NewWeaviateKeyCreateAccepted() *WeaviateKeyCreateAccepted {
	return &WeaviateKeyCreateAccepted{}
}

// WithPayload adds the payload to the weaviate key create accepted response
func (o *WeaviateKeyCreateAccepted) WithPayload(payload *models.KeyTokenGetResponse) *WeaviateKeyCreateAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate key create accepted response
func (o *WeaviateKeyCreateAccepted) SetPayload(payload *models.KeyTokenGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateKeyCreateAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateKeyCreateUnauthorizedCode is the HTTP code returned for type WeaviateKeyCreateUnauthorized
const WeaviateKeyCreateUnauthorizedCode int = 401

/*WeaviateKeyCreateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateKeyCreateUnauthorized
*/
type WeaviateKeyCreateUnauthorized struct {
}

// NewWeaviateKeyCreateUnauthorized creates WeaviateKeyCreateUnauthorized with default headers values
func NewWeaviateKeyCreateUnauthorized() *WeaviateKeyCreateUnauthorized {
	return &WeaviateKeyCreateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateKeyCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateKeyCreateUnprocessableEntityCode is the HTTP code returned for type WeaviateKeyCreateUnprocessableEntity
const WeaviateKeyCreateUnprocessableEntityCode int = 422

/*WeaviateKeyCreateUnprocessableEntity Request body contains well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response weaviateKeyCreateUnprocessableEntity
*/
type WeaviateKeyCreateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewWeaviateKeyCreateUnprocessableEntity creates WeaviateKeyCreateUnprocessableEntity with default headers values
func NewWeaviateKeyCreateUnprocessableEntity() *WeaviateKeyCreateUnprocessableEntity {
	return &WeaviateKeyCreateUnprocessableEntity{}
}

// WithPayload adds the payload to the weaviate key create unprocessable entity response
func (o *WeaviateKeyCreateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *WeaviateKeyCreateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate key create unprocessable entity response
func (o *WeaviateKeyCreateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateKeyCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateKeyCreateNotImplementedCode is the HTTP code returned for type WeaviateKeyCreateNotImplemented
const WeaviateKeyCreateNotImplementedCode int = 501

/*WeaviateKeyCreateNotImplemented Not (yet) implemented.

swagger:response weaviateKeyCreateNotImplemented
*/
type WeaviateKeyCreateNotImplemented struct {
}

// NewWeaviateKeyCreateNotImplemented creates WeaviateKeyCreateNotImplemented with default headers values
func NewWeaviateKeyCreateNotImplemented() *WeaviateKeyCreateNotImplemented {
	return &WeaviateKeyCreateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateKeyCreateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
