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

package things

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateThingsCreateAcceptedCode is the HTTP code returned for type WeaviateThingsCreateAccepted
const WeaviateThingsCreateAcceptedCode int = 202

/*WeaviateThingsCreateAccepted Successfully received.

swagger:response weaviateThingsCreateAccepted
*/
type WeaviateThingsCreateAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.ThingGetResponse `json:"body,omitempty"`
}

// NewWeaviateThingsCreateAccepted creates WeaviateThingsCreateAccepted with default headers values
func NewWeaviateThingsCreateAccepted() *WeaviateThingsCreateAccepted {
	return &WeaviateThingsCreateAccepted{}
}

// WithPayload adds the payload to the weaviate things create accepted response
func (o *WeaviateThingsCreateAccepted) WithPayload(payload *models.ThingGetResponse) *WeaviateThingsCreateAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things create accepted response
func (o *WeaviateThingsCreateAccepted) SetPayload(payload *models.ThingGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsCreateAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsCreateUnauthorizedCode is the HTTP code returned for type WeaviateThingsCreateUnauthorized
const WeaviateThingsCreateUnauthorizedCode int = 401

/*WeaviateThingsCreateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsCreateUnauthorized
*/
type WeaviateThingsCreateUnauthorized struct {
}

// NewWeaviateThingsCreateUnauthorized creates WeaviateThingsCreateUnauthorized with default headers values
func NewWeaviateThingsCreateUnauthorized() *WeaviateThingsCreateUnauthorized {
	return &WeaviateThingsCreateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsCreateForbiddenCode is the HTTP code returned for type WeaviateThingsCreateForbidden
const WeaviateThingsCreateForbiddenCode int = 403

/*WeaviateThingsCreateForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsCreateForbidden
*/
type WeaviateThingsCreateForbidden struct {
}

// NewWeaviateThingsCreateForbidden creates WeaviateThingsCreateForbidden with default headers values
func NewWeaviateThingsCreateForbidden() *WeaviateThingsCreateForbidden {
	return &WeaviateThingsCreateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsCreateUnprocessableEntityCode is the HTTP code returned for type WeaviateThingsCreateUnprocessableEntity
const WeaviateThingsCreateUnprocessableEntityCode int = 422

/*WeaviateThingsCreateUnprocessableEntity Request body contains well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response weaviateThingsCreateUnprocessableEntity
*/
type WeaviateThingsCreateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewWeaviateThingsCreateUnprocessableEntity creates WeaviateThingsCreateUnprocessableEntity with default headers values
func NewWeaviateThingsCreateUnprocessableEntity() *WeaviateThingsCreateUnprocessableEntity {
	return &WeaviateThingsCreateUnprocessableEntity{}
}

// WithPayload adds the payload to the weaviate things create unprocessable entity response
func (o *WeaviateThingsCreateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *WeaviateThingsCreateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things create unprocessable entity response
func (o *WeaviateThingsCreateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsCreateNotImplementedCode is the HTTP code returned for type WeaviateThingsCreateNotImplemented
const WeaviateThingsCreateNotImplementedCode int = 501

/*WeaviateThingsCreateNotImplemented Not (yet) implemented.

swagger:response weaviateThingsCreateNotImplemented
*/
type WeaviateThingsCreateNotImplemented struct {
}

// NewWeaviateThingsCreateNotImplemented creates WeaviateThingsCreateNotImplemented with default headers values
func NewWeaviateThingsCreateNotImplemented() *WeaviateThingsCreateNotImplemented {
	return &WeaviateThingsCreateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsCreateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
