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

package things

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/creativesoftwarefdn/weaviate/models"
)

// WeaviateThingsUpdateAcceptedCode is the HTTP code returned for type WeaviateThingsUpdateAccepted
const WeaviateThingsUpdateAcceptedCode int = 202

/*WeaviateThingsUpdateAccepted Successfully received.

swagger:response weaviateThingsUpdateAccepted
*/
type WeaviateThingsUpdateAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.ThingGetResponse `json:"body,omitempty"`
}

// NewWeaviateThingsUpdateAccepted creates WeaviateThingsUpdateAccepted with default headers values
func NewWeaviateThingsUpdateAccepted() *WeaviateThingsUpdateAccepted {
	return &WeaviateThingsUpdateAccepted{}
}

// WithPayload adds the payload to the weaviate things update accepted response
func (o *WeaviateThingsUpdateAccepted) WithPayload(payload *models.ThingGetResponse) *WeaviateThingsUpdateAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things update accepted response
func (o *WeaviateThingsUpdateAccepted) SetPayload(payload *models.ThingGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsUpdateUnauthorizedCode is the HTTP code returned for type WeaviateThingsUpdateUnauthorized
const WeaviateThingsUpdateUnauthorizedCode int = 401

/*WeaviateThingsUpdateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsUpdateUnauthorized
*/
type WeaviateThingsUpdateUnauthorized struct {
}

// NewWeaviateThingsUpdateUnauthorized creates WeaviateThingsUpdateUnauthorized with default headers values
func NewWeaviateThingsUpdateUnauthorized() *WeaviateThingsUpdateUnauthorized {
	return &WeaviateThingsUpdateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsUpdateForbiddenCode is the HTTP code returned for type WeaviateThingsUpdateForbidden
const WeaviateThingsUpdateForbiddenCode int = 403

/*WeaviateThingsUpdateForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsUpdateForbidden
*/
type WeaviateThingsUpdateForbidden struct {
}

// NewWeaviateThingsUpdateForbidden creates WeaviateThingsUpdateForbidden with default headers values
func NewWeaviateThingsUpdateForbidden() *WeaviateThingsUpdateForbidden {
	return &WeaviateThingsUpdateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsUpdateNotFoundCode is the HTTP code returned for type WeaviateThingsUpdateNotFound
const WeaviateThingsUpdateNotFoundCode int = 404

/*WeaviateThingsUpdateNotFound Successful query result but no resource was found.

swagger:response weaviateThingsUpdateNotFound
*/
type WeaviateThingsUpdateNotFound struct {
}

// NewWeaviateThingsUpdateNotFound creates WeaviateThingsUpdateNotFound with default headers values
func NewWeaviateThingsUpdateNotFound() *WeaviateThingsUpdateNotFound {
	return &WeaviateThingsUpdateNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingsUpdateUnprocessableEntityCode is the HTTP code returned for type WeaviateThingsUpdateUnprocessableEntity
const WeaviateThingsUpdateUnprocessableEntityCode int = 422

/*WeaviateThingsUpdateUnprocessableEntity Request body contains well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response weaviateThingsUpdateUnprocessableEntity
*/
type WeaviateThingsUpdateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewWeaviateThingsUpdateUnprocessableEntity creates WeaviateThingsUpdateUnprocessableEntity with default headers values
func NewWeaviateThingsUpdateUnprocessableEntity() *WeaviateThingsUpdateUnprocessableEntity {
	return &WeaviateThingsUpdateUnprocessableEntity{}
}

// WithPayload adds the payload to the weaviate things update unprocessable entity response
func (o *WeaviateThingsUpdateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *WeaviateThingsUpdateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things update unprocessable entity response
func (o *WeaviateThingsUpdateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsUpdateNotImplementedCode is the HTTP code returned for type WeaviateThingsUpdateNotImplemented
const WeaviateThingsUpdateNotImplementedCode int = 501

/*WeaviateThingsUpdateNotImplemented Not (yet) implemented.

swagger:response weaviateThingsUpdateNotImplemented
*/
type WeaviateThingsUpdateNotImplemented struct {
}

// NewWeaviateThingsUpdateNotImplemented creates WeaviateThingsUpdateNotImplemented with default headers values
func NewWeaviateThingsUpdateNotImplemented() *WeaviateThingsUpdateNotImplemented {
	return &WeaviateThingsUpdateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsUpdateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
