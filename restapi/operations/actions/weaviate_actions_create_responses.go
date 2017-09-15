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

package actions

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateActionsCreateAcceptedCode is the HTTP code returned for type WeaviateActionsCreateAccepted
const WeaviateActionsCreateAcceptedCode int = 202

/*WeaviateActionsCreateAccepted Successfully received.

swagger:response weaviateActionsCreateAccepted
*/
type WeaviateActionsCreateAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.ActionGetResponse `json:"body,omitempty"`
}

// NewWeaviateActionsCreateAccepted creates WeaviateActionsCreateAccepted with default headers values
func NewWeaviateActionsCreateAccepted() *WeaviateActionsCreateAccepted {
	return &WeaviateActionsCreateAccepted{}
}

// WithPayload adds the payload to the weaviate actions create accepted response
func (o *WeaviateActionsCreateAccepted) WithPayload(payload *models.ActionGetResponse) *WeaviateActionsCreateAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate actions create accepted response
func (o *WeaviateActionsCreateAccepted) SetPayload(payload *models.ActionGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateActionsCreateAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateActionsCreateUnauthorizedCode is the HTTP code returned for type WeaviateActionsCreateUnauthorized
const WeaviateActionsCreateUnauthorizedCode int = 401

/*WeaviateActionsCreateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateActionsCreateUnauthorized
*/
type WeaviateActionsCreateUnauthorized struct {
}

// NewWeaviateActionsCreateUnauthorized creates WeaviateActionsCreateUnauthorized with default headers values
func NewWeaviateActionsCreateUnauthorized() *WeaviateActionsCreateUnauthorized {
	return &WeaviateActionsCreateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateActionsCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateActionsCreateForbiddenCode is the HTTP code returned for type WeaviateActionsCreateForbidden
const WeaviateActionsCreateForbiddenCode int = 403

/*WeaviateActionsCreateForbidden The used API-key has insufficient permissions.

swagger:response weaviateActionsCreateForbidden
*/
type WeaviateActionsCreateForbidden struct {
}

// NewWeaviateActionsCreateForbidden creates WeaviateActionsCreateForbidden with default headers values
func NewWeaviateActionsCreateForbidden() *WeaviateActionsCreateForbidden {
	return &WeaviateActionsCreateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateActionsCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateActionsCreateUnprocessableEntityCode is the HTTP code returned for type WeaviateActionsCreateUnprocessableEntity
const WeaviateActionsCreateUnprocessableEntityCode int = 422

/*WeaviateActionsCreateUnprocessableEntity Request body contains well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the class is defined in the configuration file?

swagger:response weaviateActionsCreateUnprocessableEntity
*/
type WeaviateActionsCreateUnprocessableEntity struct {
}

// NewWeaviateActionsCreateUnprocessableEntity creates WeaviateActionsCreateUnprocessableEntity with default headers values
func NewWeaviateActionsCreateUnprocessableEntity() *WeaviateActionsCreateUnprocessableEntity {
	return &WeaviateActionsCreateUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateActionsCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateActionsCreateNotImplementedCode is the HTTP code returned for type WeaviateActionsCreateNotImplemented
const WeaviateActionsCreateNotImplementedCode int = 501

/*WeaviateActionsCreateNotImplemented Not (yet) implemented.

swagger:response weaviateActionsCreateNotImplemented
*/
type WeaviateActionsCreateNotImplemented struct {
}

// NewWeaviateActionsCreateNotImplemented creates WeaviateActionsCreateNotImplemented with default headers values
func NewWeaviateActionsCreateNotImplemented() *WeaviateActionsCreateNotImplemented {
	return &WeaviateActionsCreateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateActionsCreateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
