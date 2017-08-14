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
   

package commands

 
 

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateCommandsCreateAcceptedCode is the HTTP code returned for type WeaviateCommandsCreateAccepted
const WeaviateCommandsCreateAcceptedCode int = 202

/*WeaviateCommandsCreateAccepted Successfully received.

swagger:response weaviateCommandsCreateAccepted
*/
type WeaviateCommandsCreateAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.CommandGetResponse `json:"body,omitempty"`
}

// NewWeaviateCommandsCreateAccepted creates WeaviateCommandsCreateAccepted with default headers values
func NewWeaviateCommandsCreateAccepted() *WeaviateCommandsCreateAccepted {
	return &WeaviateCommandsCreateAccepted{}
}

// WithPayload adds the payload to the weaviate commands create accepted response
func (o *WeaviateCommandsCreateAccepted) WithPayload(payload *models.CommandGetResponse) *WeaviateCommandsCreateAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate commands create accepted response
func (o *WeaviateCommandsCreateAccepted) SetPayload(payload *models.CommandGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateCommandsCreateAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateCommandsCreateUnauthorizedCode is the HTTP code returned for type WeaviateCommandsCreateUnauthorized
const WeaviateCommandsCreateUnauthorizedCode int = 401

/*WeaviateCommandsCreateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateCommandsCreateUnauthorized
*/
type WeaviateCommandsCreateUnauthorized struct {
}

// NewWeaviateCommandsCreateUnauthorized creates WeaviateCommandsCreateUnauthorized with default headers values
func NewWeaviateCommandsCreateUnauthorized() *WeaviateCommandsCreateUnauthorized {
	return &WeaviateCommandsCreateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateCommandsCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateCommandsCreateForbiddenCode is the HTTP code returned for type WeaviateCommandsCreateForbidden
const WeaviateCommandsCreateForbiddenCode int = 403

/*WeaviateCommandsCreateForbidden The used API-key has insufficient permissions.

swagger:response weaviateCommandsCreateForbidden
*/
type WeaviateCommandsCreateForbidden struct {
}

// NewWeaviateCommandsCreateForbidden creates WeaviateCommandsCreateForbidden with default headers values
func NewWeaviateCommandsCreateForbidden() *WeaviateCommandsCreateForbidden {
	return &WeaviateCommandsCreateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateCommandsCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateCommandsCreateUnprocessableEntityCode is the HTTP code returned for type WeaviateCommandsCreateUnprocessableEntity
const WeaviateCommandsCreateUnprocessableEntityCode int = 422

/*WeaviateCommandsCreateUnprocessableEntity Can not validate, check the body.

swagger:response weaviateCommandsCreateUnprocessableEntity
*/
type WeaviateCommandsCreateUnprocessableEntity struct {
}

// NewWeaviateCommandsCreateUnprocessableEntity creates WeaviateCommandsCreateUnprocessableEntity with default headers values
func NewWeaviateCommandsCreateUnprocessableEntity() *WeaviateCommandsCreateUnprocessableEntity {
	return &WeaviateCommandsCreateUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateCommandsCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateCommandsCreateNotImplementedCode is the HTTP code returned for type WeaviateCommandsCreateNotImplemented
const WeaviateCommandsCreateNotImplementedCode int = 501

/*WeaviateCommandsCreateNotImplemented Not (yet) implemented.

swagger:response weaviateCommandsCreateNotImplemented
*/
type WeaviateCommandsCreateNotImplemented struct {
}

// NewWeaviateCommandsCreateNotImplemented creates WeaviateCommandsCreateNotImplemented with default headers values
func NewWeaviateCommandsCreateNotImplemented() *WeaviateCommandsCreateNotImplemented {
	return &WeaviateCommandsCreateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateCommandsCreateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
