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

// WeaviateCommandsGetQueueOKCode is the HTTP code returned for type WeaviateCommandsGetQueueOK
const WeaviateCommandsGetQueueOKCode int = 200

/*WeaviateCommandsGetQueueOK Successful response.

swagger:response weaviateCommandsGetQueueOK
*/
type WeaviateCommandsGetQueueOK struct {

	/*
	  In: Body
	*/
	Payload *models.CommandsQueueResponse `json:"body,omitempty"`
}

// NewWeaviateCommandsGetQueueOK creates WeaviateCommandsGetQueueOK with default headers values
func NewWeaviateCommandsGetQueueOK() *WeaviateCommandsGetQueueOK {
	return &WeaviateCommandsGetQueueOK{}
}

// WithPayload adds the payload to the weaviate commands get queue o k response
func (o *WeaviateCommandsGetQueueOK) WithPayload(payload *models.CommandsQueueResponse) *WeaviateCommandsGetQueueOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate commands get queue o k response
func (o *WeaviateCommandsGetQueueOK) SetPayload(payload *models.CommandsQueueResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateCommandsGetQueueOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateCommandsGetQueueUnauthorizedCode is the HTTP code returned for type WeaviateCommandsGetQueueUnauthorized
const WeaviateCommandsGetQueueUnauthorizedCode int = 401

/*WeaviateCommandsGetQueueUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateCommandsGetQueueUnauthorized
*/
type WeaviateCommandsGetQueueUnauthorized struct {
}

// NewWeaviateCommandsGetQueueUnauthorized creates WeaviateCommandsGetQueueUnauthorized with default headers values
func NewWeaviateCommandsGetQueueUnauthorized() *WeaviateCommandsGetQueueUnauthorized {
	return &WeaviateCommandsGetQueueUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateCommandsGetQueueUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateCommandsGetQueueForbiddenCode is the HTTP code returned for type WeaviateCommandsGetQueueForbidden
const WeaviateCommandsGetQueueForbiddenCode int = 403

/*WeaviateCommandsGetQueueForbidden The used API-key has insufficient permissions.

swagger:response weaviateCommandsGetQueueForbidden
*/
type WeaviateCommandsGetQueueForbidden struct {
}

// NewWeaviateCommandsGetQueueForbidden creates WeaviateCommandsGetQueueForbidden with default headers values
func NewWeaviateCommandsGetQueueForbidden() *WeaviateCommandsGetQueueForbidden {
	return &WeaviateCommandsGetQueueForbidden{}
}

// WriteResponse to the client
func (o *WeaviateCommandsGetQueueForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateCommandsGetQueueNotFoundCode is the HTTP code returned for type WeaviateCommandsGetQueueNotFound
const WeaviateCommandsGetQueueNotFoundCode int = 404

/*WeaviateCommandsGetQueueNotFound Successful query result but no resource was found.

swagger:response weaviateCommandsGetQueueNotFound
*/
type WeaviateCommandsGetQueueNotFound struct {
}

// NewWeaviateCommandsGetQueueNotFound creates WeaviateCommandsGetQueueNotFound with default headers values
func NewWeaviateCommandsGetQueueNotFound() *WeaviateCommandsGetQueueNotFound {
	return &WeaviateCommandsGetQueueNotFound{}
}

// WriteResponse to the client
func (o *WeaviateCommandsGetQueueNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateCommandsGetQueueNotImplementedCode is the HTTP code returned for type WeaviateCommandsGetQueueNotImplemented
const WeaviateCommandsGetQueueNotImplementedCode int = 501

/*WeaviateCommandsGetQueueNotImplemented Not (yet) implemented.

swagger:response weaviateCommandsGetQueueNotImplemented
*/
type WeaviateCommandsGetQueueNotImplemented struct {
}

// NewWeaviateCommandsGetQueueNotImplemented creates WeaviateCommandsGetQueueNotImplemented with default headers values
func NewWeaviateCommandsGetQueueNotImplemented() *WeaviateCommandsGetQueueNotImplemented {
	return &WeaviateCommandsGetQueueNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateCommandsGetQueueNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
