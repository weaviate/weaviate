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

// WeaviateCommandsInsertAcceptedCode is the HTTP code returned for type WeaviateCommandsInsertAccepted
const WeaviateCommandsInsertAcceptedCode int = 202

/*WeaviateCommandsInsertAccepted Successfully received.

swagger:response weaviateCommandsInsertAccepted
*/
type WeaviateCommandsInsertAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.CommandGetResponse `json:"body,omitempty"`
}

// NewWeaviateCommandsInsertAccepted creates WeaviateCommandsInsertAccepted with default headers values
func NewWeaviateCommandsInsertAccepted() *WeaviateCommandsInsertAccepted {
	return &WeaviateCommandsInsertAccepted{}
}

// WithPayload adds the payload to the weaviate commands insert accepted response
func (o *WeaviateCommandsInsertAccepted) WithPayload(payload *models.CommandGetResponse) *WeaviateCommandsInsertAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate commands insert accepted response
func (o *WeaviateCommandsInsertAccepted) SetPayload(payload *models.CommandGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateCommandsInsertAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateCommandsInsertUnauthorizedCode is the HTTP code returned for type WeaviateCommandsInsertUnauthorized
const WeaviateCommandsInsertUnauthorizedCode int = 401

/*WeaviateCommandsInsertUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateCommandsInsertUnauthorized
*/
type WeaviateCommandsInsertUnauthorized struct {
}

// NewWeaviateCommandsInsertUnauthorized creates WeaviateCommandsInsertUnauthorized with default headers values
func NewWeaviateCommandsInsertUnauthorized() *WeaviateCommandsInsertUnauthorized {
	return &WeaviateCommandsInsertUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateCommandsInsertUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateCommandsInsertForbiddenCode is the HTTP code returned for type WeaviateCommandsInsertForbidden
const WeaviateCommandsInsertForbiddenCode int = 403

/*WeaviateCommandsInsertForbidden The used API-key has insufficient permissions.

swagger:response weaviateCommandsInsertForbidden
*/
type WeaviateCommandsInsertForbidden struct {
}

// NewWeaviateCommandsInsertForbidden creates WeaviateCommandsInsertForbidden with default headers values
func NewWeaviateCommandsInsertForbidden() *WeaviateCommandsInsertForbidden {
	return &WeaviateCommandsInsertForbidden{}
}

// WriteResponse to the client
func (o *WeaviateCommandsInsertForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateCommandsInsertNotImplementedCode is the HTTP code returned for type WeaviateCommandsInsertNotImplemented
const WeaviateCommandsInsertNotImplementedCode int = 501

/*WeaviateCommandsInsertNotImplemented Not (yet) implemented.

swagger:response weaviateCommandsInsertNotImplemented
*/
type WeaviateCommandsInsertNotImplemented struct {
}

// NewWeaviateCommandsInsertNotImplemented creates WeaviateCommandsInsertNotImplemented with default headers values
func NewWeaviateCommandsInsertNotImplemented() *WeaviateCommandsInsertNotImplemented {
	return &WeaviateCommandsInsertNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateCommandsInsertNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
