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
 package events




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateThingsEventsCreateAcceptedCode is the HTTP code returned for type WeaviateThingsEventsCreateAccepted
const WeaviateThingsEventsCreateAcceptedCode int = 202

/*WeaviateThingsEventsCreateAccepted Successfully received.

swagger:response weaviateThingsEventsCreateAccepted
*/
type WeaviateThingsEventsCreateAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.EventGetResponse `json:"body,omitempty"`
}

// NewWeaviateThingsEventsCreateAccepted creates WeaviateThingsEventsCreateAccepted with default headers values
func NewWeaviateThingsEventsCreateAccepted() *WeaviateThingsEventsCreateAccepted {
	return &WeaviateThingsEventsCreateAccepted{}
}

// WithPayload adds the payload to the weaviate things events create accepted response
func (o *WeaviateThingsEventsCreateAccepted) WithPayload(payload *models.EventGetResponse) *WeaviateThingsEventsCreateAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things events create accepted response
func (o *WeaviateThingsEventsCreateAccepted) SetPayload(payload *models.EventGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsEventsCreateAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsEventsCreateUnauthorizedCode is the HTTP code returned for type WeaviateThingsEventsCreateUnauthorized
const WeaviateThingsEventsCreateUnauthorizedCode int = 401

/*WeaviateThingsEventsCreateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsEventsCreateUnauthorized
*/
type WeaviateThingsEventsCreateUnauthorized struct {
}

// NewWeaviateThingsEventsCreateUnauthorized creates WeaviateThingsEventsCreateUnauthorized with default headers values
func NewWeaviateThingsEventsCreateUnauthorized() *WeaviateThingsEventsCreateUnauthorized {
	return &WeaviateThingsEventsCreateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsEventsCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsEventsCreateForbiddenCode is the HTTP code returned for type WeaviateThingsEventsCreateForbidden
const WeaviateThingsEventsCreateForbiddenCode int = 403

/*WeaviateThingsEventsCreateForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsEventsCreateForbidden
*/
type WeaviateThingsEventsCreateForbidden struct {
}

// NewWeaviateThingsEventsCreateForbidden creates WeaviateThingsEventsCreateForbidden with default headers values
func NewWeaviateThingsEventsCreateForbidden() *WeaviateThingsEventsCreateForbidden {
	return &WeaviateThingsEventsCreateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsEventsCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsEventsCreateNotImplementedCode is the HTTP code returned for type WeaviateThingsEventsCreateNotImplemented
const WeaviateThingsEventsCreateNotImplementedCode int = 501

/*WeaviateThingsEventsCreateNotImplemented Not (yet) implemented.

swagger:response weaviateThingsEventsCreateNotImplemented
*/
type WeaviateThingsEventsCreateNotImplemented struct {
}

// NewWeaviateThingsEventsCreateNotImplemented creates WeaviateThingsEventsCreateNotImplemented with default headers values
func NewWeaviateThingsEventsCreateNotImplemented() *WeaviateThingsEventsCreateNotImplemented {
	return &WeaviateThingsEventsCreateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsEventsCreateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
