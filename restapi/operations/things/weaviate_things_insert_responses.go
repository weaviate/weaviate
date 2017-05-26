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
 package things




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateThingsInsertAcceptedCode is the HTTP code returned for type WeaviateThingsInsertAccepted
const WeaviateThingsInsertAcceptedCode int = 202

/*WeaviateThingsInsertAccepted Successfully received.

swagger:response weaviateThingsInsertAccepted
*/
type WeaviateThingsInsertAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.ThingGetResponse `json:"body,omitempty"`
}

// NewWeaviateThingsInsertAccepted creates WeaviateThingsInsertAccepted with default headers values
func NewWeaviateThingsInsertAccepted() *WeaviateThingsInsertAccepted {
	return &WeaviateThingsInsertAccepted{}
}

// WithPayload adds the payload to the weaviate things insert accepted response
func (o *WeaviateThingsInsertAccepted) WithPayload(payload *models.ThingGetResponse) *WeaviateThingsInsertAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things insert accepted response
func (o *WeaviateThingsInsertAccepted) SetPayload(payload *models.ThingGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsInsertAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsInsertUnauthorizedCode is the HTTP code returned for type WeaviateThingsInsertUnauthorized
const WeaviateThingsInsertUnauthorizedCode int = 401

/*WeaviateThingsInsertUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsInsertUnauthorized
*/
type WeaviateThingsInsertUnauthorized struct {
}

// NewWeaviateThingsInsertUnauthorized creates WeaviateThingsInsertUnauthorized with default headers values
func NewWeaviateThingsInsertUnauthorized() *WeaviateThingsInsertUnauthorized {
	return &WeaviateThingsInsertUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsInsertUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsInsertForbiddenCode is the HTTP code returned for type WeaviateThingsInsertForbidden
const WeaviateThingsInsertForbiddenCode int = 403

/*WeaviateThingsInsertForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsInsertForbidden
*/
type WeaviateThingsInsertForbidden struct {
}

// NewWeaviateThingsInsertForbidden creates WeaviateThingsInsertForbidden with default headers values
func NewWeaviateThingsInsertForbidden() *WeaviateThingsInsertForbidden {
	return &WeaviateThingsInsertForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsInsertForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsInsertNotImplementedCode is the HTTP code returned for type WeaviateThingsInsertNotImplemented
const WeaviateThingsInsertNotImplementedCode int = 501

/*WeaviateThingsInsertNotImplemented Not (yet) implemented.

swagger:response weaviateThingsInsertNotImplemented
*/
type WeaviateThingsInsertNotImplemented struct {
}

// NewWeaviateThingsInsertNotImplemented creates WeaviateThingsInsertNotImplemented with default headers values
func NewWeaviateThingsInsertNotImplemented() *WeaviateThingsInsertNotImplemented {
	return &WeaviateThingsInsertNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsInsertNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
