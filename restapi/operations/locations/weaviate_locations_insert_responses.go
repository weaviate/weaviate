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
 package locations




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateLocationsInsertAcceptedCode is the HTTP code returned for type WeaviateLocationsInsertAccepted
const WeaviateLocationsInsertAcceptedCode int = 202

/*WeaviateLocationsInsertAccepted Successfully received.

swagger:response weaviateLocationsInsertAccepted
*/
type WeaviateLocationsInsertAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.Location `json:"body,omitempty"`
}

// NewWeaviateLocationsInsertAccepted creates WeaviateLocationsInsertAccepted with default headers values
func NewWeaviateLocationsInsertAccepted() *WeaviateLocationsInsertAccepted {
	return &WeaviateLocationsInsertAccepted{}
}

// WithPayload adds the payload to the weaviate locations insert accepted response
func (o *WeaviateLocationsInsertAccepted) WithPayload(payload *models.Location) *WeaviateLocationsInsertAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate locations insert accepted response
func (o *WeaviateLocationsInsertAccepted) SetPayload(payload *models.Location) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateLocationsInsertAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateLocationsInsertUnauthorizedCode is the HTTP code returned for type WeaviateLocationsInsertUnauthorized
const WeaviateLocationsInsertUnauthorizedCode int = 401

/*WeaviateLocationsInsertUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateLocationsInsertUnauthorized
*/
type WeaviateLocationsInsertUnauthorized struct {
}

// NewWeaviateLocationsInsertUnauthorized creates WeaviateLocationsInsertUnauthorized with default headers values
func NewWeaviateLocationsInsertUnauthorized() *WeaviateLocationsInsertUnauthorized {
	return &WeaviateLocationsInsertUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateLocationsInsertUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateLocationsInsertForbiddenCode is the HTTP code returned for type WeaviateLocationsInsertForbidden
const WeaviateLocationsInsertForbiddenCode int = 403

/*WeaviateLocationsInsertForbidden The used API-key has insufficient permissions.

swagger:response weaviateLocationsInsertForbidden
*/
type WeaviateLocationsInsertForbidden struct {
}

// NewWeaviateLocationsInsertForbidden creates WeaviateLocationsInsertForbidden with default headers values
func NewWeaviateLocationsInsertForbidden() *WeaviateLocationsInsertForbidden {
	return &WeaviateLocationsInsertForbidden{}
}

// WriteResponse to the client
func (o *WeaviateLocationsInsertForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateLocationsInsertNotImplementedCode is the HTTP code returned for type WeaviateLocationsInsertNotImplemented
const WeaviateLocationsInsertNotImplementedCode int = 501

/*WeaviateLocationsInsertNotImplemented Not (yet) implemented.

swagger:response weaviateLocationsInsertNotImplemented
*/
type WeaviateLocationsInsertNotImplemented struct {
}

// NewWeaviateLocationsInsertNotImplemented creates WeaviateLocationsInsertNotImplemented with default headers values
func NewWeaviateLocationsInsertNotImplemented() *WeaviateLocationsInsertNotImplemented {
	return &WeaviateLocationsInsertNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateLocationsInsertNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
