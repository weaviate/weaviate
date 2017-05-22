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
 package groups




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateGroupsInsertAcceptedCode is the HTTP code returned for type WeaviateGroupsInsertAccepted
const WeaviateGroupsInsertAcceptedCode int = 202

/*WeaviateGroupsInsertAccepted Successfully received.

swagger:response weaviateGroupsInsertAccepted
*/
type WeaviateGroupsInsertAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.Group `json:"body,omitempty"`
}

// NewWeaviateGroupsInsertAccepted creates WeaviateGroupsInsertAccepted with default headers values
func NewWeaviateGroupsInsertAccepted() *WeaviateGroupsInsertAccepted {
	return &WeaviateGroupsInsertAccepted{}
}

// WithPayload adds the payload to the weaviate groups insert accepted response
func (o *WeaviateGroupsInsertAccepted) WithPayload(payload *models.Group) *WeaviateGroupsInsertAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate groups insert accepted response
func (o *WeaviateGroupsInsertAccepted) SetPayload(payload *models.Group) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateGroupsInsertAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateGroupsInsertUnauthorizedCode is the HTTP code returned for type WeaviateGroupsInsertUnauthorized
const WeaviateGroupsInsertUnauthorizedCode int = 401

/*WeaviateGroupsInsertUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateGroupsInsertUnauthorized
*/
type WeaviateGroupsInsertUnauthorized struct {
}

// NewWeaviateGroupsInsertUnauthorized creates WeaviateGroupsInsertUnauthorized with default headers values
func NewWeaviateGroupsInsertUnauthorized() *WeaviateGroupsInsertUnauthorized {
	return &WeaviateGroupsInsertUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateGroupsInsertUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateGroupsInsertForbiddenCode is the HTTP code returned for type WeaviateGroupsInsertForbidden
const WeaviateGroupsInsertForbiddenCode int = 403

/*WeaviateGroupsInsertForbidden The used API-key has insufficient permissions.

swagger:response weaviateGroupsInsertForbidden
*/
type WeaviateGroupsInsertForbidden struct {
}

// NewWeaviateGroupsInsertForbidden creates WeaviateGroupsInsertForbidden with default headers values
func NewWeaviateGroupsInsertForbidden() *WeaviateGroupsInsertForbidden {
	return &WeaviateGroupsInsertForbidden{}
}

// WriteResponse to the client
func (o *WeaviateGroupsInsertForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateGroupsInsertNotImplementedCode is the HTTP code returned for type WeaviateGroupsInsertNotImplemented
const WeaviateGroupsInsertNotImplementedCode int = 501

/*WeaviateGroupsInsertNotImplemented Not (yet) implemented.

swagger:response weaviateGroupsInsertNotImplemented
*/
type WeaviateGroupsInsertNotImplemented struct {
}

// NewWeaviateGroupsInsertNotImplemented creates WeaviateGroupsInsertNotImplemented with default headers values
func NewWeaviateGroupsInsertNotImplemented() *WeaviateGroupsInsertNotImplemented {
	return &WeaviateGroupsInsertNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateGroupsInsertNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
