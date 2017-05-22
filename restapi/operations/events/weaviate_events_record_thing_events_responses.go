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
)

// WeaviateEventsRecordThingEventsAcceptedCode is the HTTP code returned for type WeaviateEventsRecordThingEventsAccepted
const WeaviateEventsRecordThingEventsAcceptedCode int = 202

/*WeaviateEventsRecordThingEventsAccepted Successfully received.

swagger:response weaviateEventsRecordThingEventsAccepted
*/
type WeaviateEventsRecordThingEventsAccepted struct {
}

// NewWeaviateEventsRecordThingEventsAccepted creates WeaviateEventsRecordThingEventsAccepted with default headers values
func NewWeaviateEventsRecordThingEventsAccepted() *WeaviateEventsRecordThingEventsAccepted {
	return &WeaviateEventsRecordThingEventsAccepted{}
}

// WriteResponse to the client
func (o *WeaviateEventsRecordThingEventsAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
}

// WeaviateEventsRecordThingEventsUnauthorizedCode is the HTTP code returned for type WeaviateEventsRecordThingEventsUnauthorized
const WeaviateEventsRecordThingEventsUnauthorizedCode int = 401

/*WeaviateEventsRecordThingEventsUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateEventsRecordThingEventsUnauthorized
*/
type WeaviateEventsRecordThingEventsUnauthorized struct {
}

// NewWeaviateEventsRecordThingEventsUnauthorized creates WeaviateEventsRecordThingEventsUnauthorized with default headers values
func NewWeaviateEventsRecordThingEventsUnauthorized() *WeaviateEventsRecordThingEventsUnauthorized {
	return &WeaviateEventsRecordThingEventsUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateEventsRecordThingEventsUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateEventsRecordThingEventsForbiddenCode is the HTTP code returned for type WeaviateEventsRecordThingEventsForbidden
const WeaviateEventsRecordThingEventsForbiddenCode int = 403

/*WeaviateEventsRecordThingEventsForbidden The used API-key has insufficient permissions.

swagger:response weaviateEventsRecordThingEventsForbidden
*/
type WeaviateEventsRecordThingEventsForbidden struct {
}

// NewWeaviateEventsRecordThingEventsForbidden creates WeaviateEventsRecordThingEventsForbidden with default headers values
func NewWeaviateEventsRecordThingEventsForbidden() *WeaviateEventsRecordThingEventsForbidden {
	return &WeaviateEventsRecordThingEventsForbidden{}
}

// WriteResponse to the client
func (o *WeaviateEventsRecordThingEventsForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateEventsRecordThingEventsNotImplementedCode is the HTTP code returned for type WeaviateEventsRecordThingEventsNotImplemented
const WeaviateEventsRecordThingEventsNotImplementedCode int = 501

/*WeaviateEventsRecordThingEventsNotImplemented Not (yet) implemented.

swagger:response weaviateEventsRecordThingEventsNotImplemented
*/
type WeaviateEventsRecordThingEventsNotImplemented struct {
}

// NewWeaviateEventsRecordThingEventsNotImplemented creates WeaviateEventsRecordThingEventsNotImplemented with default headers values
func NewWeaviateEventsRecordThingEventsNotImplemented() *WeaviateEventsRecordThingEventsNotImplemented {
	return &WeaviateEventsRecordThingEventsNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateEventsRecordThingEventsNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
