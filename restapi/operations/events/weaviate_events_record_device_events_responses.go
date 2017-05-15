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

// WeaviateEventsRecordDeviceEventsAcceptedCode is the HTTP code returned for type WeaviateEventsRecordDeviceEventsAccepted
const WeaviateEventsRecordDeviceEventsAcceptedCode int = 202

/*WeaviateEventsRecordDeviceEventsAccepted Successfully received.

swagger:response weaviateEventsRecordDeviceEventsAccepted
*/
type WeaviateEventsRecordDeviceEventsAccepted struct {
}

// NewWeaviateEventsRecordDeviceEventsAccepted creates WeaviateEventsRecordDeviceEventsAccepted with default headers values
func NewWeaviateEventsRecordDeviceEventsAccepted() *WeaviateEventsRecordDeviceEventsAccepted {
	return &WeaviateEventsRecordDeviceEventsAccepted{}
}

// WriteResponse to the client
func (o *WeaviateEventsRecordDeviceEventsAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
}

// WeaviateEventsRecordDeviceEventsUnauthorizedCode is the HTTP code returned for type WeaviateEventsRecordDeviceEventsUnauthorized
const WeaviateEventsRecordDeviceEventsUnauthorizedCode int = 401

/*WeaviateEventsRecordDeviceEventsUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateEventsRecordDeviceEventsUnauthorized
*/
type WeaviateEventsRecordDeviceEventsUnauthorized struct {
}

// NewWeaviateEventsRecordDeviceEventsUnauthorized creates WeaviateEventsRecordDeviceEventsUnauthorized with default headers values
func NewWeaviateEventsRecordDeviceEventsUnauthorized() *WeaviateEventsRecordDeviceEventsUnauthorized {
	return &WeaviateEventsRecordDeviceEventsUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateEventsRecordDeviceEventsUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateEventsRecordDeviceEventsForbiddenCode is the HTTP code returned for type WeaviateEventsRecordDeviceEventsForbidden
const WeaviateEventsRecordDeviceEventsForbiddenCode int = 403

/*WeaviateEventsRecordDeviceEventsForbidden The used API-key has insufficient permissions.

swagger:response weaviateEventsRecordDeviceEventsForbidden
*/
type WeaviateEventsRecordDeviceEventsForbidden struct {
}

// NewWeaviateEventsRecordDeviceEventsForbidden creates WeaviateEventsRecordDeviceEventsForbidden with default headers values
func NewWeaviateEventsRecordDeviceEventsForbidden() *WeaviateEventsRecordDeviceEventsForbidden {
	return &WeaviateEventsRecordDeviceEventsForbidden{}
}

// WriteResponse to the client
func (o *WeaviateEventsRecordDeviceEventsForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateEventsRecordDeviceEventsNotImplementedCode is the HTTP code returned for type WeaviateEventsRecordDeviceEventsNotImplemented
const WeaviateEventsRecordDeviceEventsNotImplementedCode int = 501

/*WeaviateEventsRecordDeviceEventsNotImplemented Not (yet) implemented.

swagger:response weaviateEventsRecordDeviceEventsNotImplemented
*/
type WeaviateEventsRecordDeviceEventsNotImplemented struct {
}

// NewWeaviateEventsRecordDeviceEventsNotImplemented creates WeaviateEventsRecordDeviceEventsNotImplemented with default headers values
func NewWeaviateEventsRecordDeviceEventsNotImplemented() *WeaviateEventsRecordDeviceEventsNotImplemented {
	return &WeaviateEventsRecordDeviceEventsNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateEventsRecordDeviceEventsNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
