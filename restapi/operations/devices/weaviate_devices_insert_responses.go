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
 package devices




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateDevicesInsertAcceptedCode is the HTTP code returned for type WeaviateDevicesInsertAccepted
const WeaviateDevicesInsertAcceptedCode int = 202

/*WeaviateDevicesInsertAccepted Successfully received.

swagger:response weaviateDevicesInsertAccepted
*/
type WeaviateDevicesInsertAccepted struct {

	/*
	  In: Body
	*/
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaviateDevicesInsertAccepted creates WeaviateDevicesInsertAccepted with default headers values
func NewWeaviateDevicesInsertAccepted() *WeaviateDevicesInsertAccepted {
	return &WeaviateDevicesInsertAccepted{}
}

// WithPayload adds the payload to the weaviate devices insert accepted response
func (o *WeaviateDevicesInsertAccepted) WithPayload(payload *models.Device) *WeaviateDevicesInsertAccepted {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate devices insert accepted response
func (o *WeaviateDevicesInsertAccepted) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateDevicesInsertAccepted) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(202)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateDevicesInsertUnauthorizedCode is the HTTP code returned for type WeaviateDevicesInsertUnauthorized
const WeaviateDevicesInsertUnauthorizedCode int = 401

/*WeaviateDevicesInsertUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateDevicesInsertUnauthorized
*/
type WeaviateDevicesInsertUnauthorized struct {
}

// NewWeaviateDevicesInsertUnauthorized creates WeaviateDevicesInsertUnauthorized with default headers values
func NewWeaviateDevicesInsertUnauthorized() *WeaviateDevicesInsertUnauthorized {
	return &WeaviateDevicesInsertUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateDevicesInsertUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateDevicesInsertForbiddenCode is the HTTP code returned for type WeaviateDevicesInsertForbidden
const WeaviateDevicesInsertForbiddenCode int = 403

/*WeaviateDevicesInsertForbidden The used API-key has insufficient permissions.

swagger:response weaviateDevicesInsertForbidden
*/
type WeaviateDevicesInsertForbidden struct {
}

// NewWeaviateDevicesInsertForbidden creates WeaviateDevicesInsertForbidden with default headers values
func NewWeaviateDevicesInsertForbidden() *WeaviateDevicesInsertForbidden {
	return &WeaviateDevicesInsertForbidden{}
}

// WriteResponse to the client
func (o *WeaviateDevicesInsertForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateDevicesInsertNotImplementedCode is the HTTP code returned for type WeaviateDevicesInsertNotImplemented
const WeaviateDevicesInsertNotImplementedCode int = 501

/*WeaviateDevicesInsertNotImplemented Not (yet) implemented.

swagger:response weaviateDevicesInsertNotImplemented
*/
type WeaviateDevicesInsertNotImplemented struct {
}

// NewWeaviateDevicesInsertNotImplemented creates WeaviateDevicesInsertNotImplemented with default headers values
func NewWeaviateDevicesInsertNotImplemented() *WeaviateDevicesInsertNotImplemented {
	return &WeaviateDevicesInsertNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateDevicesInsertNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
