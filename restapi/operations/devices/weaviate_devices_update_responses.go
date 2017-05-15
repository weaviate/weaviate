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

// WeaviateDevicesUpdateOKCode is the HTTP code returned for type WeaviateDevicesUpdateOK
const WeaviateDevicesUpdateOKCode int = 200

/*WeaviateDevicesUpdateOK Successful update.

swagger:response weaviateDevicesUpdateOK
*/
type WeaviateDevicesUpdateOK struct {

	/*
	  In: Body
	*/
	Payload *models.Device `json:"body,omitempty"`
}

// NewWeaviateDevicesUpdateOK creates WeaviateDevicesUpdateOK with default headers values
func NewWeaviateDevicesUpdateOK() *WeaviateDevicesUpdateOK {
	return &WeaviateDevicesUpdateOK{}
}

// WithPayload adds the payload to the weaviate devices update o k response
func (o *WeaviateDevicesUpdateOK) WithPayload(payload *models.Device) *WeaviateDevicesUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate devices update o k response
func (o *WeaviateDevicesUpdateOK) SetPayload(payload *models.Device) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateDevicesUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateDevicesUpdateUnauthorizedCode is the HTTP code returned for type WeaviateDevicesUpdateUnauthorized
const WeaviateDevicesUpdateUnauthorizedCode int = 401

/*WeaviateDevicesUpdateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateDevicesUpdateUnauthorized
*/
type WeaviateDevicesUpdateUnauthorized struct {
}

// NewWeaviateDevicesUpdateUnauthorized creates WeaviateDevicesUpdateUnauthorized with default headers values
func NewWeaviateDevicesUpdateUnauthorized() *WeaviateDevicesUpdateUnauthorized {
	return &WeaviateDevicesUpdateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateDevicesUpdateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateDevicesUpdateForbiddenCode is the HTTP code returned for type WeaviateDevicesUpdateForbidden
const WeaviateDevicesUpdateForbiddenCode int = 403

/*WeaviateDevicesUpdateForbidden The used API-key has insufficient permissions.

swagger:response weaviateDevicesUpdateForbidden
*/
type WeaviateDevicesUpdateForbidden struct {
}

// NewWeaviateDevicesUpdateForbidden creates WeaviateDevicesUpdateForbidden with default headers values
func NewWeaviateDevicesUpdateForbidden() *WeaviateDevicesUpdateForbidden {
	return &WeaviateDevicesUpdateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateDevicesUpdateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateDevicesUpdateNotFoundCode is the HTTP code returned for type WeaviateDevicesUpdateNotFound
const WeaviateDevicesUpdateNotFoundCode int = 404

/*WeaviateDevicesUpdateNotFound Successful query result but no resource was found.

swagger:response weaviateDevicesUpdateNotFound
*/
type WeaviateDevicesUpdateNotFound struct {
}

// NewWeaviateDevicesUpdateNotFound creates WeaviateDevicesUpdateNotFound with default headers values
func NewWeaviateDevicesUpdateNotFound() *WeaviateDevicesUpdateNotFound {
	return &WeaviateDevicesUpdateNotFound{}
}

// WriteResponse to the client
func (o *WeaviateDevicesUpdateNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateDevicesUpdateNotImplementedCode is the HTTP code returned for type WeaviateDevicesUpdateNotImplemented
const WeaviateDevicesUpdateNotImplementedCode int = 501

/*WeaviateDevicesUpdateNotImplemented Not (yet) implemented.

swagger:response weaviateDevicesUpdateNotImplemented
*/
type WeaviateDevicesUpdateNotImplemented struct {
}

// NewWeaviateDevicesUpdateNotImplemented creates WeaviateDevicesUpdateNotImplemented with default headers values
func NewWeaviateDevicesUpdateNotImplemented() *WeaviateDevicesUpdateNotImplemented {
	return &WeaviateDevicesUpdateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateDevicesUpdateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
