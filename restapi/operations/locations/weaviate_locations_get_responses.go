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

// WeaviateLocationsGetOKCode is the HTTP code returned for type WeaviateLocationsGetOK
const WeaviateLocationsGetOKCode int = 200

/*WeaviateLocationsGetOK Successful response.

swagger:response weaviateLocationsGetOK
*/
type WeaviateLocationsGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.LocationGetResponse `json:"body,omitempty"`
}

// NewWeaviateLocationsGetOK creates WeaviateLocationsGetOK with default headers values
func NewWeaviateLocationsGetOK() *WeaviateLocationsGetOK {
	return &WeaviateLocationsGetOK{}
}

// WithPayload adds the payload to the weaviate locations get o k response
func (o *WeaviateLocationsGetOK) WithPayload(payload *models.LocationGetResponse) *WeaviateLocationsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate locations get o k response
func (o *WeaviateLocationsGetOK) SetPayload(payload *models.LocationGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateLocationsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateLocationsGetUnauthorizedCode is the HTTP code returned for type WeaviateLocationsGetUnauthorized
const WeaviateLocationsGetUnauthorizedCode int = 401

/*WeaviateLocationsGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateLocationsGetUnauthorized
*/
type WeaviateLocationsGetUnauthorized struct {
}

// NewWeaviateLocationsGetUnauthorized creates WeaviateLocationsGetUnauthorized with default headers values
func NewWeaviateLocationsGetUnauthorized() *WeaviateLocationsGetUnauthorized {
	return &WeaviateLocationsGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateLocationsGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateLocationsGetForbiddenCode is the HTTP code returned for type WeaviateLocationsGetForbidden
const WeaviateLocationsGetForbiddenCode int = 403

/*WeaviateLocationsGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateLocationsGetForbidden
*/
type WeaviateLocationsGetForbidden struct {
}

// NewWeaviateLocationsGetForbidden creates WeaviateLocationsGetForbidden with default headers values
func NewWeaviateLocationsGetForbidden() *WeaviateLocationsGetForbidden {
	return &WeaviateLocationsGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateLocationsGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateLocationsGetNotFoundCode is the HTTP code returned for type WeaviateLocationsGetNotFound
const WeaviateLocationsGetNotFoundCode int = 404

/*WeaviateLocationsGetNotFound Successful query result but no resource was found.

swagger:response weaviateLocationsGetNotFound
*/
type WeaviateLocationsGetNotFound struct {
}

// NewWeaviateLocationsGetNotFound creates WeaviateLocationsGetNotFound with default headers values
func NewWeaviateLocationsGetNotFound() *WeaviateLocationsGetNotFound {
	return &WeaviateLocationsGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateLocationsGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateLocationsGetNotImplementedCode is the HTTP code returned for type WeaviateLocationsGetNotImplemented
const WeaviateLocationsGetNotImplementedCode int = 501

/*WeaviateLocationsGetNotImplemented Not (yet) implemented.

swagger:response weaviateLocationsGetNotImplemented
*/
type WeaviateLocationsGetNotImplemented struct {
}

// NewWeaviateLocationsGetNotImplemented creates WeaviateLocationsGetNotImplemented with default headers values
func NewWeaviateLocationsGetNotImplemented() *WeaviateLocationsGetNotImplemented {
	return &WeaviateLocationsGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateLocationsGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
