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
 package adapters




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateAdaptersListOKCode is the HTTP code returned for type WeaviateAdaptersListOK
const WeaviateAdaptersListOKCode int = 200

/*WeaviateAdaptersListOK Successful response.

swagger:response weaviateAdaptersListOK
*/
type WeaviateAdaptersListOK struct {

	/*
	  In: Body
	*/
	Payload *models.AdaptersListResponse `json:"body,omitempty"`
}

// NewWeaviateAdaptersListOK creates WeaviateAdaptersListOK with default headers values
func NewWeaviateAdaptersListOK() *WeaviateAdaptersListOK {
	return &WeaviateAdaptersListOK{}
}

// WithPayload adds the payload to the weaviate adapters list o k response
func (o *WeaviateAdaptersListOK) WithPayload(payload *models.AdaptersListResponse) *WeaviateAdaptersListOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate adapters list o k response
func (o *WeaviateAdaptersListOK) SetPayload(payload *models.AdaptersListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateAdaptersListOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateAdaptersListUnauthorizedCode is the HTTP code returned for type WeaviateAdaptersListUnauthorized
const WeaviateAdaptersListUnauthorizedCode int = 401

/*WeaviateAdaptersListUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateAdaptersListUnauthorized
*/
type WeaviateAdaptersListUnauthorized struct {
}

// NewWeaviateAdaptersListUnauthorized creates WeaviateAdaptersListUnauthorized with default headers values
func NewWeaviateAdaptersListUnauthorized() *WeaviateAdaptersListUnauthorized {
	return &WeaviateAdaptersListUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersListUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateAdaptersListForbiddenCode is the HTTP code returned for type WeaviateAdaptersListForbidden
const WeaviateAdaptersListForbiddenCode int = 403

/*WeaviateAdaptersListForbidden The used API-key has insufficient permissions.

swagger:response weaviateAdaptersListForbidden
*/
type WeaviateAdaptersListForbidden struct {
}

// NewWeaviateAdaptersListForbidden creates WeaviateAdaptersListForbidden with default headers values
func NewWeaviateAdaptersListForbidden() *WeaviateAdaptersListForbidden {
	return &WeaviateAdaptersListForbidden{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersListForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateAdaptersListNotFoundCode is the HTTP code returned for type WeaviateAdaptersListNotFound
const WeaviateAdaptersListNotFoundCode int = 404

/*WeaviateAdaptersListNotFound Successful query result but no resource was found.

swagger:response weaviateAdaptersListNotFound
*/
type WeaviateAdaptersListNotFound struct {
}

// NewWeaviateAdaptersListNotFound creates WeaviateAdaptersListNotFound with default headers values
func NewWeaviateAdaptersListNotFound() *WeaviateAdaptersListNotFound {
	return &WeaviateAdaptersListNotFound{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersListNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateAdaptersListNotImplementedCode is the HTTP code returned for type WeaviateAdaptersListNotImplemented
const WeaviateAdaptersListNotImplementedCode int = 501

/*WeaviateAdaptersListNotImplemented Not (yet) implemented.

swagger:response weaviateAdaptersListNotImplemented
*/
type WeaviateAdaptersListNotImplemented struct {
}

// NewWeaviateAdaptersListNotImplemented creates WeaviateAdaptersListNotImplemented with default headers values
func NewWeaviateAdaptersListNotImplemented() *WeaviateAdaptersListNotImplemented {
	return &WeaviateAdaptersListNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateAdaptersListNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
