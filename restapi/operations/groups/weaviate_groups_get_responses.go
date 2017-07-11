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

 
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateGroupsGetOKCode is the HTTP code returned for type WeaviateGroupsGetOK
const WeaviateGroupsGetOKCode int = 200

/*WeaviateGroupsGetOK Successful response.

swagger:response weaviateGroupsGetOK
*/
type WeaviateGroupsGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.GroupGetResponse `json:"body,omitempty"`
}

// NewWeaviateGroupsGetOK creates WeaviateGroupsGetOK with default headers values
func NewWeaviateGroupsGetOK() *WeaviateGroupsGetOK {
	return &WeaviateGroupsGetOK{}
}

// WithPayload adds the payload to the weaviate groups get o k response
func (o *WeaviateGroupsGetOK) WithPayload(payload *models.GroupGetResponse) *WeaviateGroupsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate groups get o k response
func (o *WeaviateGroupsGetOK) SetPayload(payload *models.GroupGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateGroupsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateGroupsGetUnauthorizedCode is the HTTP code returned for type WeaviateGroupsGetUnauthorized
const WeaviateGroupsGetUnauthorizedCode int = 401

/*WeaviateGroupsGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateGroupsGetUnauthorized
*/
type WeaviateGroupsGetUnauthorized struct {
}

// NewWeaviateGroupsGetUnauthorized creates WeaviateGroupsGetUnauthorized with default headers values
func NewWeaviateGroupsGetUnauthorized() *WeaviateGroupsGetUnauthorized {
	return &WeaviateGroupsGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateGroupsGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateGroupsGetForbiddenCode is the HTTP code returned for type WeaviateGroupsGetForbidden
const WeaviateGroupsGetForbiddenCode int = 403

/*WeaviateGroupsGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateGroupsGetForbidden
*/
type WeaviateGroupsGetForbidden struct {
}

// NewWeaviateGroupsGetForbidden creates WeaviateGroupsGetForbidden with default headers values
func NewWeaviateGroupsGetForbidden() *WeaviateGroupsGetForbidden {
	return &WeaviateGroupsGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateGroupsGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateGroupsGetNotFoundCode is the HTTP code returned for type WeaviateGroupsGetNotFound
const WeaviateGroupsGetNotFoundCode int = 404

/*WeaviateGroupsGetNotFound Successful query result but no resource was found.

swagger:response weaviateGroupsGetNotFound
*/
type WeaviateGroupsGetNotFound struct {
}

// NewWeaviateGroupsGetNotFound creates WeaviateGroupsGetNotFound with default headers values
func NewWeaviateGroupsGetNotFound() *WeaviateGroupsGetNotFound {
	return &WeaviateGroupsGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateGroupsGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateGroupsGetNotImplementedCode is the HTTP code returned for type WeaviateGroupsGetNotImplemented
const WeaviateGroupsGetNotImplementedCode int = 501

/*WeaviateGroupsGetNotImplemented Not (yet) implemented.

swagger:response weaviateGroupsGetNotImplemented
*/
type WeaviateGroupsGetNotImplemented struct {
}

// NewWeaviateGroupsGetNotImplemented creates WeaviateGroupsGetNotImplemented with default headers values
func NewWeaviateGroupsGetNotImplemented() *WeaviateGroupsGetNotImplemented {
	return &WeaviateGroupsGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateGroupsGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
