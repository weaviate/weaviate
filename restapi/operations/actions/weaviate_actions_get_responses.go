/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 Weaviate. All rights reserved.
 * LICENSE: https://github.com/weaviate/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@weaviate.com)
 * See www.weaviate.com for details
 * Contact: @weaviate_iot / yourfriends@weaviate.com
 */

package actions

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateActionsGetOKCode is the HTTP code returned for type WeaviateActionsGetOK
const WeaviateActionsGetOKCode int = 200

/*WeaviateActionsGetOK Successful response.

swagger:response weaviateActionsGetOK
*/
type WeaviateActionsGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.ActionGetResponse `json:"body,omitempty"`
}

// NewWeaviateActionsGetOK creates WeaviateActionsGetOK with default headers values
func NewWeaviateActionsGetOK() *WeaviateActionsGetOK {
	return &WeaviateActionsGetOK{}
}

// WithPayload adds the payload to the weaviate actions get o k response
func (o *WeaviateActionsGetOK) WithPayload(payload *models.ActionGetResponse) *WeaviateActionsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate actions get o k response
func (o *WeaviateActionsGetOK) SetPayload(payload *models.ActionGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateActionsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateActionsGetUnauthorizedCode is the HTTP code returned for type WeaviateActionsGetUnauthorized
const WeaviateActionsGetUnauthorizedCode int = 401

/*WeaviateActionsGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateActionsGetUnauthorized
*/
type WeaviateActionsGetUnauthorized struct {
}

// NewWeaviateActionsGetUnauthorized creates WeaviateActionsGetUnauthorized with default headers values
func NewWeaviateActionsGetUnauthorized() *WeaviateActionsGetUnauthorized {
	return &WeaviateActionsGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateActionsGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateActionsGetForbiddenCode is the HTTP code returned for type WeaviateActionsGetForbidden
const WeaviateActionsGetForbiddenCode int = 403

/*WeaviateActionsGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateActionsGetForbidden
*/
type WeaviateActionsGetForbidden struct {
}

// NewWeaviateActionsGetForbidden creates WeaviateActionsGetForbidden with default headers values
func NewWeaviateActionsGetForbidden() *WeaviateActionsGetForbidden {
	return &WeaviateActionsGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateActionsGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateActionsGetNotFoundCode is the HTTP code returned for type WeaviateActionsGetNotFound
const WeaviateActionsGetNotFoundCode int = 404

/*WeaviateActionsGetNotFound Successful query result but no resource was found.

swagger:response weaviateActionsGetNotFound
*/
type WeaviateActionsGetNotFound struct {
}

// NewWeaviateActionsGetNotFound creates WeaviateActionsGetNotFound with default headers values
func NewWeaviateActionsGetNotFound() *WeaviateActionsGetNotFound {
	return &WeaviateActionsGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateActionsGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateActionsGetNotImplementedCode is the HTTP code returned for type WeaviateActionsGetNotImplemented
const WeaviateActionsGetNotImplementedCode int = 501

/*WeaviateActionsGetNotImplemented Not (yet) implemented.

swagger:response weaviateActionsGetNotImplemented
*/
type WeaviateActionsGetNotImplemented struct {
}

// NewWeaviateActionsGetNotImplemented creates WeaviateActionsGetNotImplemented with default headers values
func NewWeaviateActionsGetNotImplemented() *WeaviateActionsGetNotImplemented {
	return &WeaviateActionsGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateActionsGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
