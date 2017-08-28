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
   

package things


 

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateThingsActionsGetOKCode is the HTTP code returned for type WeaviateThingsActionsGetOK
const WeaviateThingsActionsGetOKCode int = 200

/*WeaviateThingsActionsGetOK Successful response.

swagger:response weaviateThingsActionsGetOK
*/
type WeaviateThingsActionsGetOK struct {

	/*
	  In: Body
	*/
	Payload *models.ActionsListResponse `json:"body,omitempty"`
}

// NewWeaviateThingsActionsGetOK creates WeaviateThingsActionsGetOK with default headers values
func NewWeaviateThingsActionsGetOK() *WeaviateThingsActionsGetOK {
	return &WeaviateThingsActionsGetOK{}
}

// WithPayload adds the payload to the weaviate things actions get o k response
func (o *WeaviateThingsActionsGetOK) WithPayload(payload *models.ActionsListResponse) *WeaviateThingsActionsGetOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate things actions get o k response
func (o *WeaviateThingsActionsGetOK) SetPayload(payload *models.ActionsListResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingsActionsGetOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingsActionsGetUnauthorizedCode is the HTTP code returned for type WeaviateThingsActionsGetUnauthorized
const WeaviateThingsActionsGetUnauthorizedCode int = 401

/*WeaviateThingsActionsGetUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingsActionsGetUnauthorized
*/
type WeaviateThingsActionsGetUnauthorized struct {
}

// NewWeaviateThingsActionsGetUnauthorized creates WeaviateThingsActionsGetUnauthorized with default headers values
func NewWeaviateThingsActionsGetUnauthorized() *WeaviateThingsActionsGetUnauthorized {
	return &WeaviateThingsActionsGetUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingsActionsGetUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingsActionsGetForbiddenCode is the HTTP code returned for type WeaviateThingsActionsGetForbidden
const WeaviateThingsActionsGetForbiddenCode int = 403

/*WeaviateThingsActionsGetForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingsActionsGetForbidden
*/
type WeaviateThingsActionsGetForbidden struct {
}

// NewWeaviateThingsActionsGetForbidden creates WeaviateThingsActionsGetForbidden with default headers values
func NewWeaviateThingsActionsGetForbidden() *WeaviateThingsActionsGetForbidden {
	return &WeaviateThingsActionsGetForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingsActionsGetForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingsActionsGetNotFoundCode is the HTTP code returned for type WeaviateThingsActionsGetNotFound
const WeaviateThingsActionsGetNotFoundCode int = 404

/*WeaviateThingsActionsGetNotFound Successful query result but no resource was found.

swagger:response weaviateThingsActionsGetNotFound
*/
type WeaviateThingsActionsGetNotFound struct {
}

// NewWeaviateThingsActionsGetNotFound creates WeaviateThingsActionsGetNotFound with default headers values
func NewWeaviateThingsActionsGetNotFound() *WeaviateThingsActionsGetNotFound {
	return &WeaviateThingsActionsGetNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingsActionsGetNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingsActionsGetNotImplementedCode is the HTTP code returned for type WeaviateThingsActionsGetNotImplemented
const WeaviateThingsActionsGetNotImplementedCode int = 501

/*WeaviateThingsActionsGetNotImplemented Not (yet) implemented.

swagger:response weaviateThingsActionsGetNotImplemented
*/
type WeaviateThingsActionsGetNotImplemented struct {
}

// NewWeaviateThingsActionsGetNotImplemented creates WeaviateThingsActionsGetNotImplemented with default headers values
func NewWeaviateThingsActionsGetNotImplemented() *WeaviateThingsActionsGetNotImplemented {
	return &WeaviateThingsActionsGetNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingsActionsGetNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
