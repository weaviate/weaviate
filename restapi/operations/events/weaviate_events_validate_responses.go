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

// WeaviateEventsValidateOKCode is the HTTP code returned for type WeaviateEventsValidateOK
const WeaviateEventsValidateOKCode int = 200

/*WeaviateEventsValidateOK Successful validated.

swagger:response weaviateEventsValidateOK
*/
type WeaviateEventsValidateOK struct {
}

// NewWeaviateEventsValidateOK creates WeaviateEventsValidateOK with default headers values
func NewWeaviateEventsValidateOK() *WeaviateEventsValidateOK {
	return &WeaviateEventsValidateOK{}
}

// WriteResponse to the client
func (o *WeaviateEventsValidateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}

// WeaviateEventsValidateUnauthorizedCode is the HTTP code returned for type WeaviateEventsValidateUnauthorized
const WeaviateEventsValidateUnauthorizedCode int = 401

/*WeaviateEventsValidateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateEventsValidateUnauthorized
*/
type WeaviateEventsValidateUnauthorized struct {
}

// NewWeaviateEventsValidateUnauthorized creates WeaviateEventsValidateUnauthorized with default headers values
func NewWeaviateEventsValidateUnauthorized() *WeaviateEventsValidateUnauthorized {
	return &WeaviateEventsValidateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateEventsValidateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateEventsValidateForbiddenCode is the HTTP code returned for type WeaviateEventsValidateForbidden
const WeaviateEventsValidateForbiddenCode int = 403

/*WeaviateEventsValidateForbidden The used API-key has insufficient permissions.

swagger:response weaviateEventsValidateForbidden
*/
type WeaviateEventsValidateForbidden struct {
}

// NewWeaviateEventsValidateForbidden creates WeaviateEventsValidateForbidden with default headers values
func NewWeaviateEventsValidateForbidden() *WeaviateEventsValidateForbidden {
	return &WeaviateEventsValidateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateEventsValidateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateEventsValidateUnprocessableEntityCode is the HTTP code returned for type WeaviateEventsValidateUnprocessableEntity
const WeaviateEventsValidateUnprocessableEntityCode int = 422

/*WeaviateEventsValidateUnprocessableEntity Can not validate, check the body.

swagger:response weaviateEventsValidateUnprocessableEntity
*/
type WeaviateEventsValidateUnprocessableEntity struct {
}

// NewWeaviateEventsValidateUnprocessableEntity creates WeaviateEventsValidateUnprocessableEntity with default headers values
func NewWeaviateEventsValidateUnprocessableEntity() *WeaviateEventsValidateUnprocessableEntity {
	return &WeaviateEventsValidateUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateEventsValidateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateEventsValidateNotImplementedCode is the HTTP code returned for type WeaviateEventsValidateNotImplemented
const WeaviateEventsValidateNotImplementedCode int = 501

/*WeaviateEventsValidateNotImplemented Not (yet) implemented.

swagger:response weaviateEventsValidateNotImplemented
*/
type WeaviateEventsValidateNotImplemented struct {
}

// NewWeaviateEventsValidateNotImplemented creates WeaviateEventsValidateNotImplemented with default headers values
func NewWeaviateEventsValidateNotImplemented() *WeaviateEventsValidateNotImplemented {
	return &WeaviateEventsValidateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateEventsValidateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
