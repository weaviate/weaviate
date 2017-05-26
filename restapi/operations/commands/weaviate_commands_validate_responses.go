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
 package commands




import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateCommandsValidateOKCode is the HTTP code returned for type WeaviateCommandsValidateOK
const WeaviateCommandsValidateOKCode int = 200

/*WeaviateCommandsValidateOK Successful validated.

swagger:response weaviateCommandsValidateOK
*/
type WeaviateCommandsValidateOK struct {
}

// NewWeaviateCommandsValidateOK creates WeaviateCommandsValidateOK with default headers values
func NewWeaviateCommandsValidateOK() *WeaviateCommandsValidateOK {
	return &WeaviateCommandsValidateOK{}
}

// WriteResponse to the client
func (o *WeaviateCommandsValidateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}

// WeaviateCommandsValidateUnauthorizedCode is the HTTP code returned for type WeaviateCommandsValidateUnauthorized
const WeaviateCommandsValidateUnauthorizedCode int = 401

/*WeaviateCommandsValidateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateCommandsValidateUnauthorized
*/
type WeaviateCommandsValidateUnauthorized struct {
}

// NewWeaviateCommandsValidateUnauthorized creates WeaviateCommandsValidateUnauthorized with default headers values
func NewWeaviateCommandsValidateUnauthorized() *WeaviateCommandsValidateUnauthorized {
	return &WeaviateCommandsValidateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateCommandsValidateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateCommandsValidateForbiddenCode is the HTTP code returned for type WeaviateCommandsValidateForbidden
const WeaviateCommandsValidateForbiddenCode int = 403

/*WeaviateCommandsValidateForbidden The used API-key has insufficient permissions.

swagger:response weaviateCommandsValidateForbidden
*/
type WeaviateCommandsValidateForbidden struct {
}

// NewWeaviateCommandsValidateForbidden creates WeaviateCommandsValidateForbidden with default headers values
func NewWeaviateCommandsValidateForbidden() *WeaviateCommandsValidateForbidden {
	return &WeaviateCommandsValidateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateCommandsValidateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateCommandsValidateUnprocessableEntityCode is the HTTP code returned for type WeaviateCommandsValidateUnprocessableEntity
const WeaviateCommandsValidateUnprocessableEntityCode int = 422

/*WeaviateCommandsValidateUnprocessableEntity Can not validate, check the body.

swagger:response weaviateCommandsValidateUnprocessableEntity
*/
type WeaviateCommandsValidateUnprocessableEntity struct {
}

// NewWeaviateCommandsValidateUnprocessableEntity creates WeaviateCommandsValidateUnprocessableEntity with default headers values
func NewWeaviateCommandsValidateUnprocessableEntity() *WeaviateCommandsValidateUnprocessableEntity {
	return &WeaviateCommandsValidateUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateCommandsValidateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateCommandsValidateNotImplementedCode is the HTTP code returned for type WeaviateCommandsValidateNotImplemented
const WeaviateCommandsValidateNotImplementedCode int = 501

/*WeaviateCommandsValidateNotImplemented Not (yet) implemented.

swagger:response weaviateCommandsValidateNotImplemented
*/
type WeaviateCommandsValidateNotImplemented struct {
}

// NewWeaviateCommandsValidateNotImplemented creates WeaviateCommandsValidateNotImplemented with default headers values
func NewWeaviateCommandsValidateNotImplemented() *WeaviateCommandsValidateNotImplemented {
	return &WeaviateCommandsValidateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateCommandsValidateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
