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
 package thing_templates




import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// WeaviateThingTemplatesValidateOKCode is the HTTP code returned for type WeaviateThingTemplatesValidateOK
const WeaviateThingTemplatesValidateOKCode int = 200

/*WeaviateThingTemplatesValidateOK Successful validated.

swagger:response weaviateThingTemplatesValidateOK
*/
type WeaviateThingTemplatesValidateOK struct {
}

// NewWeaviateThingTemplatesValidateOK creates WeaviateThingTemplatesValidateOK with default headers values
func NewWeaviateThingTemplatesValidateOK() *WeaviateThingTemplatesValidateOK {
	return &WeaviateThingTemplatesValidateOK{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesValidateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
}

// WeaviateThingTemplatesValidateUnauthorizedCode is the HTTP code returned for type WeaviateThingTemplatesValidateUnauthorized
const WeaviateThingTemplatesValidateUnauthorizedCode int = 401

/*WeaviateThingTemplatesValidateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingTemplatesValidateUnauthorized
*/
type WeaviateThingTemplatesValidateUnauthorized struct {
}

// NewWeaviateThingTemplatesValidateUnauthorized creates WeaviateThingTemplatesValidateUnauthorized with default headers values
func NewWeaviateThingTemplatesValidateUnauthorized() *WeaviateThingTemplatesValidateUnauthorized {
	return &WeaviateThingTemplatesValidateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesValidateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingTemplatesValidateForbiddenCode is the HTTP code returned for type WeaviateThingTemplatesValidateForbidden
const WeaviateThingTemplatesValidateForbiddenCode int = 403

/*WeaviateThingTemplatesValidateForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingTemplatesValidateForbidden
*/
type WeaviateThingTemplatesValidateForbidden struct {
}

// NewWeaviateThingTemplatesValidateForbidden creates WeaviateThingTemplatesValidateForbidden with default headers values
func NewWeaviateThingTemplatesValidateForbidden() *WeaviateThingTemplatesValidateForbidden {
	return &WeaviateThingTemplatesValidateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesValidateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingTemplatesValidateUnprocessableEntityCode is the HTTP code returned for type WeaviateThingTemplatesValidateUnprocessableEntity
const WeaviateThingTemplatesValidateUnprocessableEntityCode int = 422

/*WeaviateThingTemplatesValidateUnprocessableEntity Can not validate, check the body.

swagger:response weaviateThingTemplatesValidateUnprocessableEntity
*/
type WeaviateThingTemplatesValidateUnprocessableEntity struct {
}

// NewWeaviateThingTemplatesValidateUnprocessableEntity creates WeaviateThingTemplatesValidateUnprocessableEntity with default headers values
func NewWeaviateThingTemplatesValidateUnprocessableEntity() *WeaviateThingTemplatesValidateUnprocessableEntity {
	return &WeaviateThingTemplatesValidateUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesValidateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateThingTemplatesValidateNotImplementedCode is the HTTP code returned for type WeaviateThingTemplatesValidateNotImplemented
const WeaviateThingTemplatesValidateNotImplementedCode int = 501

/*WeaviateThingTemplatesValidateNotImplemented Not (yet) implemented.

swagger:response weaviateThingTemplatesValidateNotImplemented
*/
type WeaviateThingTemplatesValidateNotImplemented struct {
}

// NewWeaviateThingTemplatesValidateNotImplemented creates WeaviateThingTemplatesValidateNotImplemented with default headers values
func NewWeaviateThingTemplatesValidateNotImplemented() *WeaviateThingTemplatesValidateNotImplemented {
	return &WeaviateThingTemplatesValidateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesValidateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
