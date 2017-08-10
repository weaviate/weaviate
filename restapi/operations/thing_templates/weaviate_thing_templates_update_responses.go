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

	"github.com/weaviate/weaviate/models"
)

// WeaviateThingTemplatesUpdateOKCode is the HTTP code returned for type WeaviateThingTemplatesUpdateOK
const WeaviateThingTemplatesUpdateOKCode int = 200

/*WeaviateThingTemplatesUpdateOK Successful updated.

swagger:response weaviateThingTemplatesUpdateOK
*/
type WeaviateThingTemplatesUpdateOK struct {

	/*
	  In: Body
	*/
	Payload *models.ThingTemplateGetResponse `json:"body,omitempty"`
}

// NewWeaviateThingTemplatesUpdateOK creates WeaviateThingTemplatesUpdateOK with default headers values
func NewWeaviateThingTemplatesUpdateOK() *WeaviateThingTemplatesUpdateOK {
	return &WeaviateThingTemplatesUpdateOK{}
}

// WithPayload adds the payload to the weaviate thing templates update o k response
func (o *WeaviateThingTemplatesUpdateOK) WithPayload(payload *models.ThingTemplateGetResponse) *WeaviateThingTemplatesUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate thing templates update o k response
func (o *WeaviateThingTemplatesUpdateOK) SetPayload(payload *models.ThingTemplateGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingTemplatesUpdateUnauthorizedCode is the HTTP code returned for type WeaviateThingTemplatesUpdateUnauthorized
const WeaviateThingTemplatesUpdateUnauthorizedCode int = 401

/*WeaviateThingTemplatesUpdateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingTemplatesUpdateUnauthorized
*/
type WeaviateThingTemplatesUpdateUnauthorized struct {
}

// NewWeaviateThingTemplatesUpdateUnauthorized creates WeaviateThingTemplatesUpdateUnauthorized with default headers values
func NewWeaviateThingTemplatesUpdateUnauthorized() *WeaviateThingTemplatesUpdateUnauthorized {
	return &WeaviateThingTemplatesUpdateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesUpdateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingTemplatesUpdateForbiddenCode is the HTTP code returned for type WeaviateThingTemplatesUpdateForbidden
const WeaviateThingTemplatesUpdateForbiddenCode int = 403

/*WeaviateThingTemplatesUpdateForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingTemplatesUpdateForbidden
*/
type WeaviateThingTemplatesUpdateForbidden struct {
}

// NewWeaviateThingTemplatesUpdateForbidden creates WeaviateThingTemplatesUpdateForbidden with default headers values
func NewWeaviateThingTemplatesUpdateForbidden() *WeaviateThingTemplatesUpdateForbidden {
	return &WeaviateThingTemplatesUpdateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesUpdateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingTemplatesUpdateNotFoundCode is the HTTP code returned for type WeaviateThingTemplatesUpdateNotFound
const WeaviateThingTemplatesUpdateNotFoundCode int = 404

/*WeaviateThingTemplatesUpdateNotFound Successful query result but no resource was found.

swagger:response weaviateThingTemplatesUpdateNotFound
*/
type WeaviateThingTemplatesUpdateNotFound struct {
}

// NewWeaviateThingTemplatesUpdateNotFound creates WeaviateThingTemplatesUpdateNotFound with default headers values
func NewWeaviateThingTemplatesUpdateNotFound() *WeaviateThingTemplatesUpdateNotFound {
	return &WeaviateThingTemplatesUpdateNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesUpdateNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingTemplatesUpdateUnprocessableEntityCode is the HTTP code returned for type WeaviateThingTemplatesUpdateUnprocessableEntity
const WeaviateThingTemplatesUpdateUnprocessableEntityCode int = 422

/*WeaviateThingTemplatesUpdateUnprocessableEntity Can not validate, check the body.

swagger:response weaviateThingTemplatesUpdateUnprocessableEntity
*/
type WeaviateThingTemplatesUpdateUnprocessableEntity struct {
}

// NewWeaviateThingTemplatesUpdateUnprocessableEntity creates WeaviateThingTemplatesUpdateUnprocessableEntity with default headers values
func NewWeaviateThingTemplatesUpdateUnprocessableEntity() *WeaviateThingTemplatesUpdateUnprocessableEntity {
	return &WeaviateThingTemplatesUpdateUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesUpdateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateThingTemplatesUpdateNotImplementedCode is the HTTP code returned for type WeaviateThingTemplatesUpdateNotImplemented
const WeaviateThingTemplatesUpdateNotImplementedCode int = 501

/*WeaviateThingTemplatesUpdateNotImplemented Not (yet) implemented.

swagger:response weaviateThingTemplatesUpdateNotImplemented
*/
type WeaviateThingTemplatesUpdateNotImplemented struct {
}

// NewWeaviateThingTemplatesUpdateNotImplemented creates WeaviateThingTemplatesUpdateNotImplemented with default headers values
func NewWeaviateThingTemplatesUpdateNotImplemented() *WeaviateThingTemplatesUpdateNotImplemented {
	return &WeaviateThingTemplatesUpdateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesUpdateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
