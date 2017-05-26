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

// WeaviateThingTemplatesPatchOKCode is the HTTP code returned for type WeaviateThingTemplatesPatchOK
const WeaviateThingTemplatesPatchOKCode int = 200

/*WeaviateThingTemplatesPatchOK Successful updated.

swagger:response weaviateThingTemplatesPatchOK
*/
type WeaviateThingTemplatesPatchOK struct {

	/*
	  In: Body
	*/
	Payload *models.ThingTemplate `json:"body,omitempty"`
}

// NewWeaviateThingTemplatesPatchOK creates WeaviateThingTemplatesPatchOK with default headers values
func NewWeaviateThingTemplatesPatchOK() *WeaviateThingTemplatesPatchOK {
	return &WeaviateThingTemplatesPatchOK{}
}

// WithPayload adds the payload to the weaviate thing templates patch o k response
func (o *WeaviateThingTemplatesPatchOK) WithPayload(payload *models.ThingTemplate) *WeaviateThingTemplatesPatchOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate thing templates patch o k response
func (o *WeaviateThingTemplatesPatchOK) SetPayload(payload *models.ThingTemplate) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesPatchOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateThingTemplatesPatchBadRequestCode is the HTTP code returned for type WeaviateThingTemplatesPatchBadRequest
const WeaviateThingTemplatesPatchBadRequestCode int = 400

/*WeaviateThingTemplatesPatchBadRequest The patch-JSON is malformed.

swagger:response weaviateThingTemplatesPatchBadRequest
*/
type WeaviateThingTemplatesPatchBadRequest struct {
}

// NewWeaviateThingTemplatesPatchBadRequest creates WeaviateThingTemplatesPatchBadRequest with default headers values
func NewWeaviateThingTemplatesPatchBadRequest() *WeaviateThingTemplatesPatchBadRequest {
	return &WeaviateThingTemplatesPatchBadRequest{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesPatchBadRequest) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(400)
}

// WeaviateThingTemplatesPatchUnauthorizedCode is the HTTP code returned for type WeaviateThingTemplatesPatchUnauthorized
const WeaviateThingTemplatesPatchUnauthorizedCode int = 401

/*WeaviateThingTemplatesPatchUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateThingTemplatesPatchUnauthorized
*/
type WeaviateThingTemplatesPatchUnauthorized struct {
}

// NewWeaviateThingTemplatesPatchUnauthorized creates WeaviateThingTemplatesPatchUnauthorized with default headers values
func NewWeaviateThingTemplatesPatchUnauthorized() *WeaviateThingTemplatesPatchUnauthorized {
	return &WeaviateThingTemplatesPatchUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesPatchUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateThingTemplatesPatchForbiddenCode is the HTTP code returned for type WeaviateThingTemplatesPatchForbidden
const WeaviateThingTemplatesPatchForbiddenCode int = 403

/*WeaviateThingTemplatesPatchForbidden The used API-key has insufficient permissions.

swagger:response weaviateThingTemplatesPatchForbidden
*/
type WeaviateThingTemplatesPatchForbidden struct {
}

// NewWeaviateThingTemplatesPatchForbidden creates WeaviateThingTemplatesPatchForbidden with default headers values
func NewWeaviateThingTemplatesPatchForbidden() *WeaviateThingTemplatesPatchForbidden {
	return &WeaviateThingTemplatesPatchForbidden{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesPatchForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateThingTemplatesPatchNotFoundCode is the HTTP code returned for type WeaviateThingTemplatesPatchNotFound
const WeaviateThingTemplatesPatchNotFoundCode int = 404

/*WeaviateThingTemplatesPatchNotFound Successful query result but no resource was found.

swagger:response weaviateThingTemplatesPatchNotFound
*/
type WeaviateThingTemplatesPatchNotFound struct {
}

// NewWeaviateThingTemplatesPatchNotFound creates WeaviateThingTemplatesPatchNotFound with default headers values
func NewWeaviateThingTemplatesPatchNotFound() *WeaviateThingTemplatesPatchNotFound {
	return &WeaviateThingTemplatesPatchNotFound{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesPatchNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateThingTemplatesPatchUnprocessableEntityCode is the HTTP code returned for type WeaviateThingTemplatesPatchUnprocessableEntity
const WeaviateThingTemplatesPatchUnprocessableEntityCode int = 422

/*WeaviateThingTemplatesPatchUnprocessableEntity The patch-JSON is valid but unprocessable.

swagger:response weaviateThingTemplatesPatchUnprocessableEntity
*/
type WeaviateThingTemplatesPatchUnprocessableEntity struct {
}

// NewWeaviateThingTemplatesPatchUnprocessableEntity creates WeaviateThingTemplatesPatchUnprocessableEntity with default headers values
func NewWeaviateThingTemplatesPatchUnprocessableEntity() *WeaviateThingTemplatesPatchUnprocessableEntity {
	return &WeaviateThingTemplatesPatchUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesPatchUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateThingTemplatesPatchNotImplementedCode is the HTTP code returned for type WeaviateThingTemplatesPatchNotImplemented
const WeaviateThingTemplatesPatchNotImplementedCode int = 501

/*WeaviateThingTemplatesPatchNotImplemented Not (yet) implemented.

swagger:response weaviateThingTemplatesPatchNotImplemented
*/
type WeaviateThingTemplatesPatchNotImplemented struct {
}

// NewWeaviateThingTemplatesPatchNotImplemented creates WeaviateThingTemplatesPatchNotImplemented with default headers values
func NewWeaviateThingTemplatesPatchNotImplemented() *WeaviateThingTemplatesPatchNotImplemented {
	return &WeaviateThingTemplatesPatchNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateThingTemplatesPatchNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
