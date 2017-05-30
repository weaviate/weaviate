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




import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/models"
)

// WeaviateGroupsUpdateOKCode is the HTTP code returned for type WeaviateGroupsUpdateOK
const WeaviateGroupsUpdateOKCode int = 200

/*WeaviateGroupsUpdateOK Successful updated.

swagger:response weaviateGroupsUpdateOK
*/
type WeaviateGroupsUpdateOK struct {

	/*
	  In: Body
	*/
	Payload *models.GroupGetResponse `json:"body,omitempty"`
}

// NewWeaviateGroupsUpdateOK creates WeaviateGroupsUpdateOK with default headers values
func NewWeaviateGroupsUpdateOK() *WeaviateGroupsUpdateOK {
	return &WeaviateGroupsUpdateOK{}
}

// WithPayload adds the payload to the weaviate groups update o k response
func (o *WeaviateGroupsUpdateOK) WithPayload(payload *models.GroupGetResponse) *WeaviateGroupsUpdateOK {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the weaviate groups update o k response
func (o *WeaviateGroupsUpdateOK) SetPayload(payload *models.GroupGetResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *WeaviateGroupsUpdateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(200)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// WeaviateGroupsUpdateUnauthorizedCode is the HTTP code returned for type WeaviateGroupsUpdateUnauthorized
const WeaviateGroupsUpdateUnauthorizedCode int = 401

/*WeaviateGroupsUpdateUnauthorized Unauthorized or invalid credentials.

swagger:response weaviateGroupsUpdateUnauthorized
*/
type WeaviateGroupsUpdateUnauthorized struct {
}

// NewWeaviateGroupsUpdateUnauthorized creates WeaviateGroupsUpdateUnauthorized with default headers values
func NewWeaviateGroupsUpdateUnauthorized() *WeaviateGroupsUpdateUnauthorized {
	return &WeaviateGroupsUpdateUnauthorized{}
}

// WriteResponse to the client
func (o *WeaviateGroupsUpdateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(401)
}

// WeaviateGroupsUpdateForbiddenCode is the HTTP code returned for type WeaviateGroupsUpdateForbidden
const WeaviateGroupsUpdateForbiddenCode int = 403

/*WeaviateGroupsUpdateForbidden The used API-key has insufficient permissions.

swagger:response weaviateGroupsUpdateForbidden
*/
type WeaviateGroupsUpdateForbidden struct {
}

// NewWeaviateGroupsUpdateForbidden creates WeaviateGroupsUpdateForbidden with default headers values
func NewWeaviateGroupsUpdateForbidden() *WeaviateGroupsUpdateForbidden {
	return &WeaviateGroupsUpdateForbidden{}
}

// WriteResponse to the client
func (o *WeaviateGroupsUpdateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
}

// WeaviateGroupsUpdateNotFoundCode is the HTTP code returned for type WeaviateGroupsUpdateNotFound
const WeaviateGroupsUpdateNotFoundCode int = 404

/*WeaviateGroupsUpdateNotFound Successful query result but no resource was found.

swagger:response weaviateGroupsUpdateNotFound
*/
type WeaviateGroupsUpdateNotFound struct {
}

// NewWeaviateGroupsUpdateNotFound creates WeaviateGroupsUpdateNotFound with default headers values
func NewWeaviateGroupsUpdateNotFound() *WeaviateGroupsUpdateNotFound {
	return &WeaviateGroupsUpdateNotFound{}
}

// WriteResponse to the client
func (o *WeaviateGroupsUpdateNotFound) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(404)
}

// WeaviateGroupsUpdateUnprocessableEntityCode is the HTTP code returned for type WeaviateGroupsUpdateUnprocessableEntity
const WeaviateGroupsUpdateUnprocessableEntityCode int = 422

/*WeaviateGroupsUpdateUnprocessableEntity Can not validate, check the body.

swagger:response weaviateGroupsUpdateUnprocessableEntity
*/
type WeaviateGroupsUpdateUnprocessableEntity struct {
}

// NewWeaviateGroupsUpdateUnprocessableEntity creates WeaviateGroupsUpdateUnprocessableEntity with default headers values
func NewWeaviateGroupsUpdateUnprocessableEntity() *WeaviateGroupsUpdateUnprocessableEntity {
	return &WeaviateGroupsUpdateUnprocessableEntity{}
}

// WriteResponse to the client
func (o *WeaviateGroupsUpdateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
}

// WeaviateGroupsUpdateNotImplementedCode is the HTTP code returned for type WeaviateGroupsUpdateNotImplemented
const WeaviateGroupsUpdateNotImplementedCode int = 501

/*WeaviateGroupsUpdateNotImplemented Not (yet) implemented.

swagger:response weaviateGroupsUpdateNotImplemented
*/
type WeaviateGroupsUpdateNotImplemented struct {
}

// NewWeaviateGroupsUpdateNotImplemented creates WeaviateGroupsUpdateNotImplemented with default headers values
func NewWeaviateGroupsUpdateNotImplemented() *WeaviateGroupsUpdateNotImplemented {
	return &WeaviateGroupsUpdateNotImplemented{}
}

// WriteResponse to the client
func (o *WeaviateGroupsUpdateNotImplemented) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(501)
}
